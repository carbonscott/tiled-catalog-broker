"""Tests for the 401 retry mechanism and DLQ routing.

End-to-end tests use FastStream's TestRedisBroker to publish messages
through the real subscriber → exception middleware → error handler pipeline.
Lua script and scheduler tests use testcontainers Redis directly.

Fixtures for redis_container, redis_client, retry_pop_script, mock_settings,
mock_amsc_client, mock_tiled_root, and inject_context are in conftest.py.
"""

import asyncio
import time
from unittest.mock import AsyncMock, patch

import pytest
from faststream.redis import TestRedisBroker

from amsc_connector.core.constants import (
    HEADER_EVENT_ID,
    HEADER_FIRST_FAILED_AT,
    HEADER_RETRY_COUNT,
    RETRY_ZSET,
    STREAM_SYNC,
)
from amsc_connector.core.exceptions import (
    EntityRegistrationAuthError,
    EntityRegistrationError,
    TiledFetchError,
)
from amsc_connector.core.models import (
    RetryErrorType,
    RetryPayload,
)
from amsc_connector.worker import (
    _retry_scheduler_loop,
    broker,
)


def _make_auth_exc(
    detail: str = "Authentication failed",
    entity_type: str = "scientificWork",
    catalog_name: str = "test-catalog",
    location: str = "http://example.com/node",
) -> EntityRegistrationAuthError:
    return EntityRegistrationAuthError(
        detail,
        entity_type=entity_type,
        catalog_name=catalog_name,
        location=location,
    )


def _make_exc(
    status_code: int | None = None,
    detail: str = "error",
    entity_type: str = "scientificWork",
    catalog_name: str = "test-catalog",
    location: str = "http://example.com/node",
) -> EntityRegistrationError:
    return EntityRegistrationError(
        detail,
        status_code=status_code,
        entity_type=entity_type,
        catalog_name=catalog_name,
        location=location,
    )


# ---------------------------------------------------------------------------
# End-to-end handler tests — 401 routes to retry ZSET
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_401_routes_to_retry_zset(
    redis_client,
    mock_settings,
    inject_context,
):
    """A 401 error should ZADD a RetryPayload to the retry ZSET."""
    exc = _make_auth_exc()

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            side_effect=exc,
        ):
            await br.publish(
                {"path": ["VDP", "H_test"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-001"},
            )

    # Should have one item in the retry ZSET
    count = await redis_client.zcard(RETRY_ZSET)
    assert count == 1

    # Verify the stored payload
    items = await redis_client.zrange(RETRY_ZSET, 0, -1, withscores=True)
    payload_json, score = items[0]
    payload = RetryPayload.model_validate_json(payload_json)

    assert payload.path == ["VDP", "H_test"]
    assert payload.event_id == "evt-001"
    assert payload.retry_count == 1
    assert payload.last_error_type == RetryErrorType.AUTH_FAILED
    assert payload.entity_type == "scientificWork"
    assert score > time.time()  # Scheduled in the future


@pytest.mark.asyncio
async def test_401_retry_count_increments(
    redis_client,
    mock_settings,
    inject_context,
):
    """Retry count should be incremented from the incoming message header."""
    exc = _make_auth_exc()

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            side_effect=exc,
        ):
            await br.publish(
                {"path": ["VDP", "H_test"]},
                stream=STREAM_SYNC,
                headers={
                    HEADER_EVENT_ID: "evt-001",
                    HEADER_RETRY_COUNT: "5",
                },
            )

    items = await redis_client.zrange(RETRY_ZSET, 0, -1)
    payload = RetryPayload.model_validate_json(items[0])
    assert payload.retry_count == 6


@pytest.mark.asyncio
async def test_401_first_failed_at_preserved(
    redis_client,
    mock_settings,
    inject_context,
):
    """first_failed_at from the original failure should be preserved."""
    exc = _make_auth_exc()
    original_time = 500.0

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            side_effect=exc,
        ):
            await br.publish(
                {"path": ["VDP", "H_test"]},
                stream=STREAM_SYNC,
                headers={
                    HEADER_EVENT_ID: "evt-001",
                    HEADER_RETRY_COUNT: "3",
                    HEADER_FIRST_FAILED_AT: str(original_time),
                },
            )

    items = await redis_client.zrange(RETRY_ZSET, 0, -1)
    payload = RetryPayload.model_validate_json(items[0])
    assert payload.first_failed_at == original_time


# ---------------------------------------------------------------------------
# End-to-end handler tests — non-401 routes to DLQ stream
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_non_401_routes_to_dlq(
    redis_client,
    mock_settings,
    inject_context,
):
    """A non-401 error should publish to DLQ stream, not retry ZSET."""
    exc = _make_exc(status_code=500, detail="Internal server error")

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            side_effect=exc,
        ):
            await br.publish(
                {"path": ["VDP", "H_test"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-001"},
            )

    # Nothing in retry ZSET
    count = await redis_client.zcard(RETRY_ZSET)
    assert count == 0


@pytest.mark.asyncio
async def test_none_status_code_routes_to_dlq(
    redis_client,
    mock_settings,
    inject_context,
):
    """An error with no status code should go to DLQ."""
    exc = _make_exc(status_code=None, detail="Connection refused")

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            side_effect=exc,
        ):
            await br.publish(
                {"path": ["VDP", "H_test"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-001"},
            )

    # Nothing in retry ZSET
    count = await redis_client.zcard(RETRY_ZSET)
    assert count == 0


@pytest.mark.asyncio
async def test_tiled_fetch_error_routes_to_dlq(
    redis_client,
    mock_settings,
    inject_context,
):
    """A TiledFetchError should publish to DLQ stream."""
    exc = TiledFetchError(
        "Tiled unreachable",
        path=["VDP", "H_test"],
        status_code=503,
    )

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            side_effect=exc,
        ):
            await br.publish(
                {"path": ["VDP", "H_test"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-002"},
            )

    # Nothing in retry ZSET
    count = await redis_client.zcard(RETRY_ZSET)
    assert count == 0


# ---------------------------------------------------------------------------
# Lua script tests — atomic pop behavior (real Redis)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lua_pops_ready_items(redis_client, retry_pop_script):
    """Items with score <= now should be popped."""
    payload = RetryPayload(
        path=["VDP", "H_test"],
        event_id="evt-lua",
        retry_count=1,
        first_failed_at=time.time(),
        last_error_type=RetryErrorType.AUTH_FAILED,
        entity_type="scientificWork",
    )
    # Score in the past → ready
    await redis_client.zadd(RETRY_ZSET, {payload.model_dump_json(): time.time() - 10})

    results = await retry_pop_script(keys=[RETRY_ZSET], args=[str(time.time()), "50"])
    assert len(results) == 1

    restored = RetryPayload.model_validate_json(results[0])
    assert restored.event_id == "evt-lua"

    # ZSET now empty
    assert await redis_client.zcard(RETRY_ZSET) == 0


@pytest.mark.asyncio
async def test_lua_skips_future_items(redis_client, retry_pop_script):
    """Items with score in the future should NOT be popped."""
    payload = RetryPayload(
        path=["VDP", "H_future"],
        event_id="evt-future",
        retry_count=1,
        first_failed_at=time.time(),
        last_error_type=RetryErrorType.AUTH_FAILED,
        entity_type="artifact",
    )
    await redis_client.zadd(RETRY_ZSET, {payload.model_dump_json(): time.time() + 3600})

    results = await retry_pop_script(keys=[RETRY_ZSET], args=[str(time.time()), "50"])
    assert len(results) == 0
    assert await redis_client.zcard(RETRY_ZSET) == 1


@pytest.mark.asyncio
async def test_lua_respects_limit(redis_client, retry_pop_script):
    """Lua script should respect the LIMIT argument."""
    now = time.time()
    for i in range(10):
        payload = RetryPayload(
            path=["VDP", f"H_limit_{i}"],
            event_id=f"evt-limit-{i}",
            retry_count=1,
            first_failed_at=now,
            last_error_type=RetryErrorType.AUTH_FAILED,
            entity_type="artifact",
        )
        await redis_client.zadd(RETRY_ZSET, {payload.model_dump_json(): now - 10})

    results = await retry_pop_script(keys=[RETRY_ZSET], args=[str(now), "3"])
    assert len(results) == 3
    assert await redis_client.zcard(RETRY_ZSET) == 7


@pytest.mark.asyncio
async def test_lua_concurrent_no_duplicates(redis_client, retry_pop_script):
    """Concurrent Lua calls should not return duplicate items."""
    now = time.time()
    for i in range(20):
        payload = RetryPayload(
            path=["VDP", f"H_conc_{i}"],
            event_id=f"evt-conc-{i}",
            retry_count=1,
            first_failed_at=now,
            last_error_type=RetryErrorType.AUTH_FAILED,
            entity_type="scientificWork",
        )
        await redis_client.zadd(RETRY_ZSET, {payload.model_dump_json(): now - 10})

    r1, r2 = await asyncio.gather(
        retry_pop_script(keys=[RETRY_ZSET], args=[str(now), "50"]),
        retry_pop_script(keys=[RETRY_ZSET], args=[str(now), "50"]),
    )

    all_items = list(r1) + list(r2)
    assert len(all_items) == 20
    assert len(set(all_items)) == 20  # No duplicates
    assert await redis_client.zcard(RETRY_ZSET) == 0


# ---------------------------------------------------------------------------
# Scheduler tests — real Redis + mocked broker.publish
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_dispatches_ready_items(redis_client, retry_pop_script):
    """Scheduler should pop ready items and publish to sync stream."""
    payload = RetryPayload(
        path=["VDP", "H_sched"],
        event_id="evt-sched",
        retry_count=2,
        first_failed_at=100.0,
        last_error_type=RetryErrorType.AUTH_FAILED,
        entity_type="scientificWork",
    )
    await redis_client.zadd(RETRY_ZSET, {payload.model_dump_json(): time.time() - 10})

    mock_broker = AsyncMock()

    async def _cancel_on_sleep(seconds):
        raise asyncio.CancelledError()

    with (
        patch("amsc_connector.worker.broker", mock_broker),
        patch("amsc_connector.worker.asyncio.sleep", side_effect=_cancel_on_sleep),
    ):
        await _retry_scheduler_loop(retry_pop_script)

    mock_broker.publish.assert_awaited_once()
    call_kwargs = mock_broker.publish.call_args[1]
    assert call_kwargs["stream"] == STREAM_SYNC
    assert call_kwargs["headers"][HEADER_EVENT_ID] == "evt-sched"
    assert call_kwargs["headers"][HEADER_RETRY_COUNT] == "2"

    # Item removed from ZSET
    assert await redis_client.zcard(RETRY_ZSET) == 0


@pytest.mark.asyncio
async def test_scheduler_noop_when_empty(redis_client, retry_pop_script):
    """Scheduler should sleep without publishing when ZSET is empty."""
    mock_broker = AsyncMock()

    async def _cancel_on_sleep(seconds):
        raise asyncio.CancelledError()

    with (
        patch("amsc_connector.worker.broker", mock_broker),
        patch("amsc_connector.worker.asyncio.sleep", side_effect=_cancel_on_sleep),
    ):
        await _retry_scheduler_loop(retry_pop_script)

    mock_broker.publish.assert_not_awaited()


# ---------------------------------------------------------------------------
# Model test
# ---------------------------------------------------------------------------


def test_retry_payload_round_trip():
    """RetryPayload should serialize to JSON and deserialize back."""
    payload = RetryPayload(
        path=["VDP", "H_test"],
        event_id="evt-001",
        retry_count=3,
        first_failed_at=1000.0,
        last_error_type=RetryErrorType.AUTH_FAILED,
        entity_type="scientificWork",
    )
    json_str = payload.model_dump_json()
    restored = RetryPayload.model_validate_json(json_str)
    assert restored == payload
