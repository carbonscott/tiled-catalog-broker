"""Tests for _cleanup_consumer — the graceful shutdown helper."""

import contextlib

import pytest

from amsc_connector.core.constants import STREAM_SYNC
from amsc_connector.worker import CONSUMER_GROUP, _cleanup_consumer


async def _make_pending(redis_client, stream, group, consumer, fields):
    """Put a message in PEL: xadd → xgroup_create → xreadgroup (no ack)."""
    msg_id = await redis_client.xadd(stream, fields)
    with contextlib.suppress(Exception):
        await redis_client.xgroup_create(stream, group, id="0", mkstream=True)
    await redis_client.xreadgroup(group, consumer, streams={stream: ">"}, count=1)
    return msg_id


@pytest.mark.asyncio
async def test_cleanup_no_pending_deletes_consumer(redis_client):
    """Consumer with empty PEL is deleted cleanly."""
    await redis_client.xgroup_create(STREAM_SYNC, CONSUMER_GROUP, id="$", mkstream=True)
    # Register the consumer by delivering then acking a message
    msg_id = await redis_client.xadd(STREAM_SYNC, {b"data": b"x"})
    await redis_client.xreadgroup(
        CONSUMER_GROUP, "worker-test", streams={STREAM_SYNC: ">"}, count=1
    )
    await redis_client.xack(STREAM_SYNC, CONSUMER_GROUP, msg_id)

    await _cleanup_consumer(redis_client, STREAM_SYNC, "worker-test")

    info = await redis_client.xinfo_consumers(STREAM_SYNC, CONSUMER_GROUP)
    names = [c["name"] for c in info]
    assert b"worker-test" not in names


@pytest.mark.asyncio
async def test_cleanup_requeues_pending_then_deletes(redis_client):
    """Pending messages are re-added to stream, acked, consumer deleted."""
    fields = {b"data": b"payload"}
    await _make_pending(
        redis_client, STREAM_SYNC, CONSUMER_GROUP, "worker-dead", fields
    )

    stream_len_before = await redis_client.xlen(STREAM_SYNC)
    await _cleanup_consumer(redis_client, STREAM_SYNC, "worker-dead")

    # One new message was re-queued
    assert await redis_client.xlen(STREAM_SYNC) == stream_len_before + 1
    # Original is acked — PEL empty
    pending = await redis_client.xpending_range(
        STREAM_SYNC, CONSUMER_GROUP, min="-", max="+", count=10
    )
    assert not pending
    # Consumer deleted
    info = await redis_client.xinfo_consumers(STREAM_SYNC, CONSUMER_GROUP)
    assert not any(c["name"] == b"worker-dead" for c in info)
