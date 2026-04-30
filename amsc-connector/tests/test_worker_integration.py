"""Integration tests for the on_sync subscriber using TestRedisBroker.

Tests the happy path, skip-non-public, node-not-found, and create-or-update
flows end-to-end through the FastStream in-memory broker.

All fixtures are in conftest.py.
"""

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest
from faststream.redis import TestRedisBroker
from tiled.structures.core import StructureFamily

from amsc_connector.core.constants import HEADER_EVENT_ID, STREAM_SYNC
from amsc_connector.worker import (
    TiledNodeContext,
    broker,
)

from .conftest import make_successful_response  # pyrefly: ignore[missing-import]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tiled_context(
    path: list[str],
    *,
    metadata: dict | None = None,
    structure_family: StructureFamily = StructureFamily.container,
    mimetype: str | None = None,
    catalog_prefix: str = "test-repo.test-catalog",
) -> TiledNodeContext:
    """Build a TiledNodeContext for testing."""
    return TiledNodeContext(
        path=path,
        structure_family=structure_family,
        metadata=metadata or {"amsc_public": True},
        location=f"http://localhost:8000/{'/'.join(path)}",
        parent_fqn=(None if len(path) == 1 else ".".join([catalog_prefix, *path[:-1]])),
        mimetype=mimetype,
    )


# ---------------------------------------------------------------------------
# Happy path — container at depth 1 → ScientificWork
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_sync_creates_scientific_work(
    mock_settings,
    mock_amsc_client,
    inject_context,
):
    """Publishing a depth-1 container path should POST a scientificWork entity."""
    ctx = _make_tiled_context(["VDP"], metadata={"amsc_public": True})

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            return_value=ctx,
        ):
            await br.publish(
                {"path": ["VDP"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-sw-001"},
            )

    mock_amsc_client.post.assert_awaited_once()
    call_args = mock_amsc_client.post.call_args
    # URL should end with scientificWork
    assert "scientificWork" in call_args.args[0]
    body = call_args.kwargs["json"]
    assert body["name"] == "VDP"
    assert body["type"] == "scientificWork"


# ---------------------------------------------------------------------------
# Happy path — container at depth 2 → ArtifactCollection
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_sync_creates_artifact_collection(
    mock_settings,
    mock_amsc_client,
    inject_context,
):
    """Publishing a depth-2 container path should POST an artifactCollection."""
    ctx = _make_tiled_context(
        ["VDP", "H_test"],
        metadata={"amsc_public": True},
    )

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            return_value=ctx,
        ):
            await br.publish(
                {"path": ["VDP", "H_test"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-ac-001"},
            )

    mock_amsc_client.post.assert_awaited_once()
    body = mock_amsc_client.post.call_args.kwargs["json"]
    assert body["type"] == "artifactCollection"
    assert body["name"] == "H_test"


# ---------------------------------------------------------------------------
# Happy path — array node → Artifact
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_sync_creates_artifact(
    mock_settings,
    mock_amsc_client,
    inject_context,
):
    """Publishing an array node path should POST an artifact."""
    ctx = _make_tiled_context(
        ["VDP", "H_test", "mh_powder"],
        metadata={"amsc_public": True},
        structure_family=StructureFamily.array,
        mimetype="application/x-hdf5",
    )

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            return_value=ctx,
        ):
            await br.publish(
                {"path": ["VDP", "H_test", "mh_powder"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-art-001"},
            )

    mock_amsc_client.post.assert_awaited_once()
    body = mock_amsc_client.post.call_args.kwargs["json"]
    assert body["type"] == "artifact"
    assert body["name"] == "mh_powder"


# ---------------------------------------------------------------------------
# Skip — node not marked amsc_public
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_sync_skips_non_public(
    mock_settings,
    mock_amsc_client,
    inject_context,
):
    """Nodes without amsc_public=True should be skipped (no API call)."""
    ctx = _make_tiled_context(["VDP"], metadata={"amsc_public": False})

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            return_value=ctx,
        ):
            await br.publish(
                {"path": ["VDP"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-skip-001"},
            )

    mock_amsc_client.post.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_sync_skips_no_public_key(
    mock_settings,
    mock_amsc_client,
    inject_context,
):
    """Nodes with no amsc_public key at all should be skipped."""
    ctx = _make_tiled_context(["VDP"], metadata={"other_key": "value"})

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            return_value=ctx,
        ):
            await br.publish(
                {"path": ["VDP"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-skip-002"},
            )

    mock_amsc_client.post.assert_not_awaited()


# ---------------------------------------------------------------------------
# Node not found — KeyError from tiled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_sync_handles_node_not_found(
    mock_settings,
    mock_amsc_client,
    inject_context,
):
    """When the tiled node is deleted, on_sync should log and skip."""
    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            side_effect=KeyError("VDP/H_deleted"),
        ):
            await br.publish(
                {"path": ["VDP", "H_deleted"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-404-001"},
            )

    mock_amsc_client.post.assert_not_awaited()


# ---------------------------------------------------------------------------
# Create-or-update — 400 "already exists" falls back to PUT
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_on_sync_updates_existing_entity(
    mock_settings,
    mock_amsc_client,
    inject_context,
):
    """When POST returns 400 'already exists', should fall back to PUT."""
    ctx = _make_tiled_context(["VDP"], metadata={"amsc_public": True})

    # POST returns 400 "already exists"
    post_resp = MagicMock(spec=httpx.Response)
    post_resp.status_code = 400
    post_resp.is_error = True
    post_resp.text = json.dumps({"detail": "Entity already exists"})
    post_resp.json.return_value = {"detail": "Entity already exists"}
    mock_amsc_client.post.return_value = post_resp

    # PUT returns 200 success
    put_resp = make_successful_response(status_code=200)
    mock_amsc_client.put.return_value = put_resp

    async with TestRedisBroker(broker) as br:
        with patch(
            "amsc_connector.worker._fetch_tiled_context",
            return_value=ctx,
        ):
            await br.publish(
                {"path": ["VDP"]},
                stream=STREAM_SYNC,
                headers={HEADER_EVENT_ID: "evt-update-001"},
            )

    mock_amsc_client.post.assert_awaited_once()
    mock_amsc_client.put.assert_awaited_once()
