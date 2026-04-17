"""Tests for the delete module and `tcb delete` CLI.

Unit tests use a MagicMock client and run without a server.
Integration tests use the `tiled_client` fixture from conftest.py and
require a reachable Tiled server (auto-skipped if unavailable).

The `all`-form integration test is gated behind TCB_ALLOW_DESTRUCTIVE_TESTS=1
so CI never wipes a shared server by accident.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from tiled_catalog_broker.delete import (
    resolve_target, preview_counts, delete_target, delete_all,
)


TEST_KEY_PREFIX = "TCB_DELETE_TEST_"


# ── unit tests (no server) ───────────────────────────────────

def _mock_container(keys, child_factory=None):
    """Build a mock that behaves like a Tiled container."""
    m = MagicMock()
    m.__contains__.side_effect = lambda k: k in keys
    m.__iter__.side_effect = lambda: iter(keys)
    m.__len__.side_effect = lambda: len(keys)
    if child_factory is not None:
        m.__getitem__.side_effect = child_factory
    return m


class TestResolveTarget:
    def test_dataset_only(self):
        ds = _mock_container([])
        client = _mock_container(["DS1"], lambda k: ds)
        node, path, granularity = resolve_target(client, "DS1")
        assert node is ds
        assert path == "DS1"
        assert granularity == "dataset"

    def test_dataset_and_entity(self):
        art = _mock_container([])
        ent = _mock_container(["art1"], lambda k: art)
        ds = _mock_container(["ENT1"], lambda k: ent)
        client = _mock_container(["DS1"], lambda k: ds)

        node, path, granularity = resolve_target(client, "DS1", "ENT1")
        assert node is ent
        assert path == "DS1/ENT1"
        assert granularity == "entity"

    def test_dataset_entity_artifact(self):
        art = _mock_container([])
        ent = _mock_container(["art1"], lambda k: art)
        ds = _mock_container(["ENT1"], lambda k: ent)
        client = _mock_container(["DS1"], lambda k: ds)

        node, path, granularity = resolve_target(client, "DS1", "ENT1", "art1")
        assert node is art
        assert path == "DS1/ENT1/art1"
        assert granularity == "artifact"

    def test_missing_dataset_raises(self):
        client = _mock_container([])
        with pytest.raises(KeyError, match="No such dataset"):
            resolve_target(client, "NOPE")

    def test_missing_entity_raises(self):
        ds = _mock_container(["ENT1"])
        client = _mock_container(["DS1"], lambda k: ds)
        with pytest.raises(KeyError, match="No such entity"):
            resolve_target(client, "DS1", "NOPE")

    def test_missing_artifact_raises(self):
        ent = _mock_container(["art1"])
        ds = _mock_container(["ENT1"], lambda k: ent)
        client = _mock_container(["DS1"], lambda k: ds)
        with pytest.raises(KeyError, match="No such artifact"):
            resolve_target(client, "DS1", "ENT1", "NOPE")


class TestPreviewCounts:
    def test_dataset_counts_children(self):
        ds = _mock_container(["a", "b", "c"])
        assert preview_counts(ds, "dataset") == {"n_children": 3}

    def test_entity_counts_children(self):
        ent = _mock_container(["art1", "art2"])
        assert preview_counts(ent, "entity") == {"n_children": 2}

    def test_artifact_is_zero(self):
        art = _mock_container([])
        assert preview_counts(art, "artifact") == {"n_children": 0}

    def test_all_returns_sample(self):
        keys = [f"DS{i}" for i in range(15)]
        client = _mock_container(keys)
        result = preview_counts(client, "all")
        assert result["n_children"] == 15
        assert result["sample_keys"] == keys[:10]


class TestDeleteTargetCallsNode:
    def test_delete_target_invokes_node_delete(self):
        node = MagicMock()
        delete_target(node)
        node.delete.assert_called_once_with(recursive=True, external_only=True)


class TestDeleteAll:
    def test_all_success(self):
        child = MagicMock()
        client = _mock_container(["a", "b"], lambda k: child)
        successes, failures = delete_all(client)
        assert successes == ["a", "b"]
        assert failures == []
        assert child.delete.call_count == 2

    def test_partial_failure_reported(self):
        def child_factory(k):
            m = MagicMock()
            if k == "b":
                m.delete.side_effect = RuntimeError("boom")
            return m

        client = _mock_container(["a", "b", "c"], child_factory)
        successes, failures = delete_all(client)
        assert successes == ["a", "c"]
        assert len(failures) == 1
        assert failures[0][0] == "b"
        assert "boom" in failures[0][1]


# ── integration tests (server required) ──────────────────────
#
# Our production artifacts are registered as EXTERNAL HDF5 data sources
# (see http_register.create_data_source, Management.external). These tests
# validate the container-level delete path that applies to dataset and
# entity granularities on any Tiled server, including tiled-test which has
# no filesystem mount for external reads. Artifact-level deletion against
# real external data is exercised via tcb register + tcb delete on a
# data-mounted server (tiled-dev) -- see test_delete_artifact_external.

@pytest.mark.integration
class TestDeleteIntegration:
    """Exercise the resolve + delete path using only containers.

    All test keys are prefixed with TEST_KEY_PREFIX so pollution from a
    failed test is easy to spot and clean up.
    """

    def _fresh_key(self, suffix):
        return f"{TEST_KEY_PREFIX}{suffix}"

    def _cleanup(self, tiled_client, key):
        if key in tiled_client:
            try:
                tiled_client[key].delete(recursive=True, external_only=True)
            except Exception:
                pass

    def test_delete_dataset(self, tiled_client):
        ds_key = self._fresh_key("DS")
        try:
            ds = tiled_client.create_container(ds_key, metadata={"kind": "test"})
            ds.create_container("ENT1", metadata={"kind": "test"})
            assert ds_key in tiled_client

            node, path, granularity = resolve_target(tiled_client, ds_key)
            assert granularity == "dataset"
            assert path == ds_key
            delete_target(node)

            assert ds_key not in list(tiled_client)
        finally:
            self._cleanup(tiled_client, ds_key)

    def test_delete_entity(self, tiled_client):
        ds_key = self._fresh_key("DS_ENT")
        try:
            ds = tiled_client.create_container(ds_key, metadata={"kind": "test"})
            ds.create_container("ENT1", metadata={"kind": "test"})
            assert "ENT1" in ds

            node, path, granularity = resolve_target(tiled_client, ds_key, "ENT1")
            assert granularity == "entity"
            assert path == f"{ds_key}/ENT1"
            delete_target(node)

            assert "ENT1" not in tiled_client[ds_key]
        finally:
            self._cleanup(tiled_client, ds_key)

    def test_resolve_missing_raises(self, tiled_client):
        with pytest.raises(KeyError):
            resolve_target(tiled_client, f"{TEST_KEY_PREFIX}DOES_NOT_EXIST")

    def test_resolve_missing_entity_raises(self, tiled_client):
        ds_key = self._fresh_key("DS_MISS")
        try:
            tiled_client.create_container(ds_key, metadata={"kind": "test"})
            with pytest.raises(KeyError, match="No such entity"):
                resolve_target(tiled_client, ds_key, "NOPE")
        finally:
            self._cleanup(tiled_client, ds_key)


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("TCB_ALLOW_DESTRUCTIVE_TESTS") != "1",
    reason="Destructive: set TCB_ALLOW_DESTRUCTIVE_TESTS=1 to run the 'all' test.",
)
class TestDeleteAllIntegration:
    def test_delete_all_wipes_root(self, tiled_client):
        # Ensure there is at least one throwaway container to delete
        k = f"{TEST_KEY_PREFIX}ALL_SMOKE"
        try:
            tiled_client.create_container(k, metadata={"kind": "test"})
            assert k in tiled_client
            successes, failures = delete_all(tiled_client)
            assert failures == []
            # After wipe, the key we created is gone
            assert k not in list(tiled_client)
        finally:
            if k in tiled_client:
                try:
                    tiled_client[k].delete(recursive=True, external_only=True)
                except Exception:
                    pass
