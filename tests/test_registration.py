# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pytest",
#     "tiled[server]",
#     "pandas",
#     "h5py",
#     "numpy",
#     "ruamel.yaml",
# ]
# ///
"""
Integration tests for data registration.

Covers manifest loading (unit) and the registered result on a live server
(integration) for the single registration route, `tcb register` (ADR-0002).
What registration *sends* is covered without a server in
`tests/test_generic_registration.py`.

Prerequisites:
    # For the integration tests, start a server with data registered:
    uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret

Run with:
    uv run --with pytest pytest tests/test_registration.py -v
"""

import os
from pathlib import Path

import pytest
import pandas as pd

# Synthetic test manifests ship with the repo. After the generic-registration
# refactor (commit 30400b2) there is no single "latest manifest" -- each dataset
# YAML names its own manifest parquets. These tests use the VDP testdata
# manifests under tests/testdata/vdp as a stable fixture for manifest loading.
TESTDATA_DIR = Path(__file__).parent / "testdata"
VDP_ENTITIES_PARQUET = TESTDATA_DIR / "vdp" / "vdp_entities.parquet"
VDP_ARTIFACTS_PARQUET = TESTDATA_DIR / "vdp" / "vdp_artifacts.parquet"


class TestLoadManifests:
    """Tests for manifest loading (used by both registration methods)."""

    def test_load_entities_manifest(self):
        """Test that entities manifest can be loaded."""
        df = pd.read_parquet(VDP_ENTITIES_PARQUET)

        assert len(df) > 0
        assert "uid" in df.columns

    def test_load_artifacts_manifest(self):
        """Test that Artifacts manifest can be loaded."""
        df = pd.read_parquet(VDP_ARTIFACTS_PARQUET)

        assert len(df) > 0
        assert "type" in df.columns
        assert "uid" in df.columns

    def test_manifests_have_matching_uids(self):
        """Test that artifact uids match entity uids."""
        ent_df = pd.read_parquet(VDP_ENTITIES_PARQUET)
        art_df = pd.read_parquet(VDP_ARTIFACTS_PARQUET)

        ent_uids = set(ent_df["uid"])
        art_uids = set(art_df["uid"])

        # All artifact uids should exist in entities
        assert art_uids.issubset(ent_uids)


@pytest.mark.integration
class TestHttpRegistration:
    """Integration tests for HTTP-based registration.

    Requires running Tiled server with registered data.
    """

    def test_server_has_containers(self, tiled_client):
        """Test that registered entities appear as containers."""
        assert len(tiled_client) > 0

    def test_container_has_metadata(self, tiled_client):
        """Test that containers have physics parameters in metadata."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        # Check physics parameters
        assert "Ja_meV" in h.metadata
        assert "Jb_meV" in h.metadata
        assert "Jc_meV" in h.metadata
        assert "Dc_meV" in h.metadata

    def test_container_has_artifact_paths(self, tiled_client):
        """Test that containers have artifact paths in metadata (Mode A)."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        # Check for path metadata (Mode A support)
        path_keys = [k for k in h.metadata.keys() if k.startswith("path_")]
        assert len(path_keys) > 0

    def test_container_has_children(self, tiled_client):
        """Test that containers have artifact children (Mode B)."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        children = list(h.keys())
        assert len(children) > 0

    def test_container_children_are_arrays(self, tiled_client):
        """Test that children are accessible as arrays."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        children = list(h.keys())
        if "mh_powder_30T" in children:
            arr = h["mh_powder_30T"][:]
            assert arr.ndim == 1
            assert len(arr) == 200  # M(H) has 200 points
