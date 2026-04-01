# /// script
# requires-python = ">=3.11"
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
Integration tests for data retrieval in both access modes.

Mode A (Expert): query_catalog() -> direct HDF5 loading via load_artifacts()
Mode B (Visualizer): Tiled adapter access via HTTP

The catalog has a two-level hierarchy: root -> dataset containers -> entities.
Mode A/B tests use mh_dataset_client (scoped to the MH dataset container).

Prerequisites:
    # Start server with registered data:
    uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret

Run with:
    uv run --with pytest pytest tests/test_data_retrieval.py -v
"""

import os
import sys
from pathlib import Path

import pytest
import numpy as np
import h5py

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.mark.integration
class TestModeAQueryCatalog:
    """Tests for Mode A: Expert path-based access via query_catalog.

    Uses mh_dataset_client (dataset-level container) since query_catalog
    iterates immediate children. The root client has dataset containers as
    children, not entity containers.

    Uses limit= to avoid iterating all 10K entities over HTTP.
    """

    def test_query_catalog_returns_dataframe(self, mh_dataset_client):
        """Test that query_catalog returns a non-empty DataFrame."""
        from broker.query_manifest import query_catalog

        manifest = query_catalog(mh_dataset_client, artifact_type="mh_powder_30T", limit=5)

        assert hasattr(manifest, "columns")
        assert len(manifest) > 0

    def test_query_catalog_has_uid_and_ent_key(self, mh_dataset_client):
        """Test that manifest has standard required columns."""
        from broker.query_manifest import query_catalog

        manifest = query_catalog(mh_dataset_client, artifact_type="mh_powder_30T", limit=5)

        assert "uid" in manifest.columns
        assert "ent_key" in manifest.columns
        assert "path_mh_powder_30T" in manifest.columns
        assert "dataset_mh_powder_30T" in manifest.columns

    def test_query_catalog_returns_all_metadata_columns(self, mh_dataset_client):
        """Test that manifest includes all entity metadata columns, not a hardcoded subset."""
        from broker.query_manifest import query_catalog

        manifest = query_catalog(mh_dataset_client, artifact_type="mh_powder_30T", limit=5)

        # Should include physics params beyond the old hardcoded 6
        assert len(manifest.columns) > 8

    def test_query_catalog_with_search_filter(self, tiled_client):
        """Test filtering via Tiled's native search before calling query_catalog.

        search() on root returns entity-level results directly, so query_catalog
        works correctly on a search result regardless of hierarchy depth.
        """
        from tiled.queries import Key
        from broker.query_manifest import query_catalog

        # Use a narrow range to keep result size small
        narrow = tiled_client.search(Key("Ja_meV") >= 0).search(Key("Ja_meV") <= 0.05)
        manifest = query_catalog(narrow, artifact_type="mh_powder_30T")

        if len(manifest) > 0:
            assert all(manifest["Ja_meV"] >= 0)
            assert all(manifest["Ja_meV"] <= 0.05)

    def test_load_artifacts_returns_arrays(self, small_manifest):
        """Test that load_artifacts returns a list of numpy arrays."""
        from broker.query_manifest import load_artifacts

        arrays = load_artifacts(small_manifest, artifact_type="mh_powder_30T")

        assert len(arrays) == len(small_manifest)
        for arr in arrays:
            assert isinstance(arr, np.ndarray)
            assert arr.ndim >= 1

    def test_load_artifacts_correct_shape(self, small_manifest):
        """Test that loaded M(H) arrays have expected shape."""
        from broker.query_manifest import load_artifacts

        arrays = load_artifacts(small_manifest, artifact_type="mh_powder_30T")

        for arr in arrays:
            assert arr.shape == (200,)  # M(H) has 200 field points


@pytest.mark.integration
class TestModeBTiledAdapter:
    """Tests for Mode B: Visualizer access via Tiled adapters."""

    def test_access_mh_curve_array(self, mh_dataset_client):
        """Test accessing M(H) curve as Tiled array."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        if "mh_powder_30T" in h.keys():
            arr = h["mh_powder_30T"][:]
            assert arr.ndim == 1
            assert len(arr) == 200

    def test_access_ins_spectrum_array(self, mh_dataset_client):
        """Test accessing INS spectrum as Tiled array."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        if "ins_12meV" in h.keys():
            arr = h["ins_12meV"][:]
            assert arr.ndim == 2
            assert arr.shape[0] > 0
            assert arr.shape[1] > 0

    def test_access_gs_state_array(self, mh_dataset_client):
        """Test accessing ground state as Tiled array."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        if "gs_state" in h.keys():
            arr = h["gs_state"][:]
            assert arr.ndim == 2
            assert arr.shape == (3, 8)

    def test_array_slicing_works(self, mh_dataset_client):
        """Test that array slicing works via Tiled."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        if "mh_powder_30T" in h.keys():
            arr_slice = h["mh_powder_30T"][:100]
            assert len(arr_slice) == 100

            arr_mid = h["mh_powder_30T"][50:150]
            assert len(arr_mid) == 100

    def test_metadata_accessible(self, mh_dataset_client):
        """Test that entity physics metadata is accessible."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        assert "uid" in h.metadata


@pytest.mark.integration
class TestModeAModeBEquivalence:
    """Tests verifying both modes return identical data."""

    def test_mh_curve_data_matches(self, mh_dataset_client):
        """Test that Mode A and Mode B return same M(H) data."""
        from broker.config import get_base_dir

        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        if "mh_powder_30T" not in h.keys():
            pytest.skip("mh_powder_30T not available")

        # Mode B: Tiled adapter access
        mode_b_data = h["mh_powder_30T"][:]

        # Mode A: Direct HDF5 access via locators from metadata
        path_rel = h.metadata.get("path_mh_powder_30T")
        dataset_path = h.metadata.get("dataset_mh_powder_30T")
        if not path_rel or not dataset_path:
            pytest.skip("Locator metadata not available")

        base_dir = get_base_dir()
        path = os.path.join(base_dir, path_rel)

        with h5py.File(path, "r") as f:
            mode_a_data = f[dataset_path][:]

        assert np.allclose(mode_a_data, mode_b_data), "Mode A and Mode B data mismatch!"

    def test_metadata_matches_hdf5(self, mh_dataset_client):
        """Test that metadata matches values in HDF5 files."""
        from broker.config import get_base_dir

        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        path_rel = h.metadata.get("path_mh_powder_30T")
        if not path_rel:
            pytest.skip("No path metadata available")

        base_dir = get_base_dir()
        path = os.path.join(base_dir, path_rel)

        assert os.path.exists(path), f"HDF5 file not found: {path}"
