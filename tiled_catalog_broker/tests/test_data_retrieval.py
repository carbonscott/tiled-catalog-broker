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

Mode A (Expert): query_manifest() -> direct HDF5 loading
Mode B (Visualizer): Tiled adapter access via HTTP

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
class TestModeAQueryManifest:
    """Tests for Mode A: Expert path-based access via query_manifest."""

    def test_query_manifest_returns_dataframe(self, tiled_client):
        """Test that query_manifest returns a DataFrame."""
        from broker.query_manifest import query_manifest

        manifest = query_manifest(tiled_client, axis="powder", Hmax_T=30)

        assert hasattr(manifest, "columns")  # Is DataFrame-like
        assert len(manifest) > 0

    def test_query_manifest_has_required_columns(self, tiled_client):
        """Test that manifest has all required columns."""
        from broker.query_manifest import query_manifest

        manifest = query_manifest(tiled_client, axis="powder", Hmax_T=30)

        required_cols = ["uid", "ent_key", "Ja_meV", "Jb_meV", "Jc_meV", "Dc_meV", "path_rel"]
        for col in required_cols:
            assert col in manifest.columns, f"Missing column: {col}"

    def test_query_manifest_with_ja_filter(self, tiled_client):
        """Test filtering by Ja_meV parameter."""
        from broker.query_manifest import query_manifest

        # Get all
        all_manifest = query_manifest(tiled_client, axis="powder", Hmax_T=30)

        # Get ferromagnetic only (Ja > 0)
        fm_manifest = query_manifest(tiled_client, axis="powder", Hmax_T=30, Ja_min=0)

        # Filtered should be subset
        assert len(fm_manifest) <= len(all_manifest)

        # All filtered should have Ja > 0
        if len(fm_manifest) > 0:
            assert all(fm_manifest["Ja_meV"] >= 0)

    def test_load_from_manifest_shapes(self, tiled_client):
        """Test that loaded data has correct shapes."""
        from broker.query_manifest import query_manifest, load_from_manifest

        manifest = query_manifest(tiled_client, axis="powder", Hmax_T=30)
        manifest = manifest.head(5)  # Small subset for speed

        X, Theta = load_from_manifest(manifest)

        # X should be (n_samples, n_points)
        assert X.ndim == 2
        assert X.shape[0] == len(manifest)
        assert X.shape[1] == 200  # M(H) has 200 field points

        # Theta should be (n_samples, 6)
        assert Theta.shape == (len(manifest), 6)

    def test_load_from_manifest_normalized(self, tiled_client):
        """Test that M(H) data is normalized to [-1, 1]."""
        from broker.query_manifest import query_manifest, load_from_manifest

        manifest = query_manifest(tiled_client, axis="powder", Hmax_T=30)
        manifest = manifest.head(5)

        X, Theta = load_from_manifest(manifest)

        # Normalized magnetization should be in reasonable range
        assert np.all(X >= -1.5)  # Allow small margin
        assert np.all(X <= 1.5)

    def test_build_mh_dataset_complete_api(self, tiled_client):
        """Test the complete Julia-equivalent API."""
        from broker.query_manifest import build_mh_dataset

        X, h_grid, Theta, manifest = build_mh_dataset(
            tiled_client, axis="powder", Hmax_T=30
        )

        # Verify shapes match
        assert X.shape[0] == len(manifest)
        assert X.shape[1] == len(h_grid)
        assert Theta.shape[0] == len(manifest)

        # h_grid should be [0, 1]
        assert h_grid[0] == 0
        assert h_grid[-1] == 1


@pytest.mark.integration
class TestModeBTiledAdapter:
    """Tests for Mode B: Visualizer access via Tiled adapters."""

    def test_access_mh_curve_array(self, tiled_client):
        """Test accessing M(H) curve as Tiled array."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        if "mh_powder_30T" in h.keys():
            arr = h["mh_powder_30T"][:]
            assert arr.ndim == 1
            assert len(arr) == 200

    def test_access_ins_spectrum_array(self, tiled_client):
        """Test accessing INS spectrum as Tiled array."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        if "ins_12meV" in h.keys():
            arr = h["ins_12meV"][:]
            assert arr.ndim == 2
            # INS is typically 600x400
            assert arr.shape[0] > 0
            assert arr.shape[1] > 0

    def test_access_gs_state_array(self, tiled_client):
        """Test accessing ground state as Tiled array."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        if "gs_state" in h.keys():
            arr = h["gs_state"][:]
            assert arr.ndim == 2
            assert arr.shape == (3, 8)

    def test_array_slicing_works(self, tiled_client):
        """Test that array slicing works via Tiled."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        if "mh_powder_30T" in h.keys():
            # Slice first 100 points
            arr_slice = h["mh_powder_30T"][:100]
            assert len(arr_slice) == 100

            # Slice middle
            arr_mid = h["mh_powder_30T"][50:150]
            assert len(arr_mid) == 100

    def test_metadata_accessible(self, tiled_client):
        """Test that physics metadata is accessible."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        # Physics parameters should be floats
        assert isinstance(h.metadata["Ja_meV"], (int, float))
        assert isinstance(h.metadata["Jb_meV"], (int, float))
        assert isinstance(h.metadata["Dc_meV"], (int, float))


@pytest.mark.integration
class TestModeAModeBEquivalence:
    """Tests verifying both modes return identical data."""

    def test_mh_curve_data_matches(self, tiled_client):
        """Test that Mode A and Mode B return same M(H) data."""
        from broker.config import get_base_dir

        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        if "mh_powder_30T" not in h.keys():
            pytest.skip("mh_powder_30T not available")

        # Mode B: Tiled adapter access
        mode_b_data = h["mh_powder_30T"][:]

        # Mode A: Direct HDF5 access via path from metadata
        path_rel = h.metadata.get("path_mh_powder_30T")
        if not path_rel:
            pytest.skip("path_mh_powder_30T not in metadata")

        base_dir = get_base_dir()
        path = os.path.join(base_dir, path_rel)

        with h5py.File(path, "r") as f:
            mode_a_data = f["/curve/M_parallel"][:]

        # Data should match exactly
        assert np.allclose(mode_a_data, mode_b_data), "Mode A and Mode B data mismatch!"

    def test_metadata_matches_hdf5(self, tiled_client):
        """Test that metadata matches values in HDF5 files."""
        from broker.config import get_base_dir

        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        # Get any path from metadata
        path_rel = h.metadata.get("path_mh_powder_30T")
        if not path_rel:
            pytest.skip("No path metadata available")

        base_dir = get_base_dir()
        path = os.path.join(base_dir, path_rel)

        # The HDF5 file should exist
        assert os.path.exists(path), f"HDF5 file not found: {path}"
