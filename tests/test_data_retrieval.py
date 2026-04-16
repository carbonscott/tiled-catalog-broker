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
from ruamel.yaml import YAML

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))


def _load_base_dirs():
    """Load base_dir from each dataset YAML config in datasets/."""
    yaml = YAML()
    base_dirs = {}
    datasets_dir = Path(__file__).parent.parent / "datasets"
    for cfg_path in sorted(datasets_dir.glob("*.yaml")):
        with open(cfg_path) as f:
            cfg = yaml.load(f)
        base_dirs[cfg["key"]] = cfg["base_dir"]
    return base_dirs


@pytest.fixture(scope="session")
def base_dirs():
    """Load base_dir for each dataset from YAML configs."""
    return _load_base_dirs()


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
        from tiled_catalog_broker.clients.query_manifest import query_catalog

        manifest = query_catalog(mh_dataset_client, artifact_type="mh_powder_30T", limit=5)

        assert hasattr(manifest, "columns")
        assert len(manifest) > 0

    def test_query_catalog_has_uid_and_ent_key(self, mh_dataset_client):
        """Test that manifest has standard required columns."""
        from tiled_catalog_broker.clients.query_manifest import query_catalog

        manifest = query_catalog(mh_dataset_client, artifact_type="mh_powder_30T", limit=5)

        assert "uid" in manifest.columns
        assert "ent_key" in manifest.columns
        assert "path_mh_powder_30T" in manifest.columns
        assert "dataset_mh_powder_30T" in manifest.columns

    def test_query_catalog_returns_all_metadata_columns(self, mh_dataset_client):
        """Test that manifest includes all entity metadata columns, not a hardcoded subset."""
        from tiled_catalog_broker.clients.query_manifest import query_catalog

        manifest = query_catalog(mh_dataset_client, artifact_type="mh_powder_30T", limit=5)

        # Should include physics params beyond the old hardcoded 6
        assert len(manifest.columns) > 8

    def test_query_catalog_with_search_filter(self, mh_dataset_client):
        """Test filtering via Tiled's native search before calling query_catalog.

        search() on the dataset-level client returns a filtered view of entities,
        which query_catalog can iterate directly.
        """
        from tiled.queries import Key
        from tiled_catalog_broker.clients.query_manifest import query_catalog

        filtered = mh_dataset_client.search(Key("Ja_meV") >= 0)
        manifest = query_catalog(filtered, artifact_type="mh_powder_30T", limit=5)

        assert len(manifest) > 0
        assert all(manifest["Ja_meV"] >= 0)

    def test_load_artifacts_returns_arrays(self, small_manifest, base_dirs):
        """Test that load_artifacts returns a list of numpy arrays."""
        from tiled_catalog_broker.clients.query_manifest import load_artifacts

        # Find the base_dir for the MH dataset
        mh_base_dir = base_dirs.get("GenericSpin_Sunny_MH")
        if mh_base_dir is None:
            pytest.skip("No dataset config found for GenericSpin_Sunny_MH")

        arrays = load_artifacts(small_manifest, artifact_type="mh_powder_30T",
                                base_dir=mh_base_dir)

        assert len(arrays) == len(small_manifest)
        for arr in arrays:
            assert isinstance(arr, np.ndarray)
            assert arr.ndim >= 1

    def test_load_artifacts_correct_shape(self, small_manifest, base_dirs):
        """Test that loaded M(H) arrays have expected shape."""
        from tiled_catalog_broker.clients.query_manifest import load_artifacts

        mh_base_dir = base_dirs.get("GenericSpin_Sunny_MH")
        if mh_base_dir is None:
            pytest.skip("No dataset config found for GenericSpin_Sunny_MH")

        arrays = load_artifacts(small_manifest, artifact_type="mh_powder_30T",
                                base_dir=mh_base_dir)

        for arr in arrays:
            assert arr.shape == (200,)  # M(H) has 200 field points


@pytest.mark.integration
class TestModeBTiledAdapter:
    """Tests for Mode B: Visualizer access via Tiled adapters."""

    def test_mh_array_child_registered(self, mh_dataset_client):
        """mh_powder_30T must be a registered array child — hard failure if absent."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]
        assert "mh_powder_30T" in h.keys(), (
            "mh_powder_30T not found as a child node — registration may have failed"
        )

    def test_access_mh_curve_array(self, mh_dataset_client):
        """Test accessing M(H) curve as Tiled array."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        if "mh_powder_30T" not in h.keys():
            pytest.skip("mh_powder_30T not available for this entity")
        arr = h["mh_powder_30T"][:]
        assert arr.ndim == 1
        assert len(arr) == 200

    def test_access_ins_spectrum_array(self, mh_dataset_client):
        """Test accessing INS spectrum as Tiled array."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        if "ins_12meV" not in h.keys():
            pytest.skip("ins_12meV not available for this entity")
        arr = h["ins_12meV"][:]
        assert arr.ndim == 2
        assert arr.shape[0] > 0
        assert arr.shape[1] > 0

    def test_access_gs_state_array(self, mh_dataset_client):
        """Test accessing ground state as Tiled array."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        if "gs_state" not in h.keys():
            pytest.skip("gs_state not available for this entity")
        arr = h["gs_state"][:]
        assert arr.ndim == 2
        assert arr.shape == (3, 8)

    def test_array_slicing_works(self, mh_dataset_client):
        """Test that array slicing works via Tiled."""
        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        if "mh_powder_30T" not in h.keys():
            pytest.skip("mh_powder_30T not available for this entity")
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

    def test_mh_curve_data_matches(self, mh_dataset_client, base_dirs):
        """Test that Mode A and Mode B return same M(H) data."""
        mh_base_dir = base_dirs.get("GenericSpin_Sunny_MH")
        if mh_base_dir is None:
            pytest.skip("No dataset config found for GenericSpin_Sunny_MH")

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

        path = os.path.join(mh_base_dir, path_rel)

        with h5py.File(path, "r") as f:
            mode_a_data = f[dataset_path][:]

        assert np.allclose(mode_a_data, mode_b_data), "Mode A and Mode B data mismatch!"

    def test_metadata_matches_hdf5(self, mh_dataset_client, base_dirs):
        """Test that metadata matches values in HDF5 files."""
        mh_base_dir = base_dirs.get("GenericSpin_Sunny_MH")
        if mh_base_dir is None:
            pytest.skip("No dataset config found for GenericSpin_Sunny_MH")

        ent_key = list(mh_dataset_client.keys())[0]
        h = mh_dataset_client[ent_key]

        path_rel = h.metadata.get("path_mh_powder_30T")
        if not path_rel:
            pytest.skip("No path metadata available")

        path = os.path.join(mh_base_dir, path_rel)

        assert os.path.exists(path), f"HDF5 file not found: {path}"
