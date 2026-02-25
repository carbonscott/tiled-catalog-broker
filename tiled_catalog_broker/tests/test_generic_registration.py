# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "pandas",
#     "pyarrow",
#     "h5py",
#     "numpy",
#     "ruamel.yaml",
#     "canonicaljson",
# ]
# ///
"""
Tests for generic registration with both VDP and NiPS3 datasets.

Verifies that prepare_node_data() produces correct metadata, locators,
and artifact structures for any dataset -- no hardcoded parameter names.

Uses synthetic test data from tests/testdata/. No running Tiled server needed.

Run with:
    uv run --with pytest --with pandas --with pyarrow --with h5py \
      --with 'ruamel.yaml' --with canonicaljson \
      pytest tests/test_generic_registration.py -v
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

TESTDATA_DIR = Path(__file__).parent / "testdata"


@pytest.fixture
def vdp_manifests():
    """Load VDP synthetic test manifests."""
    vdp_dir = TESTDATA_DIR / "vdp"
    ent_df = pd.read_parquet(vdp_dir / "vdp_entities.parquet")
    art_df = pd.read_parquet(vdp_dir / "vdp_artifacts.parquet")
    base_dir = str(vdp_dir)
    return ent_df, art_df, base_dir


@pytest.fixture
def nips3_manifests():
    """Load NiPS3 synthetic test manifests."""
    nips3_dir = TESTDATA_DIR / "nips3"
    ent_df = pd.read_parquet(nips3_dir / "nips3_entities.parquet")
    art_df = pd.read_parquet(nips3_dir / "nips3_artifacts.parquet")
    base_dir = str(nips3_dir)
    return ent_df, art_df, base_dir


@pytest.fixture(autouse=True)
def clear_shape_cache():
    """Clear the HDF5 shape cache between tests."""
    from broker.utils import get_artifact_shape
    get_artifact_shape.__defaults__[-1].clear()
    yield
    get_artifact_shape.__defaults__[-1].clear()


# ─── VDP Tests ───────────────────────────────────────────────────────────────


class TestVDPRegistration:
    """Test generic registration with VDP-style data."""

    def test_correct_node_counts(self, vdp_manifests):
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        ent_nodes, art_nodes, art_ds = prepare_node_data(
            ent_df, art_df, max_entities=5, base_dir=base_dir
        )

        assert len(ent_nodes) == 5
        assert len(art_nodes) == 15  # 3 artifacts per entity
        assert len(art_ds) == 15

    def test_entity_key_from_manifest(self, vdp_manifests):
        """Keys are read from the manifest's 'key' column, not computed."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        ent_nodes, _, _ = prepare_node_data(
            ent_df, art_df, max_entities=5, base_dir=base_dir
        )

        for i, node in enumerate(ent_nodes):
            expected_key = ent_df.iloc[i]["key"]
            assert node["key"] == expected_key

    def test_missing_key_column_raises(self, vdp_manifests):
        """Registration fails early if manifest lacks 'key' column."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        ent_no_key = ent_df.drop(columns=["key"])
        with pytest.raises(ValueError, match="missing required 'key' column"):
            prepare_node_data(ent_no_key, art_df, max_entities=1, base_dir=base_dir)

    def test_entity_metadata_has_vdp_params(self, vdp_manifests):
        """VDP metadata should have Ja_meV, Jb_meV, etc. (read dynamically)."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        ent_nodes, _, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        meta = ent_nodes[0]["metadata"]
        assert "uid" in meta
        assert "Ja_meV" in meta
        assert "Jb_meV" in meta
        assert "Jc_meV" in meta
        assert "Dc_meV" in meta
        assert "spin_s" in meta
        assert "g_factor" in meta

    def test_entity_metadata_has_locators(self, vdp_manifests):
        """Locators (path_, dataset_) stored in entity metadata for Mode A."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        ent_nodes, _, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        meta = ent_nodes[0]["metadata"]

        # VDP has 3 artifact types
        assert "path_mh_powder_30T" in meta
        assert "path_gs_state" in meta
        assert "path_ins_12meV" in meta

        assert "dataset_mh_powder_30T" in meta
        assert "dataset_gs_state" in meta
        assert "dataset_ins_12meV" in meta

        # VDP has no index (single-entity files)
        index_keys = [k for k in meta if k.startswith("index_")]
        assert len(index_keys) == 0

    def test_artifact_keys_match_types(self, vdp_manifests):
        """Artifact keys come directly from the type column."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        _, art_nodes, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        keys = {node["key"] for node in art_nodes}
        assert keys == {"mh_powder_30T", "gs_state", "ins_12meV"}

    def test_artifact_shapes_from_hdf5(self, vdp_manifests):
        """Shapes are read from actual HDF5 files, not hardcoded."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        _, art_nodes, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        shapes = {node["key"]: node["metadata"]["shape"] for node in art_nodes}
        assert shapes["mh_powder_30T"] == [10]
        assert shapes["gs_state"] == [3, 4]
        assert shapes["ins_12meV"] == [6, 5]

    def test_data_source_parameters(self, vdp_manifests):
        """Data sources carry dataset path from manifest."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        _, _, art_ds = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        ds_by_key = {ds["art_key"]: ds for ds in art_ds}
        assert ds_by_key["mh_powder_30T"]["parameters"]["dataset"] == "/curve/M_parallel"
        assert ds_by_key["gs_state"]["parameters"]["dataset"] == "/gs/spin_dir"
        assert ds_by_key["ins_12meV"]["parameters"]["dataset"] == "/ins/broadened"

    def test_max_entities_limit(self, vdp_manifests):
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        ent_nodes, art_nodes, _ = prepare_node_data(
            ent_df, art_df, max_entities=2, base_dir=base_dir
        )

        assert len(ent_nodes) == 2
        assert len(art_nodes) == 6  # 3 per entity


# ─── NiPS3 Tests ─────────────────────────────────────────────────────────────


class TestNiPS3Registration:
    """Test generic registration with NiPS3-style data (batched files)."""

    def test_correct_node_counts(self, nips3_manifests):
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = nips3_manifests

        ent_nodes, art_nodes, art_ds = prepare_node_data(
            ent_df, art_df, max_entities=5, base_dir=base_dir
        )

        assert len(ent_nodes) == 5
        assert len(art_nodes) == 10  # 2 artifacts per entity
        assert len(art_ds) == 10

    def test_entity_metadata_has_nips3_params(self, nips3_manifests):
        """NiPS3 metadata should have F2_dd, F2_dp, etc. (read dynamically)."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = nips3_manifests

        ent_nodes, _, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        meta = ent_nodes[0]["metadata"]
        assert "uid" in meta
        assert "F2_dd" in meta
        assert "F2_dp" in meta
        assert "F4_dd" in meta
        assert "G1_dp" in meta
        assert "G3_dp" in meta

        # Should NOT have VDP params
        assert "Ja_meV" not in meta
        assert "Jb_meV" not in meta

    def test_entity_metadata_has_index_locators(self, nips3_manifests):
        """NiPS3 locators should include index for batched files."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = nips3_manifests

        ent_nodes, _, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        meta = ent_nodes[0]["metadata"]

        # NiPS3 has 2 artifact types
        assert "path_rixs" in meta
        assert "path_mag" in meta
        assert "dataset_rixs" in meta
        assert "dataset_mag" in meta

        # NiPS3 is batched, so index should be present
        assert "index_rixs" in meta
        assert "index_mag" in meta
        assert meta["index_rixs"] == 0  # First entity

    def test_artifact_keys_match_types(self, nips3_manifests):
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = nips3_manifests

        _, art_nodes, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        keys = {node["key"] for node in art_nodes}
        assert keys == {"rixs", "mag"}

    def test_batched_shapes_skip_batch_dimension(self, nips3_manifests):
        """For batched files, shape should be per-entity (batch dim removed)."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = nips3_manifests

        _, art_nodes, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        shapes = {node["key"]: node["metadata"]["shape"] for node in art_nodes}
        assert shapes["rixs"] == [6, 5]  # (5, 6, 5) -> (6, 5) per entity
        assert shapes["mag"] == [10]  # (5, 10) -> (10,) per entity

    def test_data_source_has_slice_parameter(self, nips3_manifests):
        """Data sources for batched files include slice in parameters."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = nips3_manifests

        _, _, art_ds = prepare_node_data(
            ent_df, art_df, max_entities=2, base_dir=base_dir
        )

        for ds in art_ds:
            assert "slice" in ds["parameters"]

        # First entity's artifacts should have slice="0"
        first_ent_ds = [ds for ds in art_ds if ds["parent_uid"] == "rank0000_0000"]
        for ds in first_ent_ds:
            assert ds["parameters"]["slice"] == "0"

        # Second entity's artifacts should have slice="1"
        second_ent_ds = [ds for ds in art_ds if ds["parent_uid"] == "rank0000_0001"]
        for ds in second_ent_ds:
            assert ds["parameters"]["slice"] == "1"

    def test_shared_assets_for_batched_files(self, nips3_manifests):
        """Batched files: multiple artifacts share the same HDF5 file."""
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = nips3_manifests

        _, _, art_ds = prepare_node_data(
            ent_df, art_df, max_entities=5, base_dir=base_dir
        )

        # All RIXS artifacts point to the same file
        rixs_files = {ds["h5_path"] for ds in art_ds if ds["art_key"] == "rixs"}
        assert len(rixs_files) == 1  # All share one batched file


# ─── Cross-Dataset Tests ─────────────────────────────────────────────────────


class TestGenericBehavior:
    """Tests that verify the broker is truly dataset-agnostic."""

    def test_no_hardcoded_param_names_in_metadata(self, vdp_manifests, nips3_manifests):
        """Metadata keys come from manifests, not from hardcoded lists."""
        from broker.bulk_register import prepare_node_data

        # VDP
        ent_df, art_df, base_dir = vdp_manifests
        vdp_nodes, _, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )
        vdp_meta = vdp_nodes[0]["metadata"]

        # NiPS3
        ent_df, art_df, base_dir = nips3_manifests
        nips3_nodes, _, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )
        nips3_meta = nips3_nodes[0]["metadata"]

        # Both should have uid
        assert "uid" in vdp_meta
        assert "uid" in nips3_meta

        # But different physics params (all from manifest, not hardcoded)
        vdp_params = {k for k in vdp_meta if not k.startswith(("path_", "dataset_", "index_"))}
        nips3_params = {k for k in nips3_meta if not k.startswith(("path_", "dataset_", "index_"))}

        # Only "uid" and "key" are shared (standard columns)
        shared = vdp_params & nips3_params
        assert shared == {"uid", "key"}

    def test_structure_family_correct(self, vdp_manifests):
        from broker.bulk_register import prepare_node_data
        ent_df, art_df, base_dir = vdp_manifests

        ent_nodes, art_nodes, _ = prepare_node_data(
            ent_df, art_df, max_entities=1, base_dir=base_dir
        )

        for node in ent_nodes:
            assert node["structure_family"] == "container"
        for node in art_nodes:
            assert node["structure_family"] == "array"

    def test_all_metadata_values_json_safe(self, vdp_manifests, nips3_manifests):
        """All metadata values must be JSON-serializable."""
        import json
        from broker.bulk_register import prepare_node_data

        for manifests in [vdp_manifests, nips3_manifests]:
            ent_df, art_df, base_dir = manifests
            ent_nodes, art_nodes, _ = prepare_node_data(
                ent_df, art_df, max_entities=5, base_dir=base_dir
            )
            for node in ent_nodes + art_nodes:
                # Should not raise
                json.dumps(node["metadata"])
