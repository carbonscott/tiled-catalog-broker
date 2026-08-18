# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pytest",
#     "pandas",
#     "pyarrow",
#     "h5py",
#     "numpy",
#     "tiled[server]",
# ]
# ///
"""
Tests that HTTP registration is genuinely dataset-agnostic.

Drives ``_register_one_entity`` (the single registration route, ADR-0002) against a
mock Tiled parent container and inspects what it *would* send: the entity key, its
metadata, and each artifact's key, metadata, and DataSource. Two synthetic datasets
with disjoint parameter names — VDP-style (one file per entity) and NiPS3-style
(batched, entity i is row i) — prove no parameter name, artifact type, or file layout
is hardcoded.

Uses synthetic test data from tests/testdata/. No running Tiled server needed.

Run with:
    uv run --with pytest --with pandas --with pyarrow --with h5py \
      --with 'tiled[server]' pytest tests/test_generic_registration.py -v
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

TESTDATA_DIR = Path(__file__).parent / "testdata"


@pytest.fixture
def vdp_manifests():
    """VDP-style synthetic manifests: one HDF5 file per entity, 3 artifacts each."""
    vdp_dir = TESTDATA_DIR / "vdp"
    ent_df = pd.read_parquet(vdp_dir / "vdp_entities.parquet")
    art_df = pd.read_parquet(vdp_dir / "vdp_artifacts.parquet")
    return ent_df, art_df, str(vdp_dir)


@pytest.fixture
def nips3_manifests():
    """NiPS3-style synthetic manifests: batched, 2 artifacts per entity."""
    nips3_dir = TESTDATA_DIR / "nips3"
    ent_df = pd.read_parquet(nips3_dir / "nips3_entities.parquet")
    art_df = pd.read_parquet(nips3_dir / "nips3_artifacts.parquet")
    return ent_df, art_df, str(nips3_dir)


def register_entity(manifests, row=0, dataset_key="TEST_KEY",
                    server_base_dir=None, inherited=None):
    """Register one entity against a mock parent; return what it sent to Tiled.

    Returns a namespace with ``result`` (the counter tuple), the entity's ``key`` and
    ``metadata``, and ``artifacts``: ``{artifact_key: kwargs-passed-to-.new()}``, where
    those kwargs carry ``metadata``, ``data_sources``, and ``structure_family``.
    """
    from tiled_catalog_broker.http_register import _register_one_entity

    ent_df, art_df, base_dir = manifests
    parent = MagicMock()
    parent.__contains__.return_value = False       # nothing registered yet
    ent_container = parent.create_container.return_value

    result = _register_one_entity(
        ent_df.iloc[row], list(ent_df.columns),
        art_df.groupby("uid"), list(art_df.columns),
        parent, base_dir=base_dir, server_base_dir=server_base_dir,
        dataset_key=dataset_key, inherited=inherited or {},
    )

    ent_kwargs = parent.create_container.call_args.kwargs
    return SimpleNamespace(
        result=result,
        key=ent_kwargs["key"],
        metadata=ent_kwargs["metadata"],
        artifacts={c.kwargs["key"]: c.kwargs
                   for c in ent_container.new.call_args_list},
    )


# ─── VDP-style: one file per entity ──────────────────────────────────────────


class TestVDPRegistration:
    """Registration of per-entity-file data."""

    def test_registers_entity_and_all_its_artifacts(self, vdp_manifests):
        reg = register_entity(vdp_manifests)
        assert reg.result == (1, 3, 0, 0)          # 1 entity, 3 artifacts, 0 skipped, 0 failed
        assert len(reg.artifacts) == 3

    def test_entity_key_derived_from_uid(self, vdp_manifests):
        """Entity keys are derived from dataset_key + uid at registration time."""
        ent_df, _, _ = vdp_manifests
        for i in range(len(ent_df)):
            reg = register_entity(vdp_manifests, row=i)
            uid = str(ent_df.iloc[i]["uid"])
            assert reg.key == f"TEST_KEY_{uid[:13]}"

    def test_entity_metadata_has_vdp_params(self, vdp_manifests):
        """VDP metadata carries Ja_meV, Jb_meV, ... — read dynamically from the manifest."""
        meta = register_entity(vdp_manifests).metadata
        assert "uid" in meta
        for param in ("Ja_meV", "Jb_meV", "Jc_meV", "Dc_meV", "spin_s", "g_factor"):
            assert param in meta

    def test_entity_metadata_has_locators(self, vdp_manifests):
        """Locators (path_, dataset_) stored in entity metadata for Mode A."""
        meta = register_entity(vdp_manifests).metadata
        for art_type in ("mh_powder_30T", "gs_state", "ins_12meV"):
            assert f"path_{art_type}" in meta
            assert f"dataset_{art_type}" in meta

        # Per-entity files carry no index — that's a batched-layout locator.
        assert [k for k in meta if k.startswith("index_")] == []

    def test_artifact_keys_match_types(self, vdp_manifests):
        """Artifact keys come directly from the manifest's type column."""
        reg = register_entity(vdp_manifests)
        assert set(reg.artifacts) == {"mh_powder_30T", "gs_state", "ins_12meV"}

    def test_artifact_shapes_read_from_hdf5(self, vdp_manifests):
        """Shapes are read from the actual HDF5 files, not hardcoded."""
        reg = register_entity(vdp_manifests)
        shapes = {k: v["metadata"]["shape"] for k, v in reg.artifacts.items()}
        assert shapes["mh_powder_30T"] == [10]
        assert shapes["gs_state"] == [3, 4]
        assert shapes["ins_12meV"] == [6, 5]

    def test_data_source_carries_dataset_path(self, vdp_manifests):
        """Each DataSource carries the HDF5 path from the manifest."""
        reg = register_entity(vdp_manifests)
        params = {k: v["data_sources"][0].parameters
                  for k, v in reg.artifacts.items()}
        assert params["mh_powder_30T"]["dataset"] == "/curve/M_parallel"
        assert params["gs_state"]["dataset"] == "/gs/spin_dir"
        assert params["ins_12meV"]["dataset"] == "/ins/broadened"

    def test_per_entity_files_have_no_slice(self, vdp_manifests):
        """`slice` is a batched-layout parameter; per-entity files omit it."""
        reg = register_entity(vdp_manifests)
        for kwargs in reg.artifacts.values():
            assert "slice" not in kwargs["data_sources"][0].parameters

    def test_existing_entity_is_skipped(self, vdp_manifests):
        """Registration is incremental: an entity already on the server is skipped."""
        from tiled_catalog_broker.http_register import _register_one_entity

        ent_df, art_df, base_dir = vdp_manifests
        parent = MagicMock()
        parent.__contains__.return_value = True    # already registered

        result = _register_one_entity(
            ent_df.iloc[0], list(ent_df.columns),
            art_df.groupby("uid"), list(art_df.columns),
            parent, base_dir=base_dir, server_base_dir=None,
            dataset_key="TEST_KEY",
        )

        assert result == (0, 0, 1, 0)              # counted as skipped
        parent.create_container.assert_not_called()


# ─── NiPS3-style: batched files ──────────────────────────────────────────────


class TestNiPS3Registration:
    """Registration of batched data — entity i is row i of a shared dataset."""

    def test_registers_entity_and_all_its_artifacts(self, nips3_manifests):
        reg = register_entity(nips3_manifests)
        assert reg.result == (1, 2, 0, 0)
        assert set(reg.artifacts) == {"rixs", "mag"}

    def test_entity_metadata_has_nips3_params(self, nips3_manifests):
        """NiPS3 metadata carries its own parameter names — and none of VDP's."""
        meta = register_entity(nips3_manifests).metadata
        assert "uid" in meta
        for param in ("F2_dd", "F2_dp", "F4_dd", "G1_dp", "G3_dp"):
            assert param in meta
        assert "Ja_meV" not in meta
        assert "Jb_meV" not in meta

    def test_entity_metadata_has_index_locators(self, nips3_manifests):
        """Batched layouts add an index_ locator — the row on axis 0."""
        meta = register_entity(nips3_manifests).metadata
        for art_type in ("rixs", "mag"):
            assert f"path_{art_type}" in meta
            assert f"dataset_{art_type}" in meta
            assert f"index_{art_type}" in meta
        assert meta["index_rixs"] == 0             # first entity

    def test_batched_shapes_drop_the_batch_dimension(self, nips3_manifests):
        """A batched artifact registers its per-entity shape, not the stacked one."""
        reg = register_entity(nips3_manifests)
        shapes = {k: v["metadata"]["shape"] for k, v in reg.artifacts.items()}
        assert shapes["rixs"] == [6, 5]            # (5, 6, 5) -> (6, 5) per entity
        assert shapes["mag"] == [10]               # (5, 10)   -> (10,) per entity

    def test_data_source_slice_tracks_the_entity_row(self, nips3_manifests):
        """Each entity's DataSource slices its own row out of the shared dataset."""
        for row, expected in [(0, "0"), (1, "1")]:
            reg = register_entity(nips3_manifests, row=row)
            for kwargs in reg.artifacts.values():
                assert kwargs["data_sources"][0].parameters["slice"] == expected

    def test_entities_share_one_asset_file(self, nips3_manifests):
        """Batched entities all point at the same HDF5 file (one asset, many slices)."""
        uris = set()
        for row in range(3):
            reg = register_entity(nips3_manifests, row=row)
            for kwargs in reg.artifacts.values():
                uris.update(a.data_uri for a in kwargs["data_sources"][0].assets)
        assert len(uris) == 1


# ─── Cross-dataset: the dataset-agnostic guarantee ───────────────────────────


class TestGenericBehavior:
    """The broker hardcodes no parameter name, artifact type, or file layout."""

    def test_no_hardcoded_param_names_in_metadata(self, vdp_manifests, nips3_manifests):
        """Two datasets, disjoint parameters — only `uid` is common."""
        def params_of(manifests):
            meta = register_entity(manifests).metadata
            return {k for k in meta
                    if not k.startswith(("path_", "dataset_", "index_"))}

        # `uid` is the one standard column kept in metadata; the entity key is
        # derived from (dataset_key, uid) rather than stored.
        assert params_of(vdp_manifests) & params_of(nips3_manifests) == {"uid"}

    def test_structure_family_is_array(self, vdp_manifests):
        from tiled.structures.core import StructureFamily
        reg = register_entity(vdp_manifests)
        for kwargs in reg.artifacts.values():
            assert kwargs["structure_family"] == StructureFamily.array

    def test_all_metadata_values_json_safe(self, vdp_manifests, nips3_manifests):
        """Everything sent to Tiled must survive JSON serialization."""
        import json

        for manifests in (vdp_manifests, nips3_manifests):
            ent_df, _, _ = manifests
            for row in range(len(ent_df)):
                reg = register_entity(manifests, row=row)
                json.dumps(reg.metadata)           # should not raise
                for kwargs in reg.artifacts.values():
                    json.dumps(kwargs["metadata"])

    @pytest.mark.parametrize("dtype", ["float32", "int32", "float64"])
    def test_dtype_survives_generate_to_register(self, tmp_path, dtype):
        """dtype is captured from the file at generate time and carried through.

        End-to-end because that is now the whole guarantee: registration never
        opens HDF5, so if `tcb generate` doesn't record the real dtype nothing
        downstream can recover it. The read adapter re-validates dtype against
        the file and raises on a mismatch, so a wrong value registers cleanly
        and then fails *every* read — a failure mode no float64 fixture catches.
        """
        import h5py
        import numpy as np
        from ruamel.yaml import YAML
        from tiled_catalog_broker.tools.generate import generate_manifests

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with h5py.File(data_dir / "e0.h5", "w") as f:
            f.create_dataset("spectrum", data=np.ones((4, 3), dtype=dtype))
            f.create_dataset("sigma", data=0.04)

        yaml_path = tmp_path / "ds.yml"
        with open(yaml_path, "w") as fh:
            YAML().dump({
                "label": "Dtype Probe",
                "key": "DTYPE_PROBE",
                "metadata": {"method": ["RIXS"], "data_type": "simulation",
                             "material": "NiPS3", "producer": "edrixs"},
                "data": {"directory": str(data_dir), "layout": "per_entity",
                         "file_pattern": "*.h5"},
                "artifacts": [{"type": "spectrum", "dataset": "/spectrum"}],
                "parameters": {"location": "root_scalars"},
            }, fh)

        ent_path, art_path = generate_manifests(
            str(yaml_path), output_dir=str(tmp_path / "manifests"))

        art_df = pd.read_parquet(art_path)
        assert art_df.loc[0, "dtype"] == dtype          # captured at generate time

        reg = register_entity(
            (pd.read_parquet(ent_path), art_df, str(data_dir)))
        art = reg.artifacts["spectrum"]

        assert art["metadata"]["dtype"] == dtype        # ...and carried through
        assert art["metadata"]["shape"] == [4, 3]
        registered = art["data_sources"][0].structure.data_type.to_numpy_dtype()
        assert registered == np.dtype(dtype)

    def test_manifest_without_shape_dtype_is_rejected(self, vdp_manifests):
        """A manifest predating shape/dtype capture fails loudly, not silently."""
        from tiled_catalog_broker.http_register import require_shape_dtype

        _, art_df, _ = vdp_manifests
        require_shape_dtype(art_df)                     # current manifests are fine

        with pytest.raises(ValueError, match="tcb generate"):
            require_shape_dtype(art_df.drop(columns=["shape", "dtype"]))

    def test_server_base_dir_overrides_asset_uri(self, vdp_manifests):
        """When the server mounts the data elsewhere, the asset URI follows it."""
        reg = register_entity(vdp_manifests, server_base_dir="/mnt/server-view")
        for kwargs in reg.artifacts.values():
            for asset in kwargs["data_sources"][0].assets:
                assert asset.data_uri.startswith("file://localhost/mnt/server-view/")


# ─── INHERITED_KEYS propagation ──────────────────────────────────────────────


class TestInheritedKeys:
    """Inherited dataset keys land on every entity and artifact node."""

    def test_inherited_keys_propagate_to_entity_and_artifact(self, vdp_manifests):
        reg = register_entity(vdp_manifests, inherited={"amsc_public": True})
        assert reg.metadata["amsc_public"] is True
        for kwargs in reg.artifacts.values():
            assert kwargs["metadata"]["amsc_public"] is True

    def test_manifest_value_wins_over_inherited(self, vdp_manifests):
        """setdefault semantics — a manifest column of the same name wins."""
        ent_df, art_df, base_dir = vdp_manifests
        ent_df = ent_df.assign(amsc_public=False)

        reg = register_entity(
            (ent_df, art_df, base_dir), inherited={"amsc_public": True})
        assert reg.metadata["amsc_public"] is False
