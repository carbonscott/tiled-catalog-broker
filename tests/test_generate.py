# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "h5py",
#     "numpy",
#     "pandas",
#     "pyarrow",
#     "ruamel.yaml",
# ]
# ///
"""
Unit tests for generate module.

Tests cover manifest generation for batched and per-entity layouts,
root-attribute parameter extraction, shared-axis capture/exclusion, and
YAML validation errors during loading.

Run with:
    uv run --with pytest --with h5py --with numpy --with pandas \
        --with pyarrow --with 'ruamel.yaml' \
        pytest tests/test_generate.py -v
"""

import os
from pathlib import Path

import json

import h5py
import numpy as np
import pandas as pd
import pytest
from ruamel.yaml import YAML

# Add project root to path for package imports
from tiled_catalog_broker.tools.generate import generate_manifests, load_yaml, _make_uid
from pydantic import ValidationError


# ---------------------------------------------------------------------------
# Helper to write YAML config files
# ---------------------------------------------------------------------------

def _write_yaml(path, cfg):
    """Write a dict as YAML to a file path."""
    yaml = YAML()
    with open(path, "w") as f:
        yaml.dump(cfg, f)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def batched_setup(tmp_path):
    """Create a batched HDF5 file and matching YAML config.

    HDF5 layout:
        /params/alpha  (3,)
        /params/beta   (3,)
        /spectra       (3, 4)
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    fpath = data_dir / "batch.h5"
    with h5py.File(fpath, "w") as f:
        g = f.create_group("params")
        g.create_dataset("alpha", data=np.array([1.0, 2.0, 3.0]))
        g.create_dataset("beta", data=np.array([0.1, 0.2, 0.3]))
        f.create_dataset("spectra", data=np.random.randn(3, 4))

    cfg = {
        "label": "test_batched",
        "key": "TEST_SIM_BATCHED",
        "data": {
            "directory": str(data_dir),
            "layout": "batched",
            "file_pattern": "*.h5",
        },
        "artifacts": [
            {"type": "spectra", "dataset": "/spectra"},
        ],
        "parameters": {
            "location": "group",
            "group": "/params",
        },
        "metadata": {
            "method": ["RIXS"],
            "data_type": "simulation",
            "material": "NiPS3",
            "producer": "edrixs",
        },
    }

    yaml_path = tmp_path / "batched.yml"
    _write_yaml(yaml_path, cfg)

    return yaml_path, data_dir


@pytest.fixture
def per_entity_setup(tmp_path):
    """Create 3 per-entity HDF5 files and matching YAML config.

    Each file has scalar params at root and a 1D spectrum array.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    for i in range(3):
        fpath = data_dir / f"entity_{i:03d}.h5"
        with h5py.File(fpath, "w") as f:
            f.create_dataset("param_a", data=float(i) * 1.5)
            f.create_dataset("param_b", data=float(i) * 0.3)
            f.create_dataset("spectrum", data=np.random.randn(10))

    cfg = {
        "label": "test_per_entity",
        "key": "TEST_SIM_PER_ENTITY",
        "data": {
            "directory": str(data_dir),
            "layout": "per_entity",
            "file_pattern": "*.h5",
        },
        "artifacts": [
            {"type": "spectrum", "dataset": "/spectrum"},
        ],
        "parameters": {
            "location": "root_scalars",
        },
        "metadata": {
            "method": ["RIXS"],
            "data_type": "simulation",
            "material": "NiPS3",
            "producer": "edrixs",
        },
    }

    yaml_path = tmp_path / "per_entity.yml"
    _write_yaml(yaml_path, cfg)

    return yaml_path, data_dir


@pytest.fixture
def root_attributes_setup(tmp_path):
    """Create a per-entity HDF5 file with root attributes as params.

    Params are stored as file-level HDF5 attributes (f.attrs).
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    for i in range(2):
        fpath = data_dir / f"sample_{i:03d}.h5"
        with h5py.File(fpath, "w") as f:
            f.attrs["temperature"] = 300.0 + i * 10.0
            f.attrs["pressure"] = 1.0 + i * 0.5
            f.create_dataset("spectrum", data=np.random.randn(8))

    cfg = {
        "label": "test_root_attrs",
        "key": "TEST_SIM_ROOT_ATTRS",
        "data": {
            "directory": str(data_dir),
            "layout": "per_entity",
            "file_pattern": "*.h5",
        },
        "artifacts": [
            {"type": "spectrum", "dataset": "/spectrum"},
        ],
        "parameters": {
            "location": "root_attributes",
        },
        "metadata": {
            "method": ["RIXS"],
            "data_type": "simulation",
            "material": "NiPS3",
            "producer": "edrixs",
        },
    }

    yaml_path = tmp_path / "root_attrs.yml"
    _write_yaml(yaml_path, cfg)

    return yaml_path, data_dir


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGenerateBatched:
    """Tests for generate_manifests with batched layout."""

    def test_generate_batched(self, batched_setup):
        """Create a batched HDF5 + YAML, run generate_manifests, check output."""
        yaml_path, data_dir = batched_setup
        output_dir = data_dir.parent / "manifests" / "test_batched"

        ent_path, art_path = generate_manifests(
            str(yaml_path), output_dir=str(output_dir)
        )

        assert os.path.exists(ent_path)
        assert os.path.exists(art_path)

        ent_df = pd.read_parquet(ent_path)
        art_df = pd.read_parquet(art_path)

        # Batched artifacts register per-entity: the leading (entity) axis is
        # already dropped in the manifest, so registration needs no adjustment.
        with h5py.File(next(data_dir.glob("*.h5"))) as f:
            on_disk = f[art_df.loc[0, "dataset"]]
            assert json.loads(art_df.loc[0, "shape"]) == list(on_disk.shape[1:])
            assert art_df.loc[0, "dtype"] == str(on_disk.dtype)

        # 3 entities from batch size of 3
        assert len(ent_df) == 3
        # 3 entities x 1 artifact type = 3 artifact rows
        assert len(art_df) == 3

        # Check parameter columns exist
        assert "alpha" in ent_df.columns
        assert "beta" in ent_df.columns
        assert "uid" in ent_df.columns
        # `key` column removed — entity keys are derived at registration from
        # (dataset_key, uid). The manifest holds the uid only.
        assert "key" not in ent_df.columns

        # Check artifact columns
        assert "uid" in art_df.columns
        assert "type" in art_df.columns
        assert "file" in art_df.columns
        assert "dataset" in art_df.columns
        assert "index" in art_df.columns

        # All artifact types should be "spectra"
        assert (art_df["type"] == "spectra").all()


class TestGeneratePerEntity:
    """Tests for generate_manifests with per-entity layout."""

    def test_generate_per_entity(self, per_entity_setup):
        """Create 3 per-entity HDF5 files + YAML, check output."""
        yaml_path, data_dir = per_entity_setup
        output_dir = data_dir.parent / "manifests" / "test_per_entity"

        ent_path, art_path = generate_manifests(
            str(yaml_path), output_dir=str(output_dir)
        )

        ent_df = pd.read_parquet(ent_path)
        art_df = pd.read_parquet(art_path)

        # 3 files = 3 entities
        assert len(ent_df) == 3
        # 3 entities x 1 artifact = 3 artifact rows
        assert len(art_df) == 3

        # Check scalar parameters were extracted
        assert "param_a" in ent_df.columns
        assert "param_b" in ent_df.columns

        # Verify parameter values
        param_a_values = sorted(ent_df["param_a"].tolist())
        assert param_a_values == pytest.approx([0.0, 1.5, 3.0])


class TestGenerateRootAttributes:
    """Tests for generate_manifests with root_attributes parameter location."""

    def test_generate_root_attributes(self, root_attributes_setup):
        """Create HDF5 with root attributes as params, verify they appear in entity manifest."""
        yaml_path, data_dir = root_attributes_setup
        output_dir = data_dir.parent / "manifests" / "test_root_attrs"

        ent_path, art_path = generate_manifests(
            str(yaml_path), output_dir=str(output_dir)
        )

        ent_df = pd.read_parquet(ent_path)

        assert len(ent_df) == 2
        assert "temperature" in ent_df.columns
        assert "pressure" in ent_df.columns

        temps = sorted(ent_df["temperature"].tolist())
        assert temps == pytest.approx([300.0, 310.0])


class TestGenerateSharedExcluded:
    """Tests for shared axes exclusion from entity columns."""

    def test_generate_shared_excluded(self, tmp_path):
        """Shared axes don't appear as entity columns."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        fpath = data_dir / "batch.h5"
        with h5py.File(fpath, "w") as f:
            g = f.create_group("params")
            g.create_dataset("alpha", data=np.array([1.0, 2.0, 3.0]))
            f.create_dataset("spectra", data=np.random.randn(3, 4))
            # Shared axis: energy grid (not batched)
            f.create_dataset("energy", data=np.linspace(0, 10, 4))

        cfg = {
            "label": "test_shared",
            "key": "TEST_SIM_SHARED",
            "data": {
                "directory": str(data_dir),
                "layout": "batched",
                "file_pattern": "*.h5",
            },
            "artifacts": [
                {"type": "spectra", "dataset": "/spectra"},
            ],
            "shared": [
                {"type": "energy", "dataset": "/energy"},
            ],
            "parameters": {
                "location": "group",
                "group": "/params",
            },
            "metadata": {
                "method": ["RIXS"],
                "data_type": "simulation",
                "material": "NiPS3",
                "producer": "edrixs",
            },
        }

        yaml_path = tmp_path / "shared.yml"
        _write_yaml(yaml_path, cfg)

        output_dir = tmp_path / "manifests" / "test_shared"
        ent_path, art_path = generate_manifests(
            str(yaml_path), output_dir=str(output_dir)
        )

        ent_df = pd.read_parquet(ent_path)

        # "energy" is a shared axis, not a parameter — it should NOT be in entity columns
        assert "energy" not in ent_df.columns
        # But the parameter should be there
        assert "alpha" in ent_df.columns

        # ...and it IS captured once in the artifact manifest as a row with no uid
        # (an artifact of the dataset, not of an entity), with shape/dtype read
        # from the file so registration never has to open it.
        art_df = pd.read_parquet(art_path)
        shared = art_df[art_df["uid"].isna()]
        assert len(shared) == 1
        assert len(art_df) == 4                      # 3 entity artifacts + 1 shared axis
        row = shared.iloc[0]
        assert row["type"] == "energy"
        assert row["file"] == "batch.h5"
        assert row["dataset"] == "/energy"
        assert pd.isna(row["index"])
        assert json.loads(row["shape"]) == [4]
        assert row["dtype"] == "float64"
        assert not (output_dir / "shared.parquet").exists()

    def test_no_shared_no_dataset_rows(self, tmp_path):
        """A YAML without `shared:` has no null-uid rows in the artifact manifest."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with h5py.File(data_dir / "e0.h5", "w") as f:
            f.create_dataset("spectrum", data=np.ones(3))
            f.create_dataset("sigma", data=0.1)
        cfg = {
            "label": "no shared", "key": "NO_SHARED",
            "data": {"directory": str(data_dir), "layout": "per_entity", "file_pattern": "*.h5"},
            "artifacts": [{"type": "spectrum", "dataset": "/spectrum"}],
            "parameters": {"location": "root_scalars"},
            "metadata": {"method": ["RIXS"], "data_type": "simulation", "material": "NiPS3"},
        }
        yaml_path = tmp_path / "ns.yml"
        _write_yaml(yaml_path, cfg)
        out = tmp_path / "m"
        _, art_path = generate_manifests(str(yaml_path), output_dir=str(out))
        art_df = pd.read_parquet(art_path)
        assert art_df["uid"].notna().all()
        assert not (out / "shared.parquet").exists()

    def test_missing_shared_dataset_is_an_error(self, tmp_path):
        """A declared shared axis that no file holds fails generate loudly."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with h5py.File(data_dir / "e0.h5", "w") as f:
            f.create_dataset("spectrum", data=np.ones(3))
            f.create_dataset("sigma", data=0.1)
        cfg = {
            "label": "bad shared", "key": "BAD_SHARED",
            "data": {"directory": str(data_dir), "layout": "per_entity", "file_pattern": "*.h5"},
            "artifacts": [{"type": "spectrum", "dataset": "/spectrum"}],
            "shared": [{"type": "energy", "dataset": "/nope"}],
            "parameters": {"location": "root_scalars"},
            "metadata": {"method": ["RIXS"], "data_type": "simulation", "material": "NiPS3"},
        }
        yaml_path = tmp_path / "bad.yml"
        _write_yaml(yaml_path, cfg)
        with pytest.raises(KeyError, match="shared axis type='energy'"):
            generate_manifests(str(yaml_path), output_dir=str(tmp_path / "m"))


class TestGenerateGrouped:
    """Tests for generate_manifests with grouped layout (one HDF5 group/entity)."""

    def test_generate_grouped(self, tmp_path):
        """Entities come from subgroups; source_group + full dataset paths emitted."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with h5py.File(data_dir / "file.h5", "w") as f:
            samples = f.create_group("samples")
            for i in range(3):
                g = samples.create_group(f"sample_{i:03d}")
                p = g.create_group("params")
                p.create_dataset("alpha", data=float(i))
                g.create_dataset("spectrum", data=np.arange(5.0))

        cfg = {
            "label": "test_grouped", "key": "TEST_SIM_GROUPED",
            "data": {"directory": str(data_dir), "layout": "grouped",
                     "file_pattern": "*.h5"},
            "artifacts": [{"type": "spectrum", "dataset": "spectrum"}],
            "parameters": {"location": "group_scalars",
                           "entity_group": "samples", "group": "params"},
            "metadata": {"method": ["RIXS"], "data_type": "simulation",
                         "material": "NiPS3", "producer": "edrixs"},
        }
        yaml_path = tmp_path / "grouped.yml"
        _write_yaml(yaml_path, cfg)

        ent_path, art_path = generate_manifests(
            str(yaml_path), output_dir=str(tmp_path / "manifests" / "g"))
        ent_df = pd.read_parquet(ent_path)
        art_df = pd.read_parquet(art_path)

        assert len(ent_df) == 3
        assert "source_group" in ent_df.columns
        assert sorted(ent_df["source_group"]) == [
            "samples/sample_000", "samples/sample_001", "samples/sample_002"]
        assert "alpha" in ent_df.columns
        # Artifact dataset paths are resolved within each entity's group
        assert "/samples/sample_000/spectrum" in set(art_df["dataset"])
        # Shape/dtype are captured here so registration never reopens the file
        assert json.loads(art_df.loc[0, "shape"]) == [5]
        assert art_df.loc[0, "dtype"] == "float64"

    def test_grouped_missing_dataset_fails_loudly(self, tmp_path):
        """A dataset path that isn't inside the entity group errors at generate time.

        Catching it here keeps unreachable paths out of the manifest, where they
        would otherwise surface only as an HTTP 500 on first read.
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with h5py.File(data_dir / "file.h5", "w") as f:
            g = f.create_group("samples").create_group("sample_000")
            g.create_group("params").create_dataset("alpha", data=1.0)
            g.create_dataset("spectrum", data=np.arange(5.0))

        cfg = {
            "label": "test_grouped_bad", "key": "TEST_SIM_GROUPED_BAD",
            "data": {"directory": str(data_dir), "layout": "grouped",
                     "file_pattern": "*.h5"},
            # absolute path — the classic mistake; it is resolved *within* the group
            "artifacts": [{"type": "spectrum", "dataset": "/samples/sample_000/spectrum"}],
            "parameters": {"location": "group_scalars",
                           "entity_group": "samples", "group": "params"},
            "metadata": {"method": ["RIXS"], "data_type": "simulation",
                         "material": "NiPS3", "producer": "edrixs"},
        }
        yaml_path = tmp_path / "grouped_bad.yml"
        _write_yaml(yaml_path, cfg)

        with pytest.raises(KeyError, match="relative to the group"):
            generate_manifests(str(yaml_path),
                               output_dir=str(tmp_path / "manifests" / "gb"))


class TestLoadYaml:
    """Tests for load_yaml()."""

    def test_load_yaml_validation_error(self, tmp_path):
        """Invalid YAML (missing key) raises ValidationError."""
        cfg = {
            # Missing label, key, data, artifacts
            "metadata": {"method": ["RIXS"]},
        }
        yaml_path = tmp_path / "invalid.yml"
        _write_yaml(yaml_path, cfg)

        with pytest.raises(ValidationError):
            load_yaml(str(yaml_path))


class TestMakeUid:
    """Tests for _make_uid — the headline content-addressing contract.

    Same params -> same UID, regardless of file order, reshards, or
    regeneration. Float-LSB drift tolerated. Namespace separates
    otherwise-identical param sets across datasets.
    """

    def test_param_reorder_stability(self):
        """Same params in different insertion order hash to the same UID."""
        a = _make_uid({"Ja_meV": 0.5, "Jb_meV": 1.0, "spin_s": 0.5}, namespace="VDP")
        b = _make_uid({"spin_s": 0.5, "Jb_meV": 1.0, "Ja_meV": 0.5}, namespace="VDP")
        assert a == b

    def test_namespace_separation(self):
        """Identical params under different namespaces produce different UIDs."""
        params = {"x": 1, "y": 2}
        assert _make_uid(params, namespace="VDP") != _make_uid(params, namespace="EDRIXS")

    def test_float_lsb_tolerance(self):
        """Float drift below the 12-decimal rounding threshold is ignored."""
        # 0.1 + 0.2 = 0.30000000000000004 -> rounds to 0.3 at 12 decimals
        assert _make_uid({"x": 0.1 + 0.2}) == _make_uid({"x": 0.3})

    def test_string_fallback_deterministic(self):
        """Positional string fallback is deterministic (same input -> same UID)."""
        s = "VDP_aaaa0007"
        assert _make_uid(s) == _make_uid(s)
        # And distinct strings produce distinct UIDs.
        assert _make_uid(s) != _make_uid("VDP_aaaa0008")


# ---------------------------------------------------------------------------
# parameters.groups — several HDF5 groups → nested entity metadata
# ---------------------------------------------------------------------------

_META = {"method": ["INS"], "data_type": "experimental", "material": "NiPS3"}


def _nexus_file(path, ei, temperature):
    """A NeXus-shaped per-entity file: scalars spread over several groups, with
    `units`/`long_name` attributes, a large string blob, a nested subgroup, and
    one NXdata-style group of arrays. Nothing NeXus-specific is *required* by the
    contract — this is simply the motivating shape."""
    with h5py.File(path, "w") as f:
        entry = f.create_group("entry")
        inst = entry.create_group("instrument")
        inst.create_dataset("Ei", data=ei).attrs["units"] = "meV"
        inst.create_dataset("name", data="SEQUOIA")
        inst.create_dataset("BL17:SEEMeta:JSON", data="{" + "x" * 2000 + "}")
        inst.create_group("detector").create_dataset("distance", data=5.5)
        entry.create_group("sample").create_dataset("chemical_formula", data="NiPS3")
        t = entry.create_group("parameters").create_dataset("temperature", data=temperature)
        t.attrs["units"] = "K"
        data = entry.create_group("data")
        data.create_dataset("data", data=np.zeros((4, 3))).attrs["long_name"] = "Intensity"
        data.create_dataset("E", data=np.arange(4.0) * ei).attrs["units"] = "meV"


@pytest.fixture
def nexus_setup(tmp_path):
    """Two NeXus-shaped files (Ei = 30, 60 meV) + a `groups:` YAML."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    for ei, t in ((30.0, 5.0), (60.0, 6.0)):
        _nexus_file(data_dir / f"Ei{int(ei)}.h5", ei, t)
    cfg = {
        "label": "nexus test", "key": "NEXUS_TEST", "metadata": dict(_META),
        "data": {"directory": str(data_dir), "layout": "per_entity", "file_pattern": "*.h5"},
        "parameters": {
            "location": "group",
            "groups": {"instrument": "/entry/instrument", "sample": "/entry/sample",
                       "parameters": "/entry/parameters"},
            "exclude": ["/entry/instrument/BL17:SEEMeta:JSON"],
        },
        "artifacts": [{"type": "data", "dataset": "/entry/data/data"},
                      {"type": "E", "dataset": "/entry/data/E"}],
    }
    return tmp_path, cfg


def _generate(tmp_path, cfg, name="ds"):
    yaml_path = tmp_path / f"{name}.yml"
    _write_yaml(yaml_path, cfg)
    ent_path, art_path = generate_manifests(str(yaml_path), output_dir=str(tmp_path / "m" / name))
    return pd.read_parquet(ent_path), pd.read_parquet(art_path)


class TestParameterGroups:

    def test_groups_become_dotted_columns(self, nexus_setup):
        """Each group's scalars land as `<group>.<field>`; arrays are not parameters."""
        ent_df, _ = _generate(*nexus_setup)
        assert len(ent_df) == 2
        row = ent_df.sort_values("instrument.Ei").iloc[0]
        assert row["instrument.Ei"] == 30.0
        assert row["instrument.name"] == "SEQUOIA"
        assert row["sample.chemical_formula"] == "NiPS3"
        assert row["parameters.temperature"] == 5.0
        assert not any(c.startswith("entry") for c in ent_df.columns)   # only named groups
        assert "data" not in ent_df.columns and "E" not in ent_df.columns

    def test_exclude_and_non_recursive_default(self, nexus_setup):
        ent_df, _ = _generate(*nexus_setup)
        assert not any("SEEMeta" in c for c in ent_df.columns)            # excluded
        assert "instrument.detector.distance" not in ent_df.columns       # not recursive

    def test_recursive_descends_into_subgroups(self, nexus_setup):
        tmp_path, cfg = nexus_setup
        cfg["parameters"]["recursive"] = True
        ent_df, _ = _generate(tmp_path, cfg)
        assert (ent_df["instrument.detector.distance"] == 5.5).all()

    def test_exclude_can_drop_a_subgroup(self, nexus_setup):
        tmp_path, cfg = nexus_setup
        cfg["parameters"]["recursive"] = True
        cfg["parameters"]["exclude"].append("/entry/instrument/detector")
        ent_df, _ = _generate(tmp_path, cfg)
        assert "instrument.detector.distance" not in ent_df.columns

    def test_field_attrs_ride_along_as_labels(self, nexus_setup):
        """A field's scalar HDF5 attributes become `<field>_<attr>` siblings."""
        ent_df, _ = _generate(*nexus_setup)
        assert (ent_df["instrument.Ei_units"] == "meV").all()
        assert (ent_df["parameters.temperature_units"] == "K").all()
        assert "instrument.name_units" not in ent_df.columns              # only where present

    def test_labels_do_not_enter_the_uid(self, nexus_setup):
        """The UID hashes parameter values only — the dotted keys, no `_units` labels."""
        ent_df, _ = _generate(*nexus_setup)
        row = ent_df.sort_values("instrument.Ei").iloc[0]
        expected = _make_uid({
            "instrument.Ei": 30.0, "instrument.name": "SEQUOIA",
            "parameters.temperature": 5.0, "sample.chemical_formula": "NiPS3",
        }, namespace="NEXUS_TEST")
        assert row["uid"] == expected

    def test_artifact_attrs_become_manifest_columns(self, nexus_setup):
        """An artifact dataset's own attributes are columns → array-node metadata."""
        _, art_df = _generate(*nexus_setup)
        by_type = art_df.groupby("type").first()
        assert by_type.loc["data", "long_name"] == "Intensity"
        assert by_type.loc["E", "units"] == "meV"
        assert pd.isna(by_type.loc["data", "units"])                      # absent → null

    def test_single_group_stays_flat(self, nexus_setup):
        """`group:` (one group) keeps bare field names — unchanged behaviour."""
        tmp_path, cfg = nexus_setup
        cfg["parameters"] = {"location": "group", "group": "/entry/sample"}
        ent_df, _ = _generate(tmp_path, cfg)
        assert "chemical_formula" in ent_df.columns

    def test_batched_groups_one_row_per_entity(self, tmp_path):
        """Batched: (N,) datasets are per-entity columns, 0-dim are broadcast."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with h5py.File(data_dir / "b.h5", "w") as f:
            f.create_dataset("spectra", data=np.zeros((3, 4)))
            f.create_group("params").create_dataset("alpha", data=[1.0, 2.0, 3.0])
            m = f.create_group("meta")
            m.create_dataset("temperature", data=[5.0, 6.0, 7.0]).attrs["units"] = "K"
            m.create_dataset("setup", data="cryo")                       # constant for the file
            m.create_dataset("wrong_len", data=[1.0, 2.0])               # not one row per entity
        cfg = {
            "label": "bg", "key": "BG", "metadata": dict(_META),
            "data": {"directory": str(data_dir), "layout": "batched", "file_pattern": "*.h5"},
            "parameters": {"location": "group", "groups": {"p": "/params", "m": "/meta"}},
            "artifacts": [{"type": "spectra", "dataset": "/spectra"}],
        }
        ent_df, _ = _generate(tmp_path, cfg)
        assert sorted(ent_df["p.alpha"]) == [1.0, 2.0, 3.0]
        assert sorted(ent_df["m.temperature"]) == [5.0, 6.0, 7.0]
        assert (ent_df["m.temperature_units"] == "K").all()
        assert (ent_df["m.setup"] == "cryo").all()
        assert "m.wrong_len" not in ent_df.columns

    def test_grouped_groups_are_entity_relative(self, tmp_path):
        """Grouped: group paths resolve inside each entity group."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with h5py.File(data_dir / "g.h5", "w") as f:
            for i in range(2):
                g = f.create_group(f"samples/sample_{i}")
                g.create_group("params").create_dataset("alpha", data=float(i))
                g.create_group("sample").create_dataset("formula", data="NiPS3")
                g.create_dataset("spectrum", data=np.zeros(5))
        cfg = {
            "label": "gg", "key": "GG", "metadata": dict(_META),
            "data": {"directory": str(data_dir), "layout": "grouped", "file_pattern": "*.h5"},
            "parameters": {"location": "group_scalars", "entity_group": "samples",
                           "groups": {"params": "params", "sample": "sample"}},
            "artifacts": [{"type": "spectrum", "dataset": "spectrum"}],
        }
        ent_df, _ = _generate(tmp_path, cfg)
        assert sorted(ent_df["params.alpha"]) == [0.0, 1.0]
        assert (ent_df["sample.formula"] == "NiPS3").all()

    def test_groups_validation(self, nexus_setup):
        tmp_path, cfg = nexus_setup
        cfg["parameters"]["group"] = "/entry/sample"                        # group AND groups
        with pytest.raises(ValidationError, match="mutually exclusive"):
            _generate(tmp_path, cfg)
        cfg["parameters"] = {"location": "root_scalars", "groups": {"s": "/entry/sample"}}
        with pytest.raises(ValidationError, match="do not apply"):
            _generate(tmp_path, cfg)
        cfg["parameters"] = {"location": "group"}
        with pytest.raises(ValidationError, match="required"):
            _generate(tmp_path, cfg)

    def test_extra_metadata_is_ignored_with_a_warning(self, nexus_setup, capsys):
        tmp_path, cfg = nexus_setup
        cfg["extra_metadata"] = [{"dataset": "/entry/title"}]
        ent_df, _ = _generate(tmp_path, cfg)
        assert "extra_metadata" in capsys.readouterr().out
        assert "title" not in ent_df.columns and "entry/title" not in ent_df.columns


class TestLinks:
    """HDF5 links are resolved by the library, not parsed by the broker: a linked
    dataset reads like any other, a dangling link is skipped, not fatal."""

    def test_soft_and_external_links_read_transparently(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with h5py.File(data_dir / "raw.h5", "w") as raw:
            raw.create_dataset("Ei", data=60.0).attrs["units"] = "meV"
            raw.create_dataset("image", data=np.ones((2, 3)))
        with h5py.File(data_dir / "e0.h5", "w") as f:
            f.create_dataset("raw/Ei", data=30.0).attrs["units"] = "meV"
            f.create_dataset("raw/image", data=np.zeros((2, 3)))
            inst = f.create_group("entry/instrument")
            inst["Ei"] = h5py.SoftLink("/raw/Ei")                          # within the file
            inst["Ei_other"] = h5py.ExternalLink("raw.h5", "/Ei")          # another file
            inst["gone"] = h5py.SoftLink("/raw/missing")                   # dangling
            f["entry/data/data"] = h5py.SoftLink("/raw/image")
            f["entry/data/other"] = h5py.ExternalLink("raw.h5", "/image")
        cfg = {
            "label": "links", "key": "LINKS", "metadata": dict(_META),
            "data": {"directory": str(data_dir), "layout": "per_entity", "file_pattern": "e*.h5"},
            "parameters": {"location": "group", "groups": {"instrument": "/entry/instrument"}},
            "artifacts": [{"type": "data", "dataset": "/entry/data/data"},
                          {"type": "other", "dataset": "/entry/data/other"}],
        }
        ent_df, art_df = _generate(tmp_path, cfg)
        assert ent_df.loc[0, "instrument.Ei"] == 30.0
        assert ent_df.loc[0, "instrument.Ei_units"] == "meV"
        assert ent_df.loc[0, "instrument.Ei_other"] == 60.0
        assert "instrument.gone" not in ent_df.columns
        shapes = {r["type"]: json.loads(r["shape"]) for _, r in art_df.iterrows()}
        assert shapes == {"data": [2, 3], "other": [2, 3]}
        # The manifest records the *link* path; the library resolves it at read time.
        assert set(art_df["dataset"]) == {"/entry/data/data", "/entry/data/other"}


class TestSharedAxisAgreement:

    def test_differing_shared_axis_is_an_error(self, tmp_path):
        """A `shared:` axis must be identical in every file that holds it."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        for i in range(2):
            with h5py.File(data_dir / f"e{i}.h5", "w") as f:
                f.create_dataset("spectrum", data=np.ones(3))
                f.create_dataset("sigma", data=0.1 * i)
                f.create_dataset("energy", data=np.arange(3.0) * (i + 1))   # differs
        cfg = {
            "label": "sa", "key": "SA", "metadata": dict(_META),
            "data": {"directory": str(data_dir), "layout": "per_entity", "file_pattern": "*.h5"},
            "parameters": {"location": "root_scalars"},
            "artifacts": [{"type": "spectrum", "dataset": "/spectrum"}],
            "shared": [{"type": "energy", "dataset": "/energy"}],
        }
        with pytest.raises(ValueError, match="differs between e0.h5 and e1.h5"):
            _generate(tmp_path, cfg)

    def test_identical_shared_axis_is_recorded_once(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        for i in range(2):
            with h5py.File(data_dir / f"e{i}.h5", "w") as f:
                f.create_dataset("spectrum", data=np.ones(3))
                f.create_dataset("sigma", data=0.1 * i)
                f.create_dataset("energy", data=np.arange(3.0)).attrs["units"] = "eV"
        cfg = {
            "label": "sb", "key": "SB", "metadata": dict(_META),
            "data": {"directory": str(data_dir), "layout": "per_entity", "file_pattern": "*.h5"},
            "parameters": {"location": "root_scalars"},
            "artifacts": [{"type": "spectrum", "dataset": "/spectrum"}],
            "shared": [{"type": "energy", "dataset": "/energy"}],
        }
        _, art_df = _generate(tmp_path, cfg)
        shared = art_df[art_df["uid"].isna()]
        assert list(shared["type"]) == ["energy"]
        assert shared.iloc[0]["units"] == "eV"


class TestGroupedRootEntityGroup:

    def test_entity_group_root_skips_root_datasets(self, tmp_path):
        """`entity_group: /` (several NXentry per file): only subgroups are entities;
        a root-level dataset beside them is not one and must not crash the walk."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        with h5py.File(data_dir / "multi.h5", "w") as f:
            f.create_dataset("file_time", data="2026-08-22")           # root dataset sibling
            for i in range(2):
                e = f.create_group(f"entry{i}")
                e.create_group("sample").create_dataset("temperature", data=float(i))
                e.create_dataset("data/data", data=np.zeros((2, 2)))
        cfg = {
            "label": "multi", "key": "MULTI", "metadata": dict(_META),
            "data": {"directory": str(data_dir), "layout": "grouped", "file_pattern": "*.h5"},
            "parameters": {"location": "group_scalars", "entity_group": "/",
                           "groups": {"sample": "sample"}},
            "artifacts": [{"type": "data", "dataset": "data/data"}],
        }
        ent_df, art_df = _generate(tmp_path, cfg)
        assert len(ent_df) == 2
        assert sorted(ent_df["sample.temperature"]) == [0.0, 1.0]
        assert {d.lstrip("/") for d in art_df["dataset"]} == {"entry0/data/data", "entry1/data/data"}


class TestArtifactLabelCollision:

    def test_attr_named_like_a_manifest_column_is_dropped_with_a_warning(self, tmp_path, capsys):
        """A dataset attribute called `type`/`shape` must not overwrite the broker's
        own manifest columns; it is dropped and reported once."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        for i in range(2):
            with h5py.File(data_dir / f"e{i}.h5", "w") as f:
                f.create_dataset("sigma", data=0.1 * i)
                ds = f.create_dataset("spectrum", data=np.ones(3))
                ds.attrs["type"] = "NX_FLOAT"
                ds.attrs["shape"] = "not a shape"
                ds.attrs["units"] = "counts"
        cfg = {
            "label": "coll", "key": "COLL", "metadata": dict(_META),
            "data": {"directory": str(data_dir), "layout": "per_entity", "file_pattern": "*.h5"},
            "parameters": {"location": "root_scalars"},
            "artifacts": [{"type": "spectrum", "dataset": "/spectrum"}],
        }
        _, art_df = _generate(tmp_path, cfg)
        assert list(art_df["type"].unique()) == ["spectrum"]
        assert all(json.loads(s) == [3] for s in art_df["shape"])
        assert (art_df["units"] == "counts").all()                 # ordinary labels still ride
        out = capsys.readouterr().out
        assert out.count("/spectrum@type is not carried") == 1      # once, not once per file
        assert out.count("/spectrum@shape is not carried") == 1

