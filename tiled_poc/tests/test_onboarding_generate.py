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
root-attribute parameter extraction, shared-axis exclusion, and
YAML validation errors during loading.

Run with:
    uv run --with pytest --with h5py --with numpy --with pandas \
        --with pyarrow --with 'ruamel.yaml' \
        pytest tests/test_generate.py -v
"""

import os
import sys
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
import pytest
from ruamel.yaml import YAML

# Add project root to path for package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from broker.onboarding.generate import generate_manifests, load_yaml
from broker.onboarding.schema import ValidationError


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

        # 3 entities from batch size of 3
        assert len(ent_df) == 3
        # 3 entities x 1 artifact type = 3 artifact rows
        assert len(art_df) == 3

        # Check parameter columns exist
        assert "alpha" in ent_df.columns
        assert "beta" in ent_df.columns
        assert "uid" in ent_df.columns
        assert "key" in ent_df.columns

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