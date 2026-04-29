# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "h5py",
#     "numpy",
#     "ruamel.yaml",
# ]
# ///
"""
Unit tests for inspect module.

Tests cover HDF5 file discovery, layout detection, directory inspection,
draft YAML emission, and cross-file consistency checks.

Run with:
    uv run --with pytest --with h5py --with numpy --with 'ruamel.yaml' \
        pytest tests/test_inspect.py -v
"""

import sys
from pathlib import Path

import h5py
import numpy as np
import pytest

# Add project root to path for package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from tiled_catalog_broker.tools.inspect import (
    check_consistency,
    classify_datasets,
    detect_layout,
    emit_draft_yaml,
    find_h5_files,
    inspect_directory,
    DatasetInfo,
)


# ---------------------------------------------------------------------------
# Fixtures for creating small HDF5 test files
# ---------------------------------------------------------------------------

@pytest.fixture
def per_entity_dir(tmp_path):
    """Create a directory with 3 per-entity HDF5 files.

    Each file has two scalar parameters and one 1D array.
    """
    for i in range(3):
        fpath = tmp_path / f"entity_{i:03d}.h5"
        with h5py.File(fpath, "w") as f:
            f.create_dataset("param_a", data=float(i) * 1.5)
            f.create_dataset("param_b", data=float(i) * 0.3)
            f.create_dataset("spectrum", data=np.random.randn(10))
    return tmp_path


@pytest.fixture
def batched_dir(tmp_path):
    """Create a directory with a single batched HDF5 file.

    Contains a params group with 1D arrays (batch dim = 3) and
    a 2D artifact array (3, 4).
    """
    fpath = tmp_path / "batch.h5"
    with h5py.File(fpath, "w") as f:
        g = f.create_group("params")
        g.create_dataset("alpha", data=np.array([1.0, 2.0, 3.0]))
        g.create_dataset("beta", data=np.array([0.1, 0.2, 0.3]))
        f.create_dataset("spectra", data=np.random.randn(3, 4))
        # A scalar that is not part of the batch dimension
        f.create_dataset("version", data=1)
    return tmp_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFindH5Files:
    """Tests for find_h5_files()."""

    def test_find_h5_files(self, per_entity_dir):
        """Creates tmp dir with 3 .h5 files, verifies find_h5_files returns them."""
        files, pattern = find_h5_files(per_entity_dir)
        assert len(files) == 3
        assert all(str(f).endswith(".h5") for f in files)
        assert isinstance(pattern, str)

    def test_find_h5_files_empty(self, tmp_path):
        """Returns empty list for a directory with no HDF5 files."""
        files, pattern = find_h5_files(tmp_path)
        assert files == []


class TestDetectLayout:
    """Tests for detect_layout()."""

    def test_detect_layout_batched(self, batched_dir):
        """Single file with arrays sharing axis-0 size > 1, plus scalar -> batched."""
        fpath = batched_dir / "batch.h5"
        datasets = {}
        with h5py.File(fpath, "r") as f:
            def visit(name, obj):
                if isinstance(obj, h5py.Dataset):
                    datasets[name] = DatasetInfo(
                        name=name,
                        shape=obj.shape,
                        dtype=str(obj.dtype),
                        ndim=obj.ndim,
                        size=obj.size,
                    )
            f.visititems(visit)

        h5_files = sorted(batched_dir.glob("*.h5"))
        layout, batch_size = detect_layout(datasets, h5_files)
        assert layout == "batched"
        assert batch_size == 3

    def test_detect_layout_per_entity(self, per_entity_dir):
        """Multiple files with scalar datasets -> per_entity."""
        fpath = sorted(per_entity_dir.glob("*.h5"))[0]
        datasets = {}
        with h5py.File(fpath, "r") as f:
            def visit(name, obj):
                if isinstance(obj, h5py.Dataset):
                    datasets[name] = DatasetInfo(
                        name=name,
                        shape=obj.shape,
                        dtype=str(obj.dtype),
                        ndim=obj.ndim,
                        size=obj.size,
                    )
            f.visititems(visit)

        h5_files = sorted(per_entity_dir.glob("*.h5"))
        layout, batch_size = detect_layout(datasets, h5_files)
        assert layout == "per_entity"
        assert batch_size == 0


class TestInspectDirectory:
    """Tests for inspect_directory()."""

    def test_inspect_directory_batched(self, batched_dir):
        """Creates a batched HDF5 file, runs inspect_directory, checks result."""
        result = inspect_directory(batched_dir)
        assert result.layout == "batched"
        assert result.batch_size == 3
        assert result.total_entities == 3
        assert len(result.h5_files) == 1
        # Check that datasets were found
        assert len(result.datasets) > 0
        # Check that spectra was classified as ARTIFACT
        assert any(
            d.category == "ARTIFACT" for d in result.datasets.values()
            if "spectra" in d.name
        )

    def test_inspect_directory_empty(self, tmp_path):
        """Returns an InspectionResult with no files for an empty directory."""
        result = inspect_directory(tmp_path)
        assert result.h5_files == []


class TestEmitDraftYaml:
    """Tests for emit_draft_yaml()."""

    def test_emit_draft_yaml_describes_key_autofill(self, batched_dir):
        """emit_draft_yaml explains that `key` is auto-filled from label."""
        result = inspect_directory(batched_dir)
        yaml_str = emit_draft_yaml(result)
        assert "auto-filled at registration" in yaml_str
        assert "slug(label)" in yaml_str

    def test_emit_draft_yaml_no_round_in_provenance(self, batched_dir):
        """Provenance section doesn't mention 'round:' or 'prior_distribution:'."""
        result = inspect_directory(batched_dir)
        yaml_str = emit_draft_yaml(result)
        # The provenance section should not contain these fields
        # (they belong in dataset_fields, not in the draft provenance block)
        provenance_start = yaml_str.find("provenance:")
        if provenance_start >= 0:
            provenance_section = yaml_str[provenance_start:]
            # Check that round and prior_distribution are not emitted as
            # provenance entries (they may appear in comments about
            # dataset_fields but not as provenance keys)
            for line in provenance_section.split("\n"):
                # Only check non-comment content lines under provenance
                stripped = line.strip()
                if stripped and not stripped.startswith("#"):
                    assert "round:" not in stripped
                    assert "prior_distribution:" not in stripped


class TestConsistencyCheck:
    """Tests for check_consistency()."""

    def test_consistency_check_pass(self, per_entity_dir):
        """Two identical-structure files produce no consistency issues."""
        h5_files = sorted(per_entity_dir.glob("*.h5"))
        assert len(h5_files) >= 2

        # Build reference datasets from the first file
        ref_datasets = {}
        with h5py.File(h5_files[0], "r") as f:
            def visit(name, obj):
                if isinstance(obj, h5py.Dataset):
                    ref_datasets[name] = DatasetInfo(
                        name=name,
                        shape=obj.shape,
                        dtype=str(obj.dtype),
                        ndim=obj.ndim,
                        size=obj.size,
                    )
            f.visititems(visit)

        issues = check_consistency(h5_files, ref_datasets, "per_entity")
        assert issues == []
