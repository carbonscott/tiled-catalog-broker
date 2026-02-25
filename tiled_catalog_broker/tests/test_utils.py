# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "ruamel.yaml",
# ]
# ///
"""
Unit tests for utils module.

Tests verify the generic artifact key generation where the manifest's
``type`` column IS the artifact key (no hardcoded type branches).

Run with:
    uv run --with pytest --with 'ruamel.yaml' pytest tests/test_utils.py -v
"""

import sys
from pathlib import Path

import pytest

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from broker.utils import make_artifact_key


class TestMakeArtifactKey:
    """Tests for make_artifact_key()."""

    def test_returns_type_directly(self):
        """The type column IS the key -- no transformation."""
        row = {"type": "mh_powder_30T"}
        assert make_artifact_key(row) == "mh_powder_30T"

    def test_gs_state(self):
        row = {"type": "gs_state"}
        assert make_artifact_key(row) == "gs_state"

    def test_ins_type(self):
        row = {"type": "ins_12meV"}
        assert make_artifact_key(row) == "ins_12meV"

    def test_nips3_rixs(self):
        """Works with NiPS3-style artifact types."""
        row = {"type": "rixs"}
        assert make_artifact_key(row) == "rixs"

    def test_nips3_mag(self):
        row = {"type": "mag"}
        assert make_artifact_key(row) == "mag"

    def test_with_prefix_path(self):
        row = {"type": "mh_powder_30T"}
        assert make_artifact_key(row, prefix="path_") == "path_mh_powder_30T"

    def test_with_prefix_dataset(self):
        row = {"type": "rixs"}
        assert make_artifact_key(row, prefix="dataset_") == "dataset_rixs"

    def test_with_prefix_empty(self):
        row = {"type": "gs_state"}
        assert make_artifact_key(row, prefix="") == "gs_state"

    def test_accepts_any_type_string(self):
        """Generic: any string is a valid type."""
        row = {"type": "custom_artifact_v2"}
        assert make_artifact_key(row) == "custom_artifact_v2"

    def test_extra_columns_ignored(self):
        """Only the type column matters for key generation."""
        row = {"type": "rixs", "axis": "powder", "Hmax_T": 30}
        assert make_artifact_key(row) == "rixs"
