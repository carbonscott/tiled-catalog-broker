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

import numpy as np
import pytest

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from tiled_catalog_broker.utils import make_artifact_key, make_entity_key, to_json_safe


class TestMakeEntityKey:
    """Tests for make_entity_key()."""

    def test_full_uuid(self):
        row = {"uid": "636ce3e4-1ea0-5f0f-a515-a4378fa5c842"}
        assert make_entity_key(row, "VDP_SIM") == "VDP_SIM_636ce3e4-1ea0"

    def test_accepts_any_dataset_key(self):
        row = {"uid": "636ce3e4-1ea0-5f0f-a515-a4378fa5c842"}
        assert make_entity_key(row, "SAM_KLEIN") == "SAM_KLEIN_636ce3e4-1ea0"
        assert make_entity_key(row, "NIPS3_MULTIMODAL") == "NIPS3_MULTIMODAL_636ce3e4-1ea0"


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


class TestToJsonSafe:
    """Tests for to_json_safe() edge cases."""

    def test_np_bool_true(self):
        """np.bool_ True → Python bool True (not raw np.bool_)."""
        result = to_json_safe(np.bool_(True))
        assert result is True
        assert type(result) is bool

    def test_np_bool_false(self):
        """np.bool_ False → Python bool False."""
        result = to_json_safe(np.bool_(False))
        assert result is False
        assert type(result) is bool

    def test_list_passthrough(self):
        """Plain Python list passes through unchanged."""
        value = [1, 2, 3]
        assert to_json_safe(value) == [1, 2, 3]

    def test_dict_passthrough(self):
        """Plain Python dict passes through unchanged."""
        value = {"a": 1}
        assert to_json_safe(value) == {"a": 1}

    def test_np_int(self):
        assert type(to_json_safe(np.int64(5))) is int

    def test_np_float(self):
        assert type(to_json_safe(np.float32(1.5))) is float

    def test_nan_becomes_none(self):
        import math
        assert to_json_safe(float("nan")) is None

    def test_none_becomes_none(self):
        assert to_json_safe(None) is None


