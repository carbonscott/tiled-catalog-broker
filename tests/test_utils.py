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

import pandas as pd

from tiled_catalog_broker.utils import (
    make_artifact_key,
    make_entity_key,
    split_constant_cols,
    to_json_safe,
)


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
        """Only the type column matters for key generation when no fan-out."""
        row = {"type": "rixs", "axis": "powder", "Hmax_T": 30}
        assert make_artifact_key(row) == "rixs"

    def test_fanout_row_uses_array_name_and_auid(self):
        """Fan-out rows (array_name + auid present) use short-auid suffix."""
        row = {
            "type": "mh_curve",
            "array_name": "H_T",
            "auid": "cfbc55c6-741b-5aa5-8f2b-b680f1f8f627",
        }
        assert make_artifact_key(row) == "H_T_cfbc55c6"

    def test_fanout_row_with_prefix(self):
        row = {
            "type": "ins_powder",
            "array_name": "broadened",
            "auid": "07fff5c0-0030-52bc-a21c-34df6efba3dc",
        }
        assert make_artifact_key(row, prefix="path_") == "path_broadened_07fff5c0"

    def test_pandas_series_fanout(self):
        """Works with pandas Series rows too."""
        row = pd.Series({
            "type": "gs_state",
            "array_name": "moment_muB",
            "auid": "9e95715f-d0d2-5290-a7f4-b66721bdd308",
        })
        assert make_artifact_key(row) == "moment_muB_9e95715f"


class TestSplitConstantCols:
    """Tests for split_constant_cols()."""

    def test_all_constant(self):
        df = pd.DataFrame({"a": [1, 1, 1], "b": ["x", "x", "x"]})
        constants, varying = split_constant_cols(df)
        assert constants == {"a": 1, "b": "x"}
        assert varying == []

    def test_all_varying(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        constants, varying = split_constant_cols(df)
        assert constants == {}
        assert set(varying) == {"a", "b"}

    def test_mixed(self):
        df = pd.DataFrame({
            "schema_version": ["1.0", "1.0", "1.0"],
            "uid": ["a", "b", "c"],
            "run_id": ["r1", "r1", "r1"],
        })
        constants, varying = split_constant_cols(df)
        assert constants == {"schema_version": "1.0", "run_id": "r1"}
        assert varying == ["uid"]

    def test_nans_ignored_for_constancy(self):
        """NaN entries don't count as a distinct value."""
        import numpy as np
        df = pd.DataFrame({"method": ["foo", np.nan, "foo"]})
        constants, varying = split_constant_cols(df)
        assert constants == {"method": "foo"}
        assert varying == []

    def test_all_null_column_dropped(self):
        import numpy as np
        df = pd.DataFrame({"empty": [np.nan, np.nan], "uid": ["a", "b"]})
        constants, varying = split_constant_cols(df)
        assert "empty" not in constants
        assert "empty" not in varying
        assert varying == ["uid"]


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


