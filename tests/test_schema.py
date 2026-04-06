# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "ruamel.yaml",
# ]
# ///
"""
Unit tests for schema module.

Tests cover loading the catalog model, vocabulary lookups, alias resolution,
and YAML config validation.

Run with:
    uv run --with pytest --with 'ruamel.yaml' pytest tests/test_schema.py -v
"""

import sys
from pathlib import Path

import pytest

# Add project root to path for package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_catalog_service.schema import (
    ValidationError,
    get_alias_map,
    get_allowed_values,
    load_catalog_model,
    resolve_aliases,
    validate,
)


@pytest.fixture
def minimal_valid_config(tmp_path):
    """A minimal config dict that passes validation."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return {
        "label": "test_dataset",
        "key": "TEST_SIM_BASIC",
        "data": {
            "directory": str(data_dir),
            "layout": "per_entity",
            "file_pattern": "*.h5",
        },
        "artifacts": [
            {"type": "spectrum", "dataset": "/spectrum"},
        ],
        "metadata": {
            "method": ["RIXS"],
            "data_type": "simulation",
            "material": "NiPS3",
            "producer": "edrixs",
        },
    }


class TestLoadCatalogModel:
    """Tests for load_catalog_model()."""

    def test_load_catalog_model(self):
        """Loading the real catalog_model.yml returns a dict with 'methods' key."""
        model = load_catalog_model()
        assert model is not None
        assert isinstance(model, dict)
        assert "methods" in model

    def test_load_catalog_model_missing(self):
        """Returns None for a nonexistent path."""
        result = load_catalog_model("/nonexistent/path/catalog_model.yml")
        assert result is None


class TestGetAllowedValues:
    """Tests for get_allowed_values()."""

    def test_get_allowed_values(self):
        """Pass a minimal model dict, get back list of IDs."""
        model = {
            "methods": [
                {"id": "RIXS", "label": "RIXS"},
                {"id": "INS", "label": "INS"},
            ]
        }
        result = get_allowed_values(model, "methods")
        assert result == ["RIXS", "INS"]

    def test_get_allowed_values_missing_field(self):
        """Returns empty list for a field not in the model."""
        model = {"methods": [{"id": "RIXS"}]}
        assert get_allowed_values(model, "materials") == []

    def test_get_allowed_values_none_model(self):
        """Returns empty list when model is None."""
        assert get_allowed_values(None, "methods") == []


class TestGetAliasMap:
    """Tests for get_alias_map()."""

    def test_get_alias_map_dict_alias(self):
        """EDRIXS alias from the real model maps to RIXS with implies."""
        model = load_catalog_model()
        alias_map = get_alias_map(model, "methods")
        assert "EDRIXS" in alias_map
        assert alias_map["EDRIXS"]["canonical"] == "RIXS"
        assert alias_map["EDRIXS"]["implies"].get("data_type") == "simulation"

    def test_get_alias_map_string_alias(self):
        """NiPS3 string aliases resolve."""
        model = load_catalog_model()
        alias_map = get_alias_map(model, "materials")
        assert "NIPS" in alias_map
        assert alias_map["NIPS"]["canonical"] == "NiPS3"
        assert alias_map["NIPS"]["implies"] == {}
        assert "nips3" in alias_map
        assert alias_map["nips3"]["canonical"] == "NiPS3"


class TestResolveAliases:
    """Tests for resolve_aliases()."""

    def test_resolve_aliases_method(self):
        """cfg with method=[EDRIXS] resolves to [RIXS] and implies data_type=simulation."""
        model = load_catalog_model()
        cfg = {"metadata": {"method": ["EDRIXS"]}}
        messages = resolve_aliases(cfg, model)
        assert cfg["metadata"]["method"] == ["RIXS"]
        assert cfg["metadata"]["data_type"] == "simulation"
        assert any("EDRIXS" in m and "RIXS" in m for m in messages)

    def test_resolve_aliases_no_change(self):
        """cfg with method=[RIXS] stays unchanged."""
        model = load_catalog_model()
        cfg = {"metadata": {"method": ["RIXS"]}}
        messages = resolve_aliases(cfg, model)
        assert cfg["metadata"]["method"] == ["RIXS"]
        # No alias resolution messages expected for canonical IDs
        assert not any("Resolved" in m and "method" in m.lower() for m in messages)


class TestValidate:
    """Tests for validate()."""

    def test_validate_valid_config(self, minimal_valid_config):
        """A complete config passes validation."""
        warnings = validate(minimal_valid_config)
        assert isinstance(warnings, list)

    def test_validate_missing_key(self, minimal_valid_config):
        """Raises ValidationError when 'key' is missing."""
        del minimal_valid_config["key"]
        del minimal_valid_config["label"]
        with pytest.raises(ValidationError) as exc_info:
            validate(minimal_valid_config)
        assert any("label" in e.lower() or "key" in e.lower() for e in exc_info.value.errors)

    def test_validate_missing_artifacts(self, minimal_valid_config):
        """Raises ValidationError when artifacts list is empty."""
        minimal_valid_config["artifacts"] = []
        with pytest.raises(ValidationError) as exc_info:
            validate(minimal_valid_config)
        assert any("artifact" in e.lower() for e in exc_info.value.errors)

    def test_validate_bad_layout(self, minimal_valid_config):
        """Raises ValidationError for an invalid layout value."""
        minimal_valid_config["data"]["layout"] = "invalid_layout"
        with pytest.raises(ValidationError) as exc_info:
            validate(minimal_valid_config)
        assert any("layout" in e.lower() for e in exc_info.value.errors)

    def test_validate_alias_accepted(self, minimal_valid_config):
        """EDRIXS in method doesn't produce a vocab warning."""
        minimal_valid_config["metadata"]["method"] = ["EDRIXS"]
        warnings = validate(minimal_valid_config)
        # After alias resolution, EDRIXS becomes RIXS; no "not in catalog model" warning
        assert not any("not in catalog model" in w and "EDRIXS" in w for w in warnings)

    def test_validate_cross_field_simulation_no_producer(self, minimal_valid_config):
        """Warns when data_type is simulation but no producer is set."""
        minimal_valid_config["metadata"]["data_type"] = "simulation"
        del minimal_valid_config["metadata"]["producer"]
        warnings = validate(minimal_valid_config)
        assert any("simulation" in w and "producer" in w for w in warnings)
