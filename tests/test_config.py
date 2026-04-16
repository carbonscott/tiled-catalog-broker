# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "ruamel.yaml",
# ]
# ///
"""
Unit tests for config module.

Tests verify configuration loading with the new manifest-based config format.
No Tiled server required.

Run with:
    uv run --with pytest --with 'ruamel.yaml' pytest tests/test_config.py -v
"""

import os
import sys
import importlib
from pathlib import Path

import pytest

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(autouse=True)
def reset_config_cache():
    """Reset config module cache before each test."""
    import tiled_catalog_broker.config as config
    config._config = None
    yield
    config._config = None


class TestLoadConfig:
    """Tests for load_config()."""

    def test_loads_broker_section(self):
        from tiled_catalog_broker.config import load_config
        cfg = load_config()
        assert isinstance(cfg, dict)
        assert "max_entities" in cfg

    def test_no_manifests_section(self):
        """Manifests section removed; code uses fallback pattern."""
        from tiled_catalog_broker.config import load_config
        cfg = load_config()
        assert "manifests" not in cfg

    def test_no_dataset_paths(self):
        """The generic config should NOT have hardcoded dataset_paths."""
        from tiled_catalog_broker.config import load_config
        cfg = load_config()
        assert "dataset_paths" not in cfg

    def test_no_default_shapes(self):
        """The generic config should NOT have hardcoded default_shapes."""
        from tiled_catalog_broker.config import load_config
        cfg = load_config()
        assert "default_shapes" not in cfg


class TestGetMaxEntities:
    """Tests for get_max_entities()."""

    def test_returns_integer(self):
        from tiled_catalog_broker.config import get_max_entities
        result = get_max_entities()
        assert isinstance(result, int)

    def test_default_is_positive(self):
        from tiled_catalog_broker.config import get_max_entities
        result = get_max_entities()
        assert result > 0

    def test_respects_env_variable(self):
        """Test that MAX_ENTITIES environment variable is respected."""
        import tiled_catalog_broker.config as config

        os.environ["MAX_ENTITIES"] = "42"
        config._config = None
        importlib.reload(config)

        result = config.get_max_entities()
        assert result == 42

        del os.environ["MAX_ENTITIES"]
        config._config = None
        importlib.reload(config)


class TestGetTiledUrl:
    """Tests for get_tiled_url()."""

    def test_returns_string(self):
        from tiled_catalog_broker.config import get_tiled_url
        url = get_tiled_url()
        assert isinstance(url, str)

    def test_default_is_localhost(self):
        from tiled_catalog_broker.config import get_tiled_url
        old_val = os.environ.pop("TILED_URL", None)
        url = get_tiled_url()
        assert url == "http://localhost:8005"
        if old_val:
            os.environ["TILED_URL"] = old_val

    def test_respects_env_variable(self):
        from tiled_catalog_broker.config import get_tiled_url
        os.environ["TILED_URL"] = "http://test:9999"
        url = get_tiled_url()
        assert url == "http://test:9999"
        del os.environ["TILED_URL"]


class TestGetApiKey:
    """Tests for get_api_key()."""

    def test_returns_string(self):
        from tiled_catalog_broker.config import get_api_key
        key = get_api_key()
        assert isinstance(key, str)

    def test_default_is_empty(self):
        from tiled_catalog_broker.config import get_api_key
        old_val = os.environ.pop("TILED_API_KEY", None)
        old_key = os.environ.pop("TILED_KEY", None)
        key = get_api_key()
        assert key == ""
        if old_val:
            os.environ["TILED_API_KEY"] = old_val
        if old_key:
            os.environ["TILED_KEY"] = old_key
