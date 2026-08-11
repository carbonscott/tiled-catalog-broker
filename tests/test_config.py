# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pytest",
# ]
# ///
"""
Unit tests for config module.

`config.py` resolves the Tiled server connection from the environment.
No Tiled server required.

Run with:
    uv run --with pytest pytest tests/test_config.py -v
"""

import os


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

    def test_falls_back_to_tiled_key(self):
        """TILED_API_KEY wins; TILED_KEY is the compatibility fallback."""
        from tiled_catalog_broker.config import get_api_key
        old_val = os.environ.pop("TILED_API_KEY", None)
        old_key = os.environ.pop("TILED_KEY", None)
        os.environ["TILED_KEY"] = "fallback"
        assert get_api_key() == "fallback"
        os.environ["TILED_API_KEY"] = "primary"
        assert get_api_key() == "primary"
        os.environ.pop("TILED_API_KEY")
        os.environ.pop("TILED_KEY")
        if old_val:
            os.environ["TILED_API_KEY"] = old_val
        if old_key:
            os.environ["TILED_KEY"] = old_key
