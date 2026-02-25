# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "tiled[server]",
#     "pandas",
#     "h5py",
#     "numpy",
#     "ruamel.yaml",
# ]
# ///
"""
Shared pytest fixtures for VDP Tiled Catalog tests.

Usage:
    # Run unit tests (no server needed)
    uv run --with pytest pytest tests/test_config.py tests/test_utils.py -v

    # Run integration tests (requires running server with data)
    uv run --with pytest pytest tests/ -v
"""

import os
import sys
from pathlib import Path

import pytest

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(scope="session")
def tiled_client():
    """Connect to running Tiled server.

    Requires server to be running:
        uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
    """
    from tiled.client import from_uri

    url = os.environ.get("TILED_URL", "http://localhost:8005")
    api_key = os.environ.get("TILED_API_KEY", "secret")

    try:
        client = from_uri(url, api_key=api_key)
        return client
    except Exception as e:
        pytest.skip(f"Tiled server not available at {url}: {e}")


@pytest.fixture
def temp_catalog_db(tmp_path):
    """Temporary database for bulk registration tests."""
    return str(tmp_path / "test_catalog.db")


@pytest.fixture(scope="session")
def small_manifest(tiled_client):
    """Load a small manifest for fast integration tests.

    Returns first 5 entities' manifest data.
    """
    from broker.query_manifest import query_manifest

    manifest = query_manifest(tiled_client, axis="powder", Hmax_T=30)
    return manifest.head(5)
