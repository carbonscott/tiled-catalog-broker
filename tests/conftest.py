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
    """Connect to running Tiled server (root level).

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


@pytest.fixture(scope="session")
def mh_dataset_client(tiled_client):
    """Return the dataset-level client for a dataset containing mh_powder_30T artifacts.

    The catalog has a two-level hierarchy: root -> dataset containers -> entities.
    query_catalog and load_artifacts must be called at the dataset level (or on a
    search() result), not at the root.
    """
    for key in tiled_client.keys():
        dataset = tiled_client[key]
        ents = list(dataset.keys())
        if ents and "path_mh_powder_30T" in dict(dataset[ents[0]].metadata):
            return dataset
    pytest.skip("No dataset with mh_powder_30T artifacts found in catalog")


@pytest.fixture
def temp_catalog_db(tmp_path):
    """Temporary database for bulk registration tests."""
    return str(tmp_path / "test_catalog.db")


@pytest.fixture(scope="session")
def small_manifest(mh_dataset_client):
    """Load a small manifest for fast integration tests.

    Returns first 5 entities' manifest data.
    """
    from tiled_catalog_broker.query_manifest import query_catalog

    return query_catalog(mh_dataset_client, artifact_type="mh_powder_30T", limit=5)
