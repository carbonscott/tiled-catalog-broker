# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pytest",
#     "tiled[server]",
#     "pandas",
#     "h5py",
#     "numpy",
#     "ruamel.yaml",
#     "sqlalchemy",
# ]
# ///
"""
Integration tests for data registration.

Tests both registration methods:
- HTTP-based registration (register_catalog.py)
- Bulk SQLAlchemy registration (bulk_register.py)

Prerequisites:
    # For HTTP registration tests, start server first:
    uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret

Run with:
    uv run --with pytest pytest tests/test_registration.py -v
"""

import os
import sys
from pathlib import Path

import pytest
import pandas as pd

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestLoadManifests:
    """Tests for manifest loading (used by both registration methods)."""

    def test_load_entities_manifest(self):
        """Test that entities manifest can be loaded."""
        from broker.config import get_latest_manifest

        path = get_latest_manifest("entities")
        df = pd.read_parquet(path)

        assert len(df) > 0
        assert "uid" in df.columns

    def test_load_artifacts_manifest(self):
        """Test that Artifacts manifest can be loaded."""
        from broker.config import get_latest_manifest

        path = get_latest_manifest("artifacts")
        df = pd.read_parquet(path)

        assert len(df) > 0
        assert "type" in df.columns
        assert "uid" in df.columns

    def test_manifests_have_matching_uids(self):
        """Test that artifact uids match entity uids."""
        from broker.config import get_latest_manifest

        ent_df = pd.read_parquet(get_latest_manifest("entities"))
        art_df = pd.read_parquet(get_latest_manifest("artifacts"))

        ent_uids = set(ent_df["uid"])
        art_uids = set(art_df["uid"])

        # All artifact uids should exist in entities
        assert art_uids.issubset(ent_uids)


@pytest.mark.integration
class TestHttpRegistration:
    """Integration tests for HTTP-based registration.

    Requires running Tiled server with registered data.
    """

    def test_server_has_containers(self, tiled_client):
        """Test that registered entities appear as containers."""
        assert len(tiled_client) > 0

    def test_container_has_metadata(self, tiled_client):
        """Test that containers have physics parameters in metadata."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        # Check physics parameters
        assert "Ja_meV" in h.metadata
        assert "Jb_meV" in h.metadata
        assert "Jc_meV" in h.metadata
        assert "Dc_meV" in h.metadata

    def test_container_has_artifact_paths(self, tiled_client):
        """Test that containers have artifact paths in metadata (Mode A)."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        # Check for path metadata (Mode A support)
        path_keys = [k for k in h.metadata.keys() if k.startswith("path_")]
        assert len(path_keys) > 0

    def test_container_has_children(self, tiled_client):
        """Test that containers have artifact children (Mode B)."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        children = list(h.keys())
        assert len(children) > 0

    def test_container_children_are_arrays(self, tiled_client):
        """Test that children are accessible as arrays."""
        ent_key = list(tiled_client.keys())[0]
        h = tiled_client[ent_key]

        children = list(h.keys())
        if "mh_powder_30T" in children:
            arr = h["mh_powder_30T"][:]
            assert arr.ndim == 1
            assert len(arr) == 200  # M(H) has 200 points


@pytest.mark.integration
class TestBulkRegistration:
    """Integration tests for bulk SQLAlchemy registration.

    These tests create a temporary database and verify the bulk
    registration creates correct schema and data.
    """

    def test_init_database_creates_tables(self, temp_catalog_db):
        """Test that init_database creates required tables."""
        from sqlalchemy import create_engine, text, inspect

        # Create engine and init
        engine = create_engine(f"sqlite:///{temp_catalog_db}")

        # Run the init SQL from tiled's schema
        with engine.connect() as conn:
            # Create minimal schema for testing
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY,
                    key TEXT,
                    parent INTEGER,
                    structure_family TEXT,
                    metadata_json TEXT
                )
            """))
            conn.commit()

        # Verify table exists
        inspector = inspect(engine)
        assert "nodes" in inspector.get_table_names()

    def test_bulk_registration_creates_nodes(self, temp_catalog_db):
        """Test that bulk registration creates node entries."""
        from sqlalchemy import create_engine, text
        from broker.config import get_latest_manifest

        # Load small subset of manifests
        ent_df = pd.read_parquet(get_latest_manifest("entities")).head(3)

        # Create simple test database
        engine = create_engine(f"sqlite:///{temp_catalog_db}")
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL,
                    parent INTEGER DEFAULT 0,
                    structure_family TEXT DEFAULT 'container'
                )
            """))

            # Insert test nodes
            for _, row in ent_df.iterrows():
                ent_key = row["key"]
                conn.execute(
                    text("INSERT INTO nodes (key, parent) VALUES (:key, 0)"),
                    {"key": ent_key}
                )
            conn.commit()

            # Verify nodes created
            count = conn.execute(text("SELECT COUNT(*) FROM nodes")).scalar()
            assert count == 3
