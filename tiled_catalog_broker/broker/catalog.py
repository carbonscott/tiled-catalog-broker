"""
Shared catalog helpers for config-driven ingest.

Provides two functions:
  - ensure_catalog(): create or connect to a Tiled catalog database
  - register_dataset(): generate nodes from manifests and bulk-register
"""

from pathlib import Path


def ensure_catalog(db_path, readable_storage, writable_storage):
    """Create catalog.db if it doesn't exist, or return engine for existing one.

    Args:
        db_path: Path to the SQLite database file.
        readable_storage: List of directories (only used on first creation).
        writable_storage: Path to writable storage directory.

    Returns:
        SQLAlchemy engine connected to the catalog database.
    """
    from sqlalchemy import create_engine

    db_path = Path(db_path)
    uri = f"sqlite:///{db_path}"

    if not db_path.exists():
        from tiled.catalog import from_uri as catalog_from_uri

        print(f"  Creating new catalog: {db_path}")
        catalog_from_uri(
            uri,
            writable_storage=str(writable_storage),
            readable_storage=readable_storage,
            init_if_not_exists=True,
        )
    else:
        print(f"  Using existing catalog: {db_path}")

    return create_engine(uri)


def register_dataset(engine, ent_df, art_df, base_dir, label,
                     dataset_key, dataset_metadata):
    """Generate nodes from manifests and bulk-register into the catalog.

    Args:
        engine: SQLAlchemy engine.
        ent_df: Entity manifest DataFrame.
        art_df: Artifact manifest DataFrame.
        base_dir: Base directory for resolving relative file paths.
        label: Dataset name (for logging).
        dataset_key: Key for the dataset container (e.g. "VDP").
        dataset_metadata: Metadata dict for the dataset container.
    """
    from .bulk_register import prepare_node_data, bulk_register
    from .utils import get_artifact_shape

    n = len(ent_df)
    print(f"\n--- Registering {label} ({n} entities) ---")

    # Clear shape cache to avoid cross-dataset collisions
    get_artifact_shape.__defaults__[-1].clear()

    ent_nodes, art_nodes, art_data_sources = prepare_node_data(
        ent_df, art_df, max_entities=n, base_dir=base_dir,
    )

    bulk_register(engine, ent_nodes, art_nodes, art_data_sources,
                  dataset_key=dataset_key, dataset_metadata=dataset_metadata)
