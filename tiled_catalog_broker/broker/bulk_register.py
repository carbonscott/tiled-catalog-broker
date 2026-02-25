#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "tiled[server]",
#     "pandas",
#     "pyarrow",
#     "h5py",
#     "numpy",
#     "ruamel.yaml",
#     "canonicaljson",
#     "sqlalchemy",
# ]
# ///
"""
Generic Bulk Registration with SQLAlchemy.

Bypasses Tiled HTTP layer for maximum bulk insert performance.
Uses direct SQLAlchemy with trigger disable/rebuild pattern.

Dataset-agnostic: reads all metadata columns dynamically from manifests.
The manifest is the contract -- no hardcoded parameter names or artifact types.

Key optimizations:
- Single database transaction for all inserts
- Disables closure table trigger during bulk load
- Rebuilds closure table with set-based SQL
- Re-enables trigger for future incremental updates

When to use:
- Initial bulk load of 1K+ entities
- Fresh database registration
- Maximum speed needed

When NOT to use:
- Incremental updates (use register_catalog.py instead)
- Server is running and serving queries

Usage:
    # Register with defaults (10 entities to catalog.db)
    python bulk_register.py

    # Register specific number of entities
    python bulk_register.py -n 1000

    # Register all entities to a specific database
    python bulk_register.py -n 10000 -o catalog-bulk.db

    # Force overwrite without prompting
    python bulk_register.py -n 100 --force

    # Environment variables still work as fallbacks
    MAX_ENTITIES=10000 CATALOG_DB=catalog-bulk.db python bulk_register.py
"""

import os
import sys
import time
import json
import hashlib
import argparse
from pathlib import Path

import pandas as pd
import canonicaljson
from sqlalchemy import create_engine, text

# Import from shared helpers
from .config import (
    get_base_dir,
    get_latest_manifest,
    get_max_entities,
    get_catalog_db_path,
)
from .utils import (
    make_artifact_key,
    to_json_safe,
    get_artifact_shape,
    ARTIFACT_STANDARD_COLS,
)


def compute_structure_id(structure):
    """Compute HEX digest of MD5 hash of RFC 8785 canonical JSON."""
    canonical = canonicaljson.encode_canonical_json(structure)
    return hashlib.md5(canonical).hexdigest()


# SQLite trigger SQL (from Tiled orm.py)
CLOSURE_TRIGGER_SQL = """
CREATE TRIGGER update_closure_table_when_inserting
AFTER INSERT ON nodes
BEGIN
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    SELECT NEW.id, NEW.id, 0;
    INSERT INTO nodes_closure(ancestor, descendant, depth)
    SELECT p.ancestor, c.descendant, p.depth+c.depth+1
    FROM nodes_closure p, nodes_closure c
    WHERE p.descendant=NEW.parent and c.ancestor=NEW.id;
END
"""


def init_database(db_path):
    """Initialize database with Tiled schema.

    Uses Tiled's catalog adapter to create schema, then returns
    a raw SQLAlchemy engine for bulk operations.
    """
    from tiled.catalog import from_uri as catalog_from_uri

    # Remove existing database for fresh start
    if os.path.exists(db_path):
        print(f"  Removing existing database: {db_path}")
        os.remove(db_path)

    # Use Tiled to create schema (runs migrations, creates triggers)
    print(f"  Initializing schema via Tiled...")
    uri = f"sqlite:///{db_path}"

    # from_uri is sync - just call it directly
    catalog_from_uri(
        uri,
        writable_storage=str(Path(db_path).parent / "storage"),
        readable_storage=[get_base_dir()],
        init_if_not_exists=True,
    )

    # Return sync engine for bulk operations
    engine = create_engine(f"sqlite:///{db_path}")
    return engine


def load_manifests():
    """Load entity and artifact manifests."""
    base_dir = get_base_dir()
    print(f"Loading manifests from {base_dir}...")

    ent_path = get_latest_manifest("entities")
    art_path = get_latest_manifest("artifacts")

    print(f"  Entities:  {Path(ent_path).name}")
    print(f"  Artifacts: {Path(art_path).name}")

    ent_df = pd.read_parquet(ent_path)
    art_df = pd.read_parquet(art_path)

    print(f"  Rows: {len(ent_df)} entities, {len(art_df)} artifacts")

    return ent_df, art_df


def prepare_node_data(ent_df, art_df, max_entities, base_dir=None):
    """Prepare all node data for bulk insert.

    Reads all metadata columns dynamically from manifests -- no hardcoded
    parameter names or artifact types.

    Args:
        ent_df: Entity manifest DataFrame.
        art_df: Artifact manifest DataFrame.
        max_entities: Maximum number of entities to process.
        base_dir: Base directory for resolving relative file paths.
            Defaults to get_base_dir().

    Returns:
        ent_nodes: List of entity node dicts
        art_nodes: List of artifact node dicts (with placeholder parent)
        art_data_sources: List of data source info for artifacts
    """
    if base_dir is None:
        base_dir = get_base_dir()

    if "key" not in ent_df.columns:
        raise ValueError(
            "Entity manifest missing required 'key' column. "
            "The manifest generator must provide a 'key' for each entity."
        )

    ent_subset = ent_df.head(max_entities)
    art_grouped = art_df.groupby("uid")

    ent_nodes = []
    art_nodes = []
    art_data_sources = []

    print(f"Preparing data for {len(ent_subset)} entities...")

    for _, ent_row in ent_subset.iterrows():
        uid = str(ent_row["uid"])
        ent_key = str(ent_row["key"])

        # Build entity metadata dynamically from ALL manifest columns
        metadata = {}
        for col in ent_df.columns:
            metadata[col] = to_json_safe(ent_row[col])

        # Attach artifact locators to entity metadata (for Mode A access)
        artifacts = None
        if uid in art_grouped.groups:
            artifacts = art_grouped.get_group(uid)
            for _, art_row in artifacts.iterrows():
                art_key = make_artifact_key(art_row)
                metadata[f"path_{art_key}"] = art_row["file"]
                metadata[f"dataset_{art_key}"] = art_row["dataset"]
                if "index" in art_row.index and pd.notna(art_row.get("index")):
                    metadata[f"index_{art_key}"] = int(art_row["index"])

        ent_nodes.append({
            "key": ent_key,
            "uid": uid,  # For linking artifacts
            "structure_family": "container",
            "metadata": metadata,
            "specs": [],
            "access_blob": {},
        })

        # Process artifacts for this entity
        if artifacts is not None:
            for _, art_row in artifacts.iterrows():
                art_key = make_artifact_key(art_row)
                h5_rel_path = art_row["file"]
                h5_full_path = os.path.join(base_dir, h5_rel_path)
                dataset_path = art_row["dataset"]
                index = None
                if "index" in art_row.index and pd.notna(art_row.get("index")):
                    index = int(art_row["index"])

                # Get shape from HDF5 (cached by dataset path)
                data_shape = get_artifact_shape(
                    base_dir, h5_rel_path, dataset_path, index
                )

                # Build artifact metadata dynamically from non-standard columns
                art_metadata = {
                    "type": art_row["type"],
                    "shape": data_shape,
                    "dtype": "float64",
                }
                for col in art_df.columns:
                    if col not in ARTIFACT_STANDARD_COLS:
                        art_metadata[col] = to_json_safe(art_row[col])

                # Build structure for this artifact
                chunks = [[dim] for dim in data_shape]
                structure = {
                    "data_type": {
                        "endianness": "little",
                        "kind": "f",
                        "itemsize": 8,
                    },
                    "chunks": chunks,
                    "shape": data_shape,
                    "dims": None,
                    "resizable": False,
                }
                structure_id = compute_structure_id(structure)

                # Build data source parameters
                ds_params = {"dataset": dataset_path}
                if index is not None:
                    ds_params["slice"] = str(int(index))

                art_nodes.append({
                    "key": art_key,
                    "parent_uid": uid,  # For linking to parent
                    "structure_family": "array",
                    "metadata": art_metadata,
                    "specs": [],
                    "access_blob": {},
                })

                art_data_sources.append({
                    "art_key": art_key,
                    "parent_uid": uid,
                    "structure_id": structure_id,
                    "structure": structure,
                    "h5_path": h5_full_path,
                    "dataset_path": dataset_path,
                    "parameters": ds_params,
                })

    print(f"  Prepared {len(ent_nodes)} entities, {len(art_nodes)} artifacts")
    return ent_nodes, art_nodes, art_data_sources


def bulk_register(engine, ent_nodes, art_nodes, art_data_sources,
                  dataset_key, dataset_metadata):
    """Bulk insert all data with trigger disable/rebuild.

    Args:
        engine: SQLAlchemy engine.
        ent_nodes: List of entity node dicts.
        art_nodes: List of artifact node dicts.
        art_data_sources: List of data source info for artifacts.
        dataset_key: Key for the dataset container (e.g. "VDP").
        dataset_metadata: Metadata dict for the dataset container.
    """

    start_time = time.time()

    with engine.connect() as conn:
        # Step 1: Disable closure table trigger
        print("Step 1: Disabling closure table trigger...")
        conn.execute(text("DROP TRIGGER IF EXISTS update_closure_table_when_inserting"))

        # Step 1b: Create or reuse dataset container
        print(f"Step 1b: Creating dataset container '{dataset_key}'...")
        row = conn.execute(text(
            "SELECT id FROM nodes WHERE parent = 0 AND key = :key"
        ), {"key": dataset_key}).fetchone()

        if row:
            dataset_parent_id = row[0]
            print(f"  Using existing container (id={dataset_parent_id})")
        else:
            result = conn.execute(text("""
                INSERT INTO nodes (parent, key, structure_family, metadata, specs, access_blob)
                VALUES (0, :key, 'container', :metadata, '[]', '{}')
            """), {
                "key": dataset_key,
                "metadata": json.dumps(dataset_metadata),
            })
            dataset_parent_id = result.lastrowid

        # Step 2: Insert entity nodes (skip existing)
        print(f"Step 2: Inserting {len(ent_nodes)} entity nodes...")
        ent_id_map = {}  # uid -> node_id
        skip_uids = set()  # uids of skipped entities

        for ent in ent_nodes:
            existing = conn.execute(text(
                "SELECT id FROM nodes WHERE parent = :parent AND key = :key"
            ), {"parent": dataset_parent_id, "key": ent["key"]}).fetchone()

            if existing:
                ent_id_map[ent["uid"]] = existing[0]
                skip_uids.add(ent["uid"])
                continue

            result = conn.execute(
                text("""
                    INSERT INTO nodes (parent, key, structure_family, metadata, specs, access_blob)
                    VALUES (:parent, :key, :structure_family, :metadata, :specs, :access_blob)
                """),
                {
                    "parent": dataset_parent_id,
                    "key": ent["key"],
                    "structure_family": ent["structure_family"],
                    "metadata": json.dumps(ent["metadata"]),
                    "specs": json.dumps(ent["specs"]),
                    "access_blob": json.dumps(ent["access_blob"]),
                }
            )
            ent_id_map[ent["uid"]] = result.lastrowid

        if skip_uids:
            print(f"  Skipped {len(skip_uids)} existing entities")

        # Step 3: Insert artifact nodes (skip if parent entity was skipped)
        art_to_insert = [a for a in art_nodes if a["parent_uid"] not in skip_uids]
        print(f"Step 3: Inserting {len(art_to_insert)} artifact nodes...")
        art_id_map = {}  # (uid, art_key) -> node_id

        for art in art_to_insert:
            parent_id = ent_id_map[art["parent_uid"]]
            result = conn.execute(
                text("""
                    INSERT INTO nodes (parent, key, structure_family, metadata, specs, access_blob)
                    VALUES (:parent, :key, :structure_family, :metadata, :specs, :access_blob)
                """),
                {
                    "parent": parent_id,
                    "key": art["key"],
                    "structure_family": art["structure_family"],
                    "metadata": json.dumps(art["metadata"]),
                    "specs": json.dumps(art["specs"]),
                    "access_blob": json.dumps(art["access_blob"]),
                }
            )
            art_id_map[(art["parent_uid"], art["key"])] = result.lastrowid

        # Filter data sources to match inserted artifacts only
        ds_to_insert = [ds for ds in art_data_sources
                        if ds["parent_uid"] not in skip_uids]

        # Step 4: Insert structures (deduplicated)
        print("Step 4: Inserting structures...")
        structures_seen = set()
        for ds in ds_to_insert:
            sid = ds["structure_id"]
            if sid not in structures_seen:
                conn.execute(
                    text("""
                        INSERT OR IGNORE INTO structures (id, structure)
                        VALUES (:id, :structure)
                    """),
                    {"id": sid, "structure": json.dumps(ds["structure"])}
                )
                structures_seen.add(sid)
        print(f"  Inserted {len(structures_seen)} unique structures")

        # Step 5: Insert assets (deduplicated by data_uri)
        print("Step 5: Inserting assets...")
        asset_id_map = {}  # data_uri -> asset_id

        for ds in ds_to_insert:
            data_uri = f"file://localhost{ds['h5_path']}"
            if data_uri not in asset_id_map:
                result = conn.execute(
                    text("""
                        INSERT OR IGNORE INTO assets (data_uri, is_directory)
                        VALUES (:data_uri, 0)
                    """),
                    {"data_uri": data_uri}
                )
                # Get the ID (either from insert or existing)
                existing = conn.execute(
                    text("SELECT id FROM assets WHERE data_uri = :data_uri"),
                    {"data_uri": data_uri}
                ).fetchone()
                asset_id_map[data_uri] = existing[0]
        print(f"  Inserted {len(asset_id_map)} unique assets")

        # Step 6: Insert data_sources
        print("Step 6: Inserting data sources...")
        ds_id_map = {}  # (uid, art_key) -> data_source_id

        for ds in ds_to_insert:
            node_id = art_id_map[(ds["parent_uid"], ds["art_key"])]
            result = conn.execute(
                text("""
                    INSERT INTO data_sources (node_id, structure_id, mimetype, parameters, properties, management, structure_family)
                    VALUES (:node_id, :structure_id, :mimetype, :parameters, :properties, :management, :structure_family)
                """),
                {
                    "node_id": node_id,
                    "structure_id": ds["structure_id"],
                    "mimetype": "application/x-hdf5",
                    "parameters": json.dumps(ds["parameters"]),
                    "properties": json.dumps({}),
                    "management": "external",
                    "structure_family": "array",
                }
            )
            ds_id_map[(ds["parent_uid"], ds["art_key"])] = result.lastrowid

        # Step 7: Insert data_source_asset_association
        print("Step 7: Inserting data source asset associations...")
        for ds in ds_to_insert:
            ds_id = ds_id_map[(ds["parent_uid"], ds["art_key"])]
            data_uri = f"file://localhost{ds['h5_path']}"
            asset_id = asset_id_map[data_uri]
            conn.execute(
                text("""
                    INSERT INTO data_source_asset_association (data_source_id, asset_id, parameter, num)
                    VALUES (:ds_id, :asset_id, :parameter, NULL)
                """),
                {"ds_id": ds_id, "asset_id": asset_id, "parameter": "data_uris"}
            )

        # Step 8: Rebuild closure table
        print("Step 8: Rebuilding closure table...")

        # Clear existing data (root node was auto-inserted)
        conn.execute(text("DELETE FROM nodes_closure"))

        # Self-references (depth=0)
        conn.execute(text("""
            INSERT INTO nodes_closure (ancestor, descendant, depth)
            SELECT id, id, 0 FROM nodes
        """))

        # Parent-child (depth=1)
        conn.execute(text("""
            INSERT INTO nodes_closure (ancestor, descendant, depth)
            SELECT parent, id, 1 FROM nodes WHERE parent IS NOT NULL
        """))

        # Grandparent (depth=2) - for root → dataset → entity
        conn.execute(text("""
            INSERT INTO nodes_closure (ancestor, descendant, depth)
            SELECT gp.parent, n.id, 2
            FROM nodes n
            JOIN nodes gp ON n.parent = gp.id
            WHERE gp.parent IS NOT NULL
        """))

        # Great-grandparent (depth=3) - for root → dataset → entity → artifact
        conn.execute(text("""
            INSERT INTO nodes_closure (ancestor, descendant, depth)
            SELECT ggp.parent, n.id, 3
            FROM nodes n
            JOIN nodes gp ON n.parent = gp.id
            JOIN nodes ggp ON gp.parent = ggp.id
            WHERE ggp.parent IS NOT NULL
        """))

        # Verify closure table
        closure_count = conn.execute(text("SELECT COUNT(*) FROM nodes_closure")).fetchone()[0]
        print(f"  Closure table rows: {closure_count}")

        # Step 9: Re-enable trigger
        print("Step 9: Re-enabling closure table trigger...")
        conn.execute(text(CLOSURE_TRIGGER_SQL))

        # Commit everything
        conn.commit()

    elapsed = time.time() - start_time
    print(f"\nBulk registration complete in {elapsed:.1f} seconds")
    return elapsed


def verify_registration(db_path):
    """Verify the bulk registration worked."""
    print("\n" + "=" * 50)
    print("Verification")
    print("=" * 50)

    engine = create_engine(f"sqlite:///{db_path}")
    with engine.connect() as conn:
        # Count tables
        nodes = conn.execute(text("SELECT COUNT(*) FROM nodes")).fetchone()[0]
        closure = conn.execute(text("SELECT COUNT(*) FROM nodes_closure")).fetchone()[0]
        data_sources = conn.execute(text("SELECT COUNT(*) FROM data_sources")).fetchone()[0]
        structures = conn.execute(text("SELECT COUNT(*) FROM structures")).fetchone()[0]
        assets = conn.execute(text("SELECT COUNT(*) FROM assets")).fetchone()[0]
        associations = conn.execute(text("SELECT COUNT(*) FROM data_source_asset_association")).fetchone()[0]

        print(f"Table counts:")
        print(f"  nodes:          {nodes}")
        print(f"  nodes_closure:  {closure}")
        print(f"  data_sources:   {data_sources}")
        print(f"  structures:     {structures}")
        print(f"  assets:         {assets}")
        print(f"  associations:   {associations}")

        # List dataset containers
        datasets = conn.execute(text("""
            SELECT id, key FROM nodes
            WHERE parent = 0 AND key != ''
        """)).fetchall()
        print(f"  datasets:       {len(datasets)}")
        for ds_id, ds_key in datasets:
            n_ents = conn.execute(text(
                "SELECT COUNT(*) FROM nodes WHERE parent = :ds_id"
            ), {"ds_id": ds_id}).fetchone()[0]
            print(f"    {ds_key}: {n_ents} entities")

        # Sample one entity from first dataset
        if datasets:
            ds_id = datasets[0][0]
            ent = conn.execute(text("""
                SELECT id, key, metadata FROM nodes
                WHERE parent = :ds_id LIMIT 1
            """), {"ds_id": ds_id}).fetchone()

            if ent:
                print(f"\nSample entity: {ent[1]} (under {datasets[0][1]})")
                meta = json.loads(ent[2])

                # Show first few metadata keys (generic -- no hardcoded param names)
                meta_keys = [k for k in meta if not k.startswith(("path_", "dataset_", "index_"))]
                print(f"  Metadata keys: {meta_keys}")

                # Count children
                children = conn.execute(text("""
                    SELECT COUNT(*) FROM nodes WHERE parent = :parent_id
                """), {"parent_id": ent[0]}).fetchone()[0]
                print(f"  Children: {children}")

                # Check locator keys in metadata
                path_keys = [k for k in meta if k.startswith("path_")]
                dataset_keys = [k for k in meta if k.startswith("dataset_")]
                index_keys = [k for k in meta if k.startswith("index_")]
                print(f"  Locator keys: {len(path_keys)} paths, {len(dataset_keys)} datasets, {len(index_keys)} indices")

    print("\n" + "=" * 50)
    print("To test with Tiled server:")
    print("=" * 50)
    print("""
1. Start server:
   uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret

2. Test retrieval:
   uv run --with 'tiled[server]' --with pandas python -c "
   from tiled.client import from_uri
   client = from_uri('http://localhost:8005', api_key='secret')
   print(f'Entities: {len(client)}')
   h = client[list(client)[0]]
   print(f'Artifacts: {list(h)}')
   print(f'Data shape: {h[list(h.keys())[0]].read().shape}')
   "
""")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Bulk register entities to Tiled catalog using SQLAlchemy.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                        # Register 10 entities (default)
  %(prog)s -n 1000                # Register 1000 entities
  %(prog)s -n 10000 -o bulk.db    # Register all to bulk.db
  %(prog)s -n 100 --force         # Overwrite without prompting

Environment variables (used as fallbacks):
  MAX_ENTITIES           Number of entities (default: from config)
  CATALOG_DB             Database filename (default: catalog.db)
"""
    )

    parser.add_argument(
        "-n", "--max-entities",
        type=int,
        default=None,
        metavar="NUM",
        help="Number of entities to register (default: 10 or MAX_ENTITIES)"
    )

    parser.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        metavar="DB_NAME",
        help="Output database filename (default: catalog.db or CATALOG_DB)"
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing database without prompting"
    )

    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 60)
    print("Generic Bulk Registration (SQLAlchemy + Trigger Rebuild)")
    print("=" * 60)

    # Determine database path (CLI > env var > config default)
    from .config import get_service_dir

    if args.output:
        db_path = os.path.join(get_service_dir(), args.output)
    elif os.environ.get("CATALOG_DB"):
        db_path = os.path.join(get_service_dir(), os.environ.get("CATALOG_DB"))
    else:
        db_path = get_catalog_db_path()

    # Determine max entities (CLI > env var > config default)
    if args.max_entities is not None:
        max_entities = args.max_entities
    else:
        max_entities = get_max_entities()

    print(f"Database:       {db_path}")
    print(f"Data dir:       {get_base_dir()}")
    print(f"Max entities:   {max_entities}")
    print()

    # Check if database exists and handle --force
    if os.path.exists(db_path) and not args.force:
        print(f"WARNING: Database already exists: {db_path}")
        response = input("Overwrite? [y/N]: ").strip().lower()
        if response != 'y':
            print("Aborted.")
            sys.exit(0)

    # Initialize database
    print("Initializing database...")
    engine = init_database(db_path)

    # Load manifests
    ent_df, art_df = load_manifests()

    # Prepare data
    ent_nodes, art_nodes, art_data_sources = prepare_node_data(
        ent_df, art_df, max_entities
    )

    # Bulk register
    print("\nStarting bulk registration...")
    elapsed = bulk_register(engine, ent_nodes, art_nodes, art_data_sources)

    # Calculate rate
    total_nodes = len(ent_nodes) + len(art_nodes) + 1  # +1 for root
    rate = total_nodes / elapsed if elapsed > 0 else 0
    print(f"Rate: {rate:.0f} nodes/sec")

    # Verify
    verify_registration(db_path)


if __name__ == "__main__":
    main()
