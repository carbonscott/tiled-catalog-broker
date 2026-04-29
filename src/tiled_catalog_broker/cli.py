"""
CLI entry points for tiled-catalog-broker.

Provides five commands:
  - tcb inspect:        Scan HDF5 data, generate draft YAML contract
  - tcb generate:       Generate Parquet manifests from finalized YAML
  - tcb stamp-key:      Write the derived catalog key into a YAML
  - tcb ingest:         Bulk SQL registration (local testing, deprecated)
  - tcb register:       HTTP registration against a running Tiled server
"""

import sys
import argparse
from pathlib import Path

DB_PATH = Path("catalog.db")
MANIFESTS_DIR = Path("manifests")
STORAGE_DIR = Path("storage")


def _load_config(config_path):
    """Load a dataset config YAML file."""
    from ruamel.yaml import YAML

    yaml = YAML()
    with open(config_path) as f:
        return yaml.load(f)


def _require_key(config, config_path):
    """Read the catalog key from a config; print + exit if missing or drifted.

    register/ingest are read-only with respect to the YAML; if the key is
    missing, the user runs `tcb stamp-key` to fill it in.
    """
    from .utils import slugify_key

    label = config.get("label")
    if not label:
        print(f"\nERROR: {config_path}: 'label' is required.", file=sys.stderr)
        sys.exit(1)
    expected = slugify_key(label)

    current = config.get("key")
    if not current or not str(current).strip():
        print(
            f"\nERROR: {config_path}: missing 'key' field. Run\n"
            f"    tcb stamp-key {config_path}\n"
            f"to derive '{expected}' from the label.",
            file=sys.stderr,
        )
        sys.exit(1)
    if current != expected:
        print(
            f"\nERROR: {config_path}: stored key '{current}' does not match "
            f"slug(label) '{expected}'. Either restore the label that "
            f"produced '{current}', or remove the 'key:' line and run "
            f"`tcb stamp-key`.",
            file=sys.stderr,
        )
        sys.exit(1)
    return current


def _build_dataset_metadata(config, label):
    """Build the full dataset container metadata dict from a config."""
    dataset_metadata = config.get("metadata", {"label": label})

    # Merge provenance into dataset container metadata. The block can be
    # present but empty (all fields commented out), which ruamel parses as
    # None rather than {} — guard so dict.update(None) doesn't crash.
    provenance = config.get("provenance")
    if provenance:
        dataset_metadata.update(provenance)

    # Attach shared axis locators to dataset container metadata
    for ax in config.get("shared", []):
        dataset_metadata[f"shared_dataset_{ax['type']}"] = ax["dataset"]

    return dataset_metadata


def _find_manifests(config_path, label, name):
    """Find entity and artifact Parquet manifests for a dataset config.

    Searches in order:
      1. Next to the YAML file: <yaml_dir>/manifests/<label>/
      2. CWD manifests: manifests/<label>/
      3. Same as 1-2 but with underscores instead of spaces
      4. Legacy CWD: manifests/<name>_entities.parquet

    Returns:
        (Path, Path) or (None, None)
    """
    yaml_dir = Path(config_path).parent
    # Try both exact label and underscore-normalized version
    label_variants = [label]
    normalized = label.replace(" ", "_")
    if normalized != label:
        label_variants.append(normalized)

    candidates = []
    for lbl in label_variants:
        candidates.append((yaml_dir / "manifests" / lbl, "next to YAML"))
        candidates.append((MANIFESTS_DIR / lbl, f"in {MANIFESTS_DIR}/"))

    for cand_dir, desc in candidates:
        ep = cand_dir / "entities.parquet"
        ap = cand_dir / "artifacts.parquet"
        if ep.exists() and ap.exists():
            return ep, ap

    # Legacy fallback
    ep = MANIFESTS_DIR / f"{name}_entities.parquet"
    ap = MANIFESTS_DIR / f"{name}_artifacts.parquet"
    if ep.exists() and ap.exists():
        return ep, ap

    return None, None


# ── tcb inspect ───────────────────────────────────────────────

def inspect_main():
    """Scan an HDF5 data directory and generate a draft YAML contract.

    The inspector auto-detects layout (per_entity, batched, grouped),
    classifies datasets, checks consistency, and emits a YAML with
    TODO markers for fields requiring human judgment.
    """
    from tiled_catalog_broker.tools.inspect import main as _inspect_main
    _inspect_main()


# ── tcb generate ─────────────────────────────────────────────

def generate_yaml_main():
    """Generate Parquet manifests from a finalized YAML contract.

    Reads a YAML config (produced by `tcb inspect` and finalized by user),
    scans the HDF5 files, and produces entities.parquet + artifacts.parquet
    compatible with `tcb ingest`.
    """
    from tiled_catalog_broker.tools.generate import main as _generate_main
    _generate_main()


# ── tcb stamp-key ─────────────────────────────────────────────

def stamp_key_main():
    """Write the derived catalog key into a YAML's `key:` field.

    Run this once after authoring a YAML so subsequent `tcb register` and
    `tcb ingest` calls have a key to use. The key is `slugify_key(label)`;
    re-running on an already-correct YAML is a no-op. Mismatch between an
    existing key and slug(label) is an error.
    """
    from ruamel.yaml import YAML

    from .utils import slugify_key

    parser = argparse.ArgumentParser(
        description="Write the derived catalog key into one or more dataset YAMLs."
    )
    parser.add_argument("configs", nargs="+", help="Dataset YAML files")
    args = parser.parse_args()

    yaml = YAML()
    yaml.preserve_quotes = True

    for config_path in args.configs:
        path = Path(config_path)
        if not path.exists():
            print(f"ERROR: {config_path} not found", file=sys.stderr)
            sys.exit(1)

        with path.open() as f:
            cfg = yaml.load(f)

        label = cfg.get("label")
        if not label:
            print(f"ERROR: {config_path}: 'label' is required", file=sys.stderr)
            sys.exit(1)

        expected = slugify_key(label)
        current = cfg.get("key")

        if current and str(current).strip():
            if current == expected:
                print(f"{config_path}: key '{current}' already correct (no change).")
                continue
            print(
                f"ERROR: {config_path}: stored key '{current}' does not match "
                f"slug(label) '{expected}'. Either restore the label that "
                f"produced '{current}', or remove the 'key:' line and re-run.",
                file=sys.stderr,
            )
            sys.exit(1)

        cfg["key"] = expected
        with path.open("w") as f:
            yaml.dump(cfg, f)
        print(f"{config_path}: stamped key '{expected}' (slug of label '{label}')")


# ── tcb ingest ────────────────────────────────────────────────

def ingest_main():
    """Bulk SQL registration (from ingest.py).

    Reads dataset config files (YAML), loads corresponding Parquet manifests
    from manifests/, and bulk-registers into catalog.db.
    """
    parser = argparse.ArgumentParser(description="Ingest datasets from config files.")
    parser.add_argument("configs", nargs="+", help="Dataset config YAML files")
    args = parser.parse_args()

    import pandas as pd
    from sqlalchemy import create_engine
    from tiled_catalog_broker.bulk_register import prepare_node_data, bulk_register, verify_registration
    from tiled_catalog_broker.utils import get_artifact_info

    print("=" * 50)
    print("Ingest")
    print("=" * 50)
    print(f"Configs: {args.configs}")
    print(f"Database: {DB_PATH.resolve()}")

    # Collect base_dirs from all configs for readable_storage
    configs = []
    for config_path in args.configs:
        if not Path(config_path).exists():
            print(f"\nERROR: Config not found: {config_path}")
            sys.exit(1)
        config = _load_config(config_path)
        name = Path(config_path).stem
        configs.append((name, config))

    readable_storage = []
    for _, c in configs:
        if "base_dir" in c:
            readable_storage.append(c["base_dir"])
        elif "data" in c and "directory" in c["data"]:
            readable_storage.append(c["data"]["directory"])

    # Ensure catalog exists
    STORAGE_DIR.mkdir(exist_ok=True)
    uri = f"sqlite:///{DB_PATH}"
    if not DB_PATH.exists():
        from tiled.catalog import from_uri as catalog_from_uri
        print(f"  Creating new catalog: {DB_PATH}")
        catalog_from_uri(
            uri,
            writable_storage=str(STORAGE_DIR),
            readable_storage=readable_storage,
            init_if_not_exists=True,
        )
    else:
        print(f"  Using existing catalog: {DB_PATH}")
    engine = create_engine(uri)

    # Register each dataset
    for config_path, (name, config) in zip(args.configs, configs):
        label = config.get("label", name)
        dataset_key = _require_key(config, config_path)
        base_dir = config.get("base_dir")
        if base_dir is None and "data" in config:
            base_dir = config["data"].get("directory")

        # Clear shape cache between datasets
        get_artifact_info.__defaults__[-1].clear()

        ent_path, art_path = _find_manifests(config_path, label, name)
        if ent_path is None or art_path is None:
            print(f"\nERROR: Parquet files not found for '{name}'.")
            print(f"  Run `tcb generate` first.")
            sys.exit(1)

        ent_df = pd.read_parquet(ent_path)
        art_df = pd.read_parquet(art_path)
        print(f"  Loaded manifests from: {ent_path.parent}")

        n = len(ent_df)
        print(f"\n--- Registering {label} ({n} entities) ---")

        dataset_metadata = _build_dataset_metadata(config, label)

        ent_nodes, art_nodes, art_data_sources = prepare_node_data(
            ent_df, art_df, max_entities=n, base_dir=base_dir,
            dataset_key=dataset_key,
        )
        bulk_register(engine, ent_nodes, art_nodes, art_data_sources,
                      dataset_key=dataset_key, dataset_metadata=dataset_metadata)

    # Verify
    print()
    verify_registration(str(DB_PATH))

    print("\nDone!")


# ── tcb register ──────────────────────────────────────────────

def register_main():
    """HTTP registration against a running Tiled server (from register.py).

    Reads dataset config files (YAML), loads corresponding Parquet manifests
    from manifests/, and registers into a running Tiled server via HTTP.
    Incremental: skips entities that already exist.
    """
    parser = argparse.ArgumentParser(
        description="Register datasets into a running Tiled server via HTTP."
    )
    parser.add_argument("configs", nargs="+", help="Dataset config YAML files")
    parser.add_argument(
        "-n", "--max-entities",
        type=int,
        default=None,
        metavar="NUM",
        help="Limit number of entities per dataset (default: all)",
    )
    args = parser.parse_args()

    import pandas as pd
    from tiled_catalog_broker.utils import check_server, get_artifact_info
    from tiled_catalog_broker.http_register import register_dataset_http, verify_registration_http
    from tiled_catalog_broker.config import get_tiled_url, get_api_key

    print("=" * 50)
    print("Register (HTTP)")
    print("=" * 50)
    print(f"Configs: {args.configs}")

    tiled_url = get_tiled_url()
    api_key = get_api_key()

    print(f"\nChecking Tiled server at {tiled_url} ...")
    if not check_server(url=tiled_url, api_key=api_key):
        print(f"ERROR: Cannot reach Tiled server at {tiled_url}")
        if not api_key:
            print("\n  No API key set. Export TILED_API_KEY:")
            print("    export TILED_API_KEY=your-key-here")
        print("\n  To use a different server, export TILED_URL.")
        sys.exit(1)
    print("Server is running.")

    from tiled.client import from_uri
    client = from_uri(tiled_url, api_key=api_key)
    print(f"Connected to {tiled_url} ({len(client)} existing containers)")

    # Load and register each dataset
    for config_path in args.configs:
        if not Path(config_path).exists():
            print(f"\nERROR: Config not found: {config_path}")
            sys.exit(1)

        config = _load_config(config_path)
        name = Path(config_path).stem
        label = config.get("label", name)
        dataset_key = _require_key(config, config_path)
        base_dir = config.get("base_dir")
        if base_dir is None and "data" in config:
            base_dir = config["data"].get("directory")

        ent_path, art_path = _find_manifests(config_path, label, name)
        if ent_path is None or art_path is None:
            print(f"\nERROR: Parquet files not found for '{name}'.")
            print(f"  Run `tcb generate` first.")
            sys.exit(1)

        ent_df = pd.read_parquet(ent_path)
        art_df = pd.read_parquet(art_path)

        # Apply limit if specified
        if args.max_entities is not None:
            ent_df = ent_df.head(args.max_entities)

        # Clear shape cache between datasets
        get_artifact_info.__defaults__[-1].clear()

        dataset_metadata = _build_dataset_metadata(config, label)

        register_dataset_http(client, ent_df, art_df, base_dir, label,
                              dataset_key=dataset_key,
                              dataset_metadata=dataset_metadata)

    # Verify
    verify_registration_http(client)

    print("\nDone!")


# ── tcb (main dispatcher) ────────────────────────────────────

def main():
    """Main entry point: tcb <command> [args]."""
    commands = {
        "inspect": inspect_main,
        "generate": generate_yaml_main,
        "stamp-key": stamp_key_main,
        "ingest": ingest_main,
        "register": register_main,
    }

    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("usage: tcb <command> [args]\n")
        print("commands:")
        print("  inspect     Scan HDF5 data directory, generate draft YAML contract")
        print("  generate    Generate Parquet manifests from a finalized YAML contract")
        print("  stamp-key   Write the derived catalog key into a YAML")
        print("  ingest      Bulk SQL registration from Parquet manifests")
        print("  register    HTTP registration against a running Tiled server")
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd not in commands:
        print(f"Unknown command: {cmd}")
        print(f"Available: {', '.join(commands)}")
        sys.exit(1)

    # Remove the subcommand from argv so argparse in each handler sees the right args
    sys.argv = [f"tcb {cmd}"] + sys.argv[2:]
    commands[cmd]()
