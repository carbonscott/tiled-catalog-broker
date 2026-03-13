"""
CLI entry points for the broker package.

Provides five commands:
  - broker-inspect:   Scan HDF5 data directory, generate draft YAML contract
  - broker-generate-yaml: Generate Parquet manifests from a YAML contract
  - broker-generate:  Manifest generation from dataset configs (legacy generators)
  - broker-ingest:    Bulk SQL registration from Parquet manifests
  - broker-register:  HTTP registration against a running Tiled server

All paths (catalog.db, manifests/, storage/, datasets/) are resolved
relative to the current working directory.
"""

import sys
import argparse
import importlib
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


def _compute_config_hash(config_path):
    """Compute SHA256 hash of a YAML config file."""
    import hashlib
    with open(config_path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def _build_dataset_metadata(config, label):
    """Build the full dataset container metadata dict from a config."""
    dataset_metadata = config.get("metadata", {"label": label})

    # Merge provenance into dataset container metadata
    if "provenance" in config:
        dataset_metadata.update(config["provenance"])

    # Attach shared axis locators to dataset container metadata
    for ax in config.get("shared", []):
        dataset_metadata[f"shared_dataset_{ax['type']}"] = ax["dataset"]

    return dataset_metadata


def _find_manifests(config_path, label, name):
    """Find entity and artifact Parquet manifests for a dataset config.

    Searches in order:
      1. Next to the YAML file: <yaml_dir>/manifests/<label>/
      2. CWD manifests: manifests/<label>/
      3. Legacy CWD: manifests/<name>_entities.parquet

    Returns:
        (Path, Path) or (None, None)
    """
    yaml_dir = Path(config_path).parent
    candidates = [
        (yaml_dir / "manifests" / label, "next to YAML"),
        (MANIFESTS_DIR / label, f"in {MANIFESTS_DIR}/"),
    ]

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


# ── broker-inspect ───────────────────────────────────────────────

def inspect_main():
    """Scan an HDF5 data directory and generate a draft YAML contract.

    The inspector auto-detects layout (per_entity, batched, grouped),
    classifies datasets, checks consistency, and emits a YAML with
    TODO markers for fields requiring human judgment.
    """
    from broker.inspect import main as _inspect_main
    _inspect_main()


# ── broker-generate-yaml ────────────────────────────────────────

def generate_yaml_main():
    """Generate Parquet manifests from a finalized YAML contract.

    Reads a YAML config (produced by broker-inspect and finalized by user),
    scans the HDF5 files, and produces entities.parquet + artifacts.parquet
    compatible with broker-ingest.
    """
    from broker.generate import main as _generate_main
    _generate_main()


# ── broker-ingest ────────────────────────────────────────────────

def ingest_main():
    """Bulk SQL registration (from ingest.py).

    Reads dataset config files (YAML), loads corresponding Parquet manifests
    from manifests/, and bulk-registers into catalog.db.
    """
    parser = argparse.ArgumentParser(description="Ingest datasets from config files.")
    parser.add_argument("configs", nargs="+", help="Dataset config YAML files")
    args = parser.parse_args()

    import pandas as pd
    from broker.catalog import ensure_catalog, register_dataset

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
    engine = ensure_catalog(DB_PATH, readable_storage, STORAGE_DIR)

    # Register each dataset
    for config_path, (name, config) in zip(args.configs, configs):
        label = config.get("label", config.get("key", name))
        base_dir = config.get("base_dir")
        if base_dir is None and "data" in config:
            base_dir = config["data"].get("directory")

        ent_path, art_path = _find_manifests(config_path, label, name)
        if ent_path is None or art_path is None:
            print(f"\nERROR: Parquet files not found for '{name}'.")
            print(f"  Run broker-generate-yaml or broker-generate first.")
            sys.exit(1)

        ent_df = pd.read_parquet(ent_path)
        art_df = pd.read_parquet(art_path)
        print(f"  Loaded manifests from: {ent_path.parent}")

        dataset_key = config["key"]
        dataset_metadata = _build_dataset_metadata(config, label)
        config_hash = _compute_config_hash(config_path)

        register_dataset(engine, ent_df, art_df, base_dir, label,
                         dataset_key=dataset_key,
                         dataset_metadata=dataset_metadata,
                         config_hash=config_hash)

    # Verify
    from broker.bulk_register import verify_registration
    print()
    verify_registration(str(DB_PATH))

    print("\nDone!")


# ── broker-generate ──────────────────────────────────────────────

def generate_main(default_generators_dir="generators"):
    """Manifest generation (from generate.py).

    Reads dataset config files (YAML) and runs the corresponding manifest
    generator module.

    Args:
        default_generators_dir: Default directory for generator modules.
            The entry point (broker-generate) defaults to "generators/";
            tiled_poc/generate.py passes "extra/".
    """
    parser = argparse.ArgumentParser(description="Generate manifests from dataset configs.")
    parser.add_argument("configs", nargs="+", help="Dataset config YAML files")
    parser.add_argument("-n", type=int, default=10, help="Entities per dataset (default: 10)")
    parser.add_argument(
        "--generators-dir",
        type=str,
        default=default_generators_dir,
        help=f"Directory containing generator modules (default: {default_generators_dir})",
    )
    args = parser.parse_args()

    # Add generators dir to path for imports
    generators_path = Path(args.generators_dir).resolve()
    sys.path.insert(0, str(generators_path))

    manifests_dir = Path("manifests")
    manifests_dir.mkdir(exist_ok=True)

    print("=" * 50)
    print("Manifest Generation")
    print("=" * 50)
    print(f"Configs: {args.configs}")
    print(f"Entities per dataset: {args.n}")
    print(f"Generators dir: {generators_path}")
    print(f"Output: {manifests_dir.resolve()}")

    for config_path in args.configs:
        if not Path(config_path).exists():
            print(f"\nERROR: Config not found: {config_path}")
            sys.exit(1)

        config = _load_config(config_path)
        name = Path(config_path).stem
        label = config.get("label", config["key"])
        generator_module = config["generator"]

        print(f"\n--- Generating {label} ({name}) ---")

        module = importlib.import_module(generator_module)
        module.generate(str(manifests_dir), n_entities=args.n)

    print("\nDone!")


# ── broker-register ──────────────────────────────────────────────

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
    from broker.utils import check_server, get_artifact_shape
    from broker.http_register import register_dataset_http, verify_registration_http

    print("=" * 50)
    print("Register (HTTP)")
    print("=" * 50)
    print(f"Configs: {args.configs}")

    # Check server is running
    from broker.config import get_tiled_url, get_api_key
    tiled_url = get_tiled_url()
    api_key = get_api_key()

    print(f"\nChecking Tiled server at {tiled_url} ...")
    if not check_server():
        print(f"ERROR: Cannot reach Tiled server at {tiled_url}")
        if not api_key:
            print("\n  No API key set. Export TILED_API_KEY:")
            print("    export TILED_API_KEY=your-key-here")
        print(f"\n  To use a different server, export TILED_URL:")
        print(f"    export TILED_URL=http://localhost:8005")
        sys.exit(1)
    print("Server is running.")

    # Connect to Tiled
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
        label = config.get("label", config.get("key", name))
        base_dir = config.get("base_dir")
        if base_dir is None and "data" in config:
            base_dir = config["data"].get("directory")

        ent_path, art_path = _find_manifests(config_path, label, name)
        if ent_path is None or art_path is None:
            print(f"\nERROR: Parquet files not found for '{name}'.")
            print(f"  Run broker-generate-yaml or broker-generate first.")
            sys.exit(1)

        ent_df = pd.read_parquet(ent_path)
        art_df = pd.read_parquet(art_path)

        # Apply limit if specified
        if args.max_entities is not None:
            ent_df = ent_df.head(args.max_entities)

        # Clear shape cache between datasets
        get_artifact_shape.__defaults__[-1].clear()

        dataset_key = config["key"]
        dataset_metadata = _build_dataset_metadata(config, label)
        config_hash = _compute_config_hash(config_path)

        register_dataset_http(client, ent_df, art_df, base_dir, label,
                              dataset_key=dataset_key,
                              dataset_metadata=dataset_metadata,
                              config_hash=config_hash)

    # Verify
    verify_registration_http(client)

    print("\nDone!")
