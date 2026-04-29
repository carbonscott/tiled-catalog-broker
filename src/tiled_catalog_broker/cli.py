"""
CLI entry points for tiled-catalog-broker.

Provides four commands:
  - tcb inspect:        Scan HDF5 data, generate draft YAML contract
  - tcb generate:       Generate Parquet manifests from finalized YAML
  - tcb ingest:         Bulk SQL registration (local testing, deprecated)
  - tcb register:       HTTP registration against a running Tiled server
"""

import argparse
import datetime
import hashlib
import re
import sys
from pathlib import Path

import pandas as pd
from ruamel.yaml import YAML
from sqlalchemy import create_engine
from tiled.catalog import from_uri as catalog_from_uri
from tiled.client import from_uri as tiled_client_from_uri

from .bulk_register import bulk_register, prepare_node_data, verify_registration
from .config import get_api_key, get_tiled_url
from .http_register import register_dataset_http, verify_registration_http
from .tools.generate import main as _generate_main
from .tools.inspect import main as _inspect_main
from .utils import check_server, get_artifact_info, slugify_key

DB_PATH = Path("catalog.db")
MANIFESTS_DIR = Path("manifests")
STORAGE_DIR = Path("storage")


def _load_config(config_path):
    """Load a dataset config YAML file."""
    yaml = YAML()
    with open(config_path) as f:
        return yaml.load(f)


def _resolve_and_persist_key(config, config_path):
    """Compute the catalog key from `label` and persist it in the YAML.

    Rules:
      - key is slugify_key(label) — the reviewer never authors it.
      - If the YAML already has a matching key, it's reused (idempotent).
      - If the YAML has a DIFFERENT key than slug(label) would produce,
        we error: label drift would silently rename the container.
      - If the YAML has no key (or blank), we compute it and patch the
        YAML with a targeted text insertion (preserving every other line
        verbatim — ruamel round-trip reflows paths and list indentation).
    """
    label = config.get("label")
    if not label:
        raise ValueError(
            f"{config_path}: 'label' is required; 'key' is derived from it."
        )
    expected = slugify_key(label)

    current = config.get("key")
    if current and str(current).strip():
        if current != expected:
            raise ValueError(
                f"{config_path}: stored key '{current}' does not match "
                f"slug(label) '{expected}'. Either restore the label that "
                f"produced '{current}', or remove the 'key:' line to "
                f"re-derive it."
            )
        config["key"] = current
        return current

    config["key"] = expected

    stamp = datetime.date.today().isoformat()
    new_line = f"key: {expected}  # auto-filled on {stamp} from slug(label)\n"

    with open(config_path) as f:
        content = f.read()

    placeholder = re.compile(
        r"^#\s*key is auto-filled.*\n", re.MULTILINE
    )
    if placeholder.search(content):
        content = placeholder.sub(new_line, content, count=1)
    else:
        label_re = re.compile(r"^(label\s*:)", re.MULTILINE)
        if label_re.search(content):
            content = label_re.sub(new_line + r"\1", content, count=1)
        else:
            content = new_line + content

    with open(config_path, "w") as f:
        f.write(content)

    print(f"  Assigned key '{expected}' (slug of label '{label}') -> {config_path}")
    return expected


def _compute_config_hash(config_path):
    """Compute SHA256 hash of a YAML config file."""
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
    _inspect_main()


# ── tcb generate ─────────────────────────────────────────────

def generate_yaml_main():
    """Generate Parquet manifests from a finalized YAML contract.

    Reads a YAML config (produced by `tcb inspect` and finalized by user),
    scans the HDF5 files, and produces entities.parquet + artifacts.parquet
    compatible with `tcb ingest`.
    """
    _generate_main()


# ── tcb ingest ────────────────────────────────────────────────

def ingest_main():
    """Bulk SQL registration (from ingest.py).

    Reads dataset config files (YAML), loads corresponding Parquet manifests
    from manifests/, and bulk-registers into catalog.db.
    """
    parser = argparse.ArgumentParser(description="Ingest datasets from config files.")
    parser.add_argument("configs", nargs="+", help="Dataset config YAML files")
    args = parser.parse_args()

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
        dataset_key = _resolve_and_persist_key(config, config_path)
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
        config_hash = _compute_config_hash(config_path)

        ent_nodes, art_nodes, art_data_sources = prepare_node_data(
            ent_df, art_df, max_entities=n, base_dir=base_dir,
            dataset_key=dataset_key,
        )
        bulk_register(engine, ent_nodes, art_nodes, art_data_sources,
                      dataset_key=dataset_key, dataset_metadata=dataset_metadata,
                      config_hash=config_hash)

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

    client = tiled_client_from_uri(tiled_url, api_key=api_key)
    print(f"Connected to {tiled_url} ({len(client)} existing containers)")

    # Load and register each dataset
    for config_path in args.configs:
        if not Path(config_path).exists():
            print(f"\nERROR: Config not found: {config_path}")
            sys.exit(1)

        config = _load_config(config_path)
        name = Path(config_path).stem
        label = config.get("label", name)
        dataset_key = _resolve_and_persist_key(config, config_path)
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
        "ingest": ingest_main,
        "register": register_main,
    }

    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("usage: tcb <command> [args]\n")
        print("commands:")
        print("  inspect    Scan HDF5 data directory, generate draft YAML contract")
        print("  generate   Generate Parquet manifests from a finalized YAML contract")
        print("  ingest     Bulk SQL registration from Parquet manifests")
        print("  register   HTTP registration against a running Tiled server")
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd not in commands:
        print(f"Unknown command: {cmd}")
        print(f"Available: {', '.join(commands)}")
        sys.exit(1)

    # Remove the subcommand from argv so argparse in each handler sees the right args
    sys.argv = [f"tcb {cmd}"] + sys.argv[2:]
    commands[cmd]()
