"""
CLI entry points for tiled-catalog-broker.

Provides four commands:
  - tcb generate:       Generate Parquet manifests from finalized YAML
  - tcb stamp-key:      Write the derived catalog key into a YAML
  - tcb register:       HTTP registration against a running Tiled server
  - tcb delete:         Delete registered data from a Tiled server
"""

import os
import sys
import argparse
from pathlib import Path


def _load_config(config_path):
    """Load a dataset config YAML file."""
    from ruamel.yaml import YAML

    yaml = YAML()
    with open(config_path) as f:
        return yaml.load(f)


def _require_key(config, config_path):
    """Read the catalog key from a config; print + exit if missing or drifted.

    `tcb register` is read-only with respect to the YAML; if the key is
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


def _manifest_dirs(config_path, label):
    """Directories that may hold a dataset's manifests, in search order.

    `tcb generate` writes to `<yaml_dir>/manifests/<label>/`; the same path under
    the working directory is also accepted. Labels containing spaces are matched
    both verbatim and underscore-normalized.
    """
    labels = [label]
    if (normalized := label.replace(" ", "_")) != label:
        labels.append(normalized)
    return [root / "manifests" / lbl
            for lbl in labels
            for root in (Path(config_path).parent, Path.cwd())]


def _find_manifests(config_path, label):
    """Locate a dataset's entity and artifact Parquet manifests.

    Returns:
        (Path, Path) or (None, None)
    """
    for d in _manifest_dirs(config_path, label):
        ep, ap = d / "entities.parquet", d / "artifacts.parquet"
        if ep.exists() and ap.exists():
            return ep, ap
    return None, None


# ── tcb generate ─────────────────────────────────────────────

def generate_yaml_main():
    """Generate Parquet manifests from a finalized YAML contract.

    Reads a dataset YAML config (authored against the contract surface —
    see docs/ONBOARDING.md), scans the HDF5 files, and produces
    entities.parquet + artifacts.parquet for `tcb register`.
    """
    from tiled_catalog_broker.tools.generate import main as _generate_main
    _generate_main()


# ── tcb stamp-key ─────────────────────────────────────────────

def stamp_key_main():
    """Write the derived catalog key into a YAML's `key:` field.

    Run this once after authoring a YAML so subsequent `tcb register` calls
    have a key to use. The key is `slugify_key(label)`;
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


# ── tcb register ──────────────────────────────────────────────

def register_main():
    """HTTP registration against a running Tiled server (from register.py).

    Reads dataset config files (YAML), loads corresponding Parquet manifests
    from manifests/, and registers into a running Tiled server via HTTP.
    Incremental: skips entities that already exist.

    With --upload, artifact arrays are read from local HDF5 and written
    through the server into its writable storage (for data the server
    cannot read from its own filesystem).
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
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        metavar="N",
        help="Concurrent registration workers "
             "(default: TCB_MAX_WORKERS env var, or 8)",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload artifact arrays through the server into its writable "
             "storage, instead of registering pointers to HDF5 files the "
             "server reads from disk. Use when the server cannot see your "
             "data's filesystem (e.g. registering from another institution).",
    )
    args = parser.parse_args()

    max_workers = args.max_workers
    if max_workers is None:
        env_val = os.environ.get("TCB_MAX_WORKERS")
        if env_val:
            max_workers = int(env_val)

    import pandas as pd
    from tiled_catalog_broker.utils import check_server
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
        label = config.get("label", Path(config_path).stem)
        dataset_key = _require_key(config, config_path)
        # `data.directory` is the contract (_models.py).
        base_dir = config.get("data", {}).get("directory")
        server_base_dir = config.get("data", {}).get("server_base_dir") or None

        if args.upload:
            if not base_dir or not os.path.isdir(base_dir):
                print(f"\nERROR: {config_path}: data.directory '{base_dir}' "
                      "not found locally. Upload mode reads artifact arrays "
                      "from your local HDF5 files.", file=sys.stderr)
                sys.exit(1)
            if server_base_dir:
                print("NOTE: data.server_base_dir is ignored with --upload "
                      "(the server never reads your files).")

        ent_path, art_path = _find_manifests(config_path, label)
        if ent_path is None or art_path is None:
            print(f"\nERROR: manifests not found for '{label}'. Looked in:")
            for d in _manifest_dirs(config_path, label):
                print(f"    {d}")
            print(f"  Run `tcb generate {config_path}` to create them.")
            sys.exit(1)

        ent_df = pd.read_parquet(ent_path)
        art_df = pd.read_parquet(art_path)

        # Apply limit if specified
        if args.max_entities is not None:
            ent_df = ent_df.head(args.max_entities)

        dataset_metadata = _build_dataset_metadata(config, label)

        kwargs = {}
        if max_workers is not None:
            kwargs["max_workers"] = max_workers

        try:
            register_dataset_http(client, ent_df, art_df, base_dir, label,
                                  dataset_key=dataset_key,
                                  dataset_metadata=dataset_metadata,
                                  server_base_dir=server_base_dir,
                                  upload=args.upload,
                                  **kwargs)
        except ValueError as e:
            print(f"\nERROR: {e}", file=sys.stderr)
            sys.exit(1)

    # Verify
    verify_registration_http(client)

    print("\nDone!")


# ── tcb delete ────────────────────────────────────────────────

def _normalize_url(url):
    """Canonical URL form for the `tcb delete all` confirmation match.

    Lowercases scheme and host, strips trailing slashes from the path.
    Path/query/fragment case is preserved (paths are case-sensitive).
    """
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(url.strip())
    return urlunsplit((
        parts.scheme.lower(),
        parts.netloc.lower(),
        parts.path.rstrip("/"),
        parts.query,
        parts.fragment,
    ))


def delete_main():
    """Delete registered data from a running Tiled server.

    Granularity is inferred from the number of positional arguments:

        tcb delete <DATASET>                       # dataset + everything under it
        tcb delete <DATASET> <ENTITY>              # one entity and its artifacts
        tcb delete <DATASET> <ENTITY> <ARTIFACT>   # one artifact array
        tcb delete all                             # every top-level container

    Note: `"all"` is a reserved sentinel — a dataset whose key is literally
    `all` (case-sensitive) cannot be deleted with the single-arg form.

    Confirmation:
      Granular forms prompt for 'y' or 'yes' (bypass with --yes).
      The 'all' form requires retyping the TILED_URL (bypass with
      --confirm <URL>, which must match exactly).

    External HDF5 files are never removed -- only catalog pointers. For
    datasets registered with --upload, the arrays the server stores are
    deleted along with the nodes (the catalog is their only home).
    """
    parser = argparse.ArgumentParser(
        prog="tcb delete",
        description="Delete registered data from a running Tiled server.",
    )
    parser.add_argument(
        "targets",
        nargs="+",
        metavar="TARGET",
        help="DATASET [ENTITY [ARTIFACT]], or the sentinel 'all'",
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip the interactive y/yes confirmation (granular forms only)",
    )
    parser.add_argument(
        "--confirm",
        metavar="URL",
        help="Bypass the URL-retype prompt for 'all' (must match TILED_URL exactly)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the preview block and exit without deleting",
    )
    args = parser.parse_args()

    from tiled_catalog_broker.utils import check_server
    from tiled_catalog_broker.config import get_tiled_url, get_api_key
    from tiled_catalog_broker.delete import (
        resolve_target, preview_counts, delete_target, delete_all, is_uploaded,
    )

    targets = args.targets
    is_all = targets[0] == "all"
    if is_all and len(targets) > 1:
        print("ERROR: 'all' takes no further arguments.")
        sys.exit(2)
    if not is_all and len(targets) > 3:
        print("ERROR: expected DATASET [ENTITY [ARTIFACT]] (got too many arguments).")
        sys.exit(2)

    print("=" * 50)
    print("Delete")
    print("=" * 50)

    tiled_url = get_tiled_url()
    api_key = get_api_key()

    print(f"\nChecking Tiled server at {tiled_url} ...")
    if not check_server():
        print(f"ERROR: Cannot reach Tiled server at {tiled_url}")
        if not api_key:
            print("\n  No API key set. Export TILED_API_KEY:")
            print("    export TILED_API_KEY=your-key-here")
        print(f"\n  To use a different server, export TILED_URL:")
        print(f"    export TILED_URL={tiled_url}")
        sys.exit(1)
    print("Server is running.")

    from tiled.client import from_uri
    client = from_uri(tiled_url, api_key=api_key)

    # Resolve target and build preview
    uploaded = False
    if is_all:
        granularity = "all"
        path = "(every top-level container)"
        counts = preview_counts(client, granularity)
    else:
        try:
            node, path, granularity = resolve_target(client, *targets)
        except KeyError as e:
            print(f"\nERROR: {e}")
            sys.exit(1)
        counts = preview_counts(node, granularity)
        # The storage marker lives on the dataset container (targets[0]),
        # whichever granularity below it is being deleted.
        uploaded = is_uploaded(client[targets[0]])

    print(f"\nTarget:      {tiled_url}/{path}")
    print(f"Granularity: {granularity}")
    if granularity == "all":
        print(f"Counts:      {counts['n_children']} top-level container(s)")
        if counts["sample_keys"]:
            sample = ", ".join(counts["sample_keys"])
            more = "" if counts["n_children"] <= 10 else f", ... (+{counts['n_children'] - 10} more)"
            print(f"Sample:      {sample}{more}")
    elif granularity == "artifact":
        print(f"Counts:      1 array")
    else:
        print(f"Counts:      {counts['n_children']} child nodes")
    if is_all:
        print("Note:        External datasets keep their HDF5 files; uploaded "
              "datasets lose their server-stored arrays.")
    elif uploaded:
        print("Note:        Uploaded dataset — the arrays stored on the server "
              "WILL be deleted.")
    else:
        print("Note:        External HDF5 files are NOT removed; only catalog entries.")

    if args.dry_run:
        print("\n[--dry-run] No changes made.")
        sys.exit(0)

    # Confirm. Match URL after normalization (lowercase scheme+host, strip
    # trailing slash) so trivially-different shapes of the same URL succeed.
    expected = _normalize_url(tiled_url)
    if is_all:
        if args.confirm is not None:
            if _normalize_url(args.confirm) != expected:
                print(f"\nERROR: --confirm does not match TILED_URL ({tiled_url!r}).")
                sys.exit(1)
        else:
            if not sys.stdin.isatty():
                print("\nERROR: Non-interactive shell. Use --confirm <URL> to proceed.")
                sys.exit(2)
            typed = input(f"\nType the server URL to confirm: ")
            if _normalize_url(typed) != expected:
                print("Aborted: URL did not match.")
                sys.exit(1)
    else:
        if not args.yes:
            if not sys.stdin.isatty():
                print("\nERROR: Non-interactive shell. Use --yes to proceed.")
                sys.exit(2)
            typed = input(f"\nType 'y' or 'yes' to confirm: ").strip().lower()
            if typed not in ("y", "yes"):
                print("Aborted.")
                sys.exit(1)

    # Execute
    from tiled.client.utils import ClientError

    print()
    if is_all:
        successes, failures = delete_all(client)
        for k in successes:
            print(f"  deleted: {k}")
        for k, err in failures:
            print(f"  FAILED:  {k}  ({err})")
        print(f"\n{len(successes)} deleted, {len(failures)} failed.")
        sys.exit(1 if failures else 0)
    else:
        try:
            delete_target(node, external_only=not uploaded)
        except ClientError as e:
            print(f"  FAILED:  {path}")
            print(f"\nERROR: {e}")
            sys.exit(1)
        print(f"  deleted: {path}")
        print("\nDone.")


# ── tcb (main dispatcher) ────────────────────────────────────

def main():
    """Main entry point: tcb <command> [args]."""
    commands = {
        "generate": generate_yaml_main,
        "stamp-key": stamp_key_main,
        "register": register_main,
        "delete": delete_main,
    }

    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print("usage: tcb <command> [args]\n")
        print("commands:")
        print("  generate    Generate Parquet manifests from a finalized YAML contract")
        print("  stamp-key   Write the derived catalog key into a YAML")
        print("  register    HTTP registration against a running Tiled server")
        print("  delete      Delete registered data from a running Tiled server")
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd not in commands:
        print(f"Unknown command: {cmd}")
        print(f"Available: {', '.join(commands)}")
        sys.exit(1)

    # Remove the subcommand from argv so argparse in each handler sees the right args
    sys.argv = [f"tcb {cmd}"] + sys.argv[2:]
    commands[cmd]()
