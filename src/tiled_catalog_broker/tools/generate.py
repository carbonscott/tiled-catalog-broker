"""
Generic manifest generator.

Reads a finalized YAML contract and produces Parquet manifests
(entities.parquet and artifacts.parquet) for Tiled registration.

The output manifests follow the broker standard:
  Entity manifest:  uid, key, <param_1>, <param_2>, ...
  Artifact manifest: uid (= entity uid), type, file, dataset, [index]

Handles three layout patterns:
  - per_entity: one HDF5 file per entity, scalars are parameters
  - batched: entities stacked along axis-0 of datasets in each file
  - grouped: one HDF5 group per entity inside a single file

Supports five parameter locations:
  - root_scalars: scalar HDF5 datasets at file root
  - root_attributes: HDF5 root-level file attributes (f.attrs)
  - group: datasets inside a named HDF5 group (e.g., /params)
  - group_scalars: scalars inside entity groups (grouped layout)
  - manifest: external CSV or Parquet file with parameter columns

Usage:
    dcs generate datasets/edrixs_sbi.yml
    dcs generate datasets/edrixs_sbi.yml --append
"""

import argparse
import datetime
import hashlib
import json
import os
import sys
from collections import OrderedDict
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from ruamel.yaml import YAML

from ..utils import slugify_key
from .schema import ValidationError, validate


# Columns in external parameter manifests that are not physics parameters.
_PARAM_MANIFEST_SKIP_COLS = {"file", "filename", "sample_idx", "output_file"}


def load_yaml(yaml_path):
    """Load and validate a dataset YAML config."""
    yaml = YAML()
    with open(yaml_path) as f:
        cfg = yaml.load(f)
    warnings = validate(cfg)
    for w in warnings:
        print(f"  Warning: {w}")
    return cfg


def compute_config_hash(yaml_path):
    """Compute SHA256 hash of a YAML config file's content."""
    with open(yaml_path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def _file_fingerprint(path):
    """Return (size_bytes, mtime_iso) for a file path."""
    stat = os.stat(path)
    mtime = datetime.datetime.fromtimestamp(
        stat.st_mtime, tz=datetime.timezone.utc
    ).isoformat()
    return stat.st_size, mtime


def generate_manifests(yaml_path, output_dir=None, append=False):
    """Generate entity and artifact manifests from a YAML config.

    Args:
        yaml_path: Path to the finalized YAML config.
        output_dir: Directory for output Parquet files (default: manifests/<label>/).
        append: If True, skip entities already in existing manifests and
            merge new entities with the existing ones.

    Returns:
        (str, str): Paths to entities.parquet and artifacts.parquet.
    """
    cfg = load_yaml(yaml_path)
    config_hash = compute_config_hash(yaml_path)

    label = cfg["label"]
    # UIDs must be stable whether or not `key` has been stamped into the YAML
    # yet by `tcb register`; always derive from slug(label).
    key_prefix = cfg.get("key") or cfg.get("key_prefix") or slugify_key(label)
    data = cfg["data"]
    directory = data["directory"]
    file_pattern = data.get("file_pattern", "**/*.h5")
    layout = data["layout"]

    if output_dir is None:
        output_dir = os.path.join(os.path.dirname(yaml_path) or ".", "manifests", label)
    os.makedirs(output_dir, exist_ok=True)

    artifacts_cfg = cfg.get("artifacts", [])
    shared_cfg = cfg.get("shared", [])
    params_cfg = cfg.get("parameters", {})
    extra_meta_cfg = cfg.get("extra_metadata", [])

    # Load external parameter manifest if location is "manifest"
    param_manifest = None
    if params_cfg.get("location") == "manifest":
        mpath = params_cfg["manifest"]
        if not os.path.isabs(mpath):
            mpath = os.path.join(directory, mpath)
        if mpath.endswith(".csv"):
            param_manifest = pd.read_csv(mpath)
        else:
            param_manifest = pd.read_parquet(mpath)
        print(f"  Loaded parameter manifest: {mpath} ({len(param_manifest)} rows)")

    # Load existing UIDs for append mode
    existing_uids = set()
    if append:
        existing_ent_path = os.path.join(output_dir, "entities.parquet")
        if os.path.exists(existing_ent_path):
            existing_df = pd.read_parquet(existing_ent_path, columns=["uid"])
            existing_uids = set(existing_df["uid"])
            print(f"  Append mode: {len(existing_uids)} existing entities will be skipped")

    # Find HDF5 files
    root = Path(directory)
    h5_files = sorted(root.glob(file_pattern))
    if not h5_files:
        h5_files = sorted(root.rglob(file_pattern))
    if not h5_files:
        print(f"Error: No HDF5 files matching '{file_pattern}' in {directory}")
        sys.exit(1)
    print(f"Found {len(h5_files)} HDF5 files")

    if layout == "per_entity":
        ent_rows, art_rows = _generate_per_entity(
            h5_files, root, key_prefix, artifacts_cfg, shared_cfg,
            params_cfg, extra_meta_cfg, cfg, param_manifest, existing_uids,
        )
    elif layout == "batched":
        ent_rows, art_rows = _generate_batched(
            h5_files, root, key_prefix, artifacts_cfg, shared_cfg,
            params_cfg, extra_meta_cfg, cfg, param_manifest, existing_uids,
        )
    elif layout == "grouped":
        ent_rows, art_rows = _generate_grouped(
            h5_files, root, key_prefix, artifacts_cfg, shared_cfg,
            params_cfg, extra_meta_cfg, cfg, param_manifest, existing_uids,
        )
    else:
        print(f"Error: Unknown layout '{layout}'")
        sys.exit(1)

    # Build DataFrames
    ent_df = pd.DataFrame(ent_rows)
    art_df = pd.DataFrame(art_rows)

    # In append mode, merge with existing manifests
    if append and existing_uids:
        old_ent_path = os.path.join(output_dir, "entities.parquet")
        old_art_path = os.path.join(output_dir, "artifacts.parquet")
        if os.path.exists(old_ent_path) and os.path.exists(old_art_path):
            old_ent = pd.read_parquet(old_ent_path)
            old_art = pd.read_parquet(old_art_path)
            ent_df = pd.concat([old_ent, ent_df], ignore_index=True)
            art_df = pd.concat([old_art, art_df], ignore_index=True)
            print(f"  Merged: {len(old_ent)} existing + {len(ent_rows)} new entities")

    # Write Parquet with provenance metadata
    ent_path = os.path.join(output_dir, "entities.parquet")
    art_path = os.path.join(output_dir, "artifacts.parquet")

    generation_meta = {
        b"generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat().encode(),
        b"generator": b"broker-generate-yaml",
        b"source_yaml": os.path.basename(str(yaml_path)).encode(),
        b"config_hash": config_hash.encode(),
        b"layout": layout.encode(),
        b"entity_count": str(len(ent_df)).encode(),
        b"artifact_count": str(len(art_df)).encode(),
    }

    ent_table = pa.Table.from_pandas(ent_df)
    ent_table = ent_table.replace_schema_metadata(
        {**(ent_table.schema.metadata or {}), **generation_meta}
    )
    pq.write_table(ent_table, ent_path)

    art_table = pa.Table.from_pandas(art_df)
    art_table = art_table.replace_schema_metadata(
        {**(art_table.schema.metadata or {}), **generation_meta}
    )
    pq.write_table(art_table, art_path)

    print(f"Entities: {len(ent_df)} rows -> {ent_path}")
    print(f"Artifacts: {len(art_df)} rows -> {art_path}")

    return ent_path, art_path


# ---------------------------------------------------------------------------
# Per-entity layout
# ---------------------------------------------------------------------------

def _warn_mixed_uid_paths(content_count, fallback_count, key_prefix):
    """Warn if a dataset mixes content-addressed UIDs with positional fallbacks."""
    if content_count and fallback_count:
        print(
            f"  WARNING: {key_prefix}: mixed UID paths "
            f"({content_count} content-addressed, {fallback_count} positional fallback)"
        )


def _generate_per_entity(h5_files, root, key_prefix, artifacts_cfg,
                         shared_cfg, params_cfg, extra_meta_cfg, cfg,
                         param_manifest=None, existing_uids=None):
    """One HDF5 file = one entity. Scalars at root are parameters."""
    ent_rows = []
    art_rows = []
    content_count = fallback_count = 0
    if existing_uids is None:
        existing_uids = set()

    # Cache file fingerprints to avoid repeated stat calls
    _fingerprint_cache = {}

    for i, h5_path in enumerate(h5_files):
        rel_path = str(h5_path.relative_to(root))
        file_stem = h5_path.stem

        # Read the entity's physical parameters first — the UID is a
        # content-addressed hash of these, not of file position.
        entity_params = {}
        extra_meta = {}
        loc = params_cfg.get("location", "root_scalars")

        if loc == "manifest" and param_manifest is not None:
            first_col = param_manifest.columns[0]
            match = param_manifest[
                param_manifest[first_col].astype(str) == file_stem
            ]
            if not match.empty:
                row = match.iloc[0]
                for col in param_manifest.columns:
                    if col not in _PARAM_MANIFEST_SKIP_COLS:
                        val = row[col]
                        if pd.notna(val):
                            entity_params[col] = _to_python(val)
        else:
            with h5py.File(h5_path, "r") as f:
                if loc == "root_scalars":
                    for ds_name in sorted(f.keys()):
                        ds = f[ds_name]
                        if isinstance(ds, h5py.Dataset) and ds.ndim == 0:
                            entity_params[ds_name] = _to_python(ds[()])
                elif loc == "root_attributes":
                    for attr_name in sorted(f.attrs.keys()):
                        entity_params[attr_name] = _to_python(f.attrs[attr_name])
                elif loc == "group":
                    group_name = params_cfg["group"].lstrip("/")
                    if group_name in f:
                        for pname in sorted(f[group_name].keys()):
                            ds = f[group_name][pname]
                            if isinstance(ds, h5py.Dataset):
                                entity_params[pname] = _to_python(ds[()])

                # Extra metadata (stored per-entity but not part of the UID hash)
                for extra in extra_meta_cfg:
                    ds_path = extra["dataset"].lstrip("/")
                    if ds_path in f:
                        ds = f[ds_path]
                        if isinstance(ds, h5py.Dataset):
                            if ds.ndim == 0:
                                extra_meta[ds_path] = _to_python(ds[()])
                            elif ds.ndim == 1 and ds.size <= 10:
                                extra_meta[ds_path] = ds[:].tolist()

        if entity_params:
            uid = _make_uid(entity_params, namespace=key_prefix)
            content_count += 1
        else:
            uid = _make_uid(f"{key_prefix}_{file_stem}")
            fallback_count += 1

        if uid in existing_uids:
            continue

        if rel_path not in _fingerprint_cache:
            _fingerprint_cache[rel_path] = _file_fingerprint(h5_path)

        entity_row = OrderedDict()
        entity_row["uid"] = uid
        entity_row.update(entity_params)
        entity_row.update(extra_meta)

        ent_rows.append(entity_row)

        # Artifact rows — uid matches entity uid for groupby in bulk_register
        # Only include artifacts whose datasets exist in this file
        fsize, fmtime = _fingerprint_cache[rel_path]
        try:
            with h5py.File(h5_path, "r") as f:
                for art in artifacts_cfg:
                    ds_path = art["dataset"].lstrip("/")
                    if ds_path not in f:
                        continue
                    art_row = OrderedDict()
                    art_row["uid"] = uid
                    art_row["type"] = art["type"]
                    art_row["file"] = rel_path
                    art_row["dataset"] = art["dataset"]
                    art_row["index"] = None
                    art_row["file_size"] = fsize
                    art_row["file_mtime"] = fmtime
                    art_rows.append(art_row)
        except OSError:
            # File may be locked or corrupted — skip artifacts
            pass

        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(h5_files)} entities...")

    _warn_mixed_uid_paths(content_count, fallback_count, key_prefix)
    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Batched layout
# ---------------------------------------------------------------------------

def _generate_batched(h5_files, root, key_prefix, artifacts_cfg,
                      shared_cfg, params_cfg, extra_meta_cfg, cfg,
                      param_manifest=None, existing_uids=None):
    """Multiple entities stacked along axis-0 in each file."""
    ent_rows = []
    art_rows = []
    content_count = fallback_count = 0
    global_idx = 0
    if existing_uids is None:
        existing_uids = set()

    for h5_path in h5_files:
        rel_path = str(h5_path.relative_to(root))
        fsize, fmtime = _file_fingerprint(h5_path)

        with h5py.File(h5_path, "r") as f:
            # Determine batch size from first artifact
            first_art_ds = artifacts_cfg[0]["dataset"].lstrip("/")
            batch_size = f[first_art_ds].shape[0]

            # Read all parameters at once
            param_arrays = {}
            loc = params_cfg.get("location", "group")
            if loc == "group":
                group_name = params_cfg["group"].lstrip("/")
                if group_name in f:
                    for pname in sorted(f[group_name].keys()):
                        param_arrays[pname] = f[group_name][pname][:]
            elif loc == "root_scalars":
                for ds_name in sorted(f.keys()):
                    ds = f[ds_name]
                    if isinstance(ds, h5py.Dataset) and ds.ndim == 1 and ds.shape[0] == batch_size:
                        param_arrays[ds_name] = ds[:]
            elif loc == "root_attributes":
                # Attributes are scalars — same value for all entities in batch
                root_attr_params = {
                    attr_name: _to_python(f.attrs[attr_name])
                    for attr_name in sorted(f.attrs.keys())
                }
            # loc == "manifest" handled below per-entity

            # Read extra metadata arrays
            extra_arrays = {}
            for extra in extra_meta_cfg:
                ds_path = extra["dataset"].lstrip("/")
                if ds_path in f:
                    ds = f[ds_path]
                    if isinstance(ds, h5py.Dataset) and ds.ndim >= 1 and ds.shape[0] == batch_size:
                        extra_arrays[ds_path] = ds[:]

            for i in range(batch_size):
                # Collect the entity's physical parameters FIRST; the UID is
                # a content-addressed hash of these, not of the global index.
                entity_params = {}
                if loc == "manifest" and param_manifest is not None:
                    pm_idx = global_idx
                    if pm_idx < len(param_manifest):
                        row = param_manifest.iloc[pm_idx]
                        for col in param_manifest.columns:
                            if col not in _PARAM_MANIFEST_SKIP_COLS:
                                val = row[col]
                                if pd.notna(val):
                                    entity_params[col] = _to_python(val)
                elif loc == "root_attributes":
                    entity_params = dict(root_attr_params)
                else:
                    for pname, arr in param_arrays.items():
                        entity_params[pname] = _to_python(arr[i])

                if entity_params:
                    uid = _make_uid(entity_params, namespace=key_prefix)
                    content_count += 1
                else:
                    uid = _make_uid(f"{key_prefix}_{global_idx:06d}")
                    fallback_count += 1

                if uid in existing_uids:
                    global_idx += 1
                    continue

                entity_row = OrderedDict()
                entity_row["uid"] = uid
                entity_row.update(entity_params)

                # Extra metadata (stored per-entity, not part of the UID)
                for ds_path, arr in extra_arrays.items():
                    col_name = ds_path.rsplit("/", 1)[-1]
                    if arr.ndim == 1:
                        entity_row[col_name] = _to_python(arr[i])
                    elif arr.ndim > 1:
                        entity_row[col_name] = arr[i].tolist()

                ent_rows.append(entity_row)

                # Artifact rows — uid matches entity uid
                for art in artifacts_cfg:
                    art_row = OrderedDict()
                    art_row["uid"] = uid
                    art_row["type"] = art["type"]
                    art_row["file"] = rel_path
                    art_row["dataset"] = art["dataset"]
                    art_row["index"] = i
                    art_row["file_size"] = fsize
                    art_row["file_mtime"] = fmtime
                    art_rows.append(art_row)

                global_idx += 1

        print(f"  Processed {h5_path.name}: {batch_size} entities (total: {global_idx})")

    _warn_mixed_uid_paths(content_count, fallback_count, key_prefix)
    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Grouped layout
# ---------------------------------------------------------------------------

def _generate_grouped(h5_files, root, key_prefix, artifacts_cfg,
                      shared_cfg, params_cfg, extra_meta_cfg, cfg,
                      param_manifest=None, existing_uids=None):
    """One HDF5 group per entity inside a file."""
    ent_rows = []
    art_rows = []
    content_count = fallback_count = 0
    global_idx = 0
    if existing_uids is None:
        existing_uids = set()

    entity_group = params_cfg.get("entity_group", "samples")

    for h5_path in h5_files:
        rel_path = str(h5_path.relative_to(root))
        fsize, fmtime = _file_fingerprint(h5_path)

        with h5py.File(h5_path, "r") as f:
            if entity_group in f and isinstance(f[entity_group], h5py.Group):
                group_keys = sorted(f[entity_group].keys())
                base_group = entity_group
            else:
                group_keys = [k for k in sorted(f.keys()) if isinstance(f[k], h5py.Group)]
                base_group = ""

            for gkey in group_keys:
                full_group = f"{base_group}/{gkey}" if base_group else gkey
                g = f[full_group]

                # Read parameters first — UID is a content-addressed hash
                # of the params, not of the group's position in the file.
                entity_params = {}
                loc = params_cfg.get("location", "group_scalars")
                if loc == "manifest" and param_manifest is not None:
                    pm_idx = global_idx
                    if pm_idx < len(param_manifest):
                        row = param_manifest.iloc[pm_idx]
                        for col in param_manifest.columns:
                            if col not in _PARAM_MANIFEST_SKIP_COLS:
                                val = row[col]
                                if pd.notna(val):
                                    entity_params[col] = _to_python(val)
                elif loc == "group_scalars":
                    param_group = params_cfg.get("group", "params")
                    param_path = param_group.lstrip("/")
                    if param_path in g and isinstance(g[param_path], h5py.Group):
                        for pname in sorted(g[param_path].keys()):
                            ds = g[param_path][pname]
                            if isinstance(ds, h5py.Dataset):
                                entity_params[pname] = _to_python(ds[()])
                    else:
                        for ds_name in sorted(g.keys()):
                            ds = g[ds_name]
                            if isinstance(ds, h5py.Dataset) and ds.ndim == 0:
                                entity_params[ds_name] = _to_python(ds[()])
                elif loc == "root_attributes":
                    for attr_name in sorted(f.attrs.keys()):
                        entity_params[attr_name] = _to_python(f.attrs[attr_name])

                if entity_params:
                    uid = _make_uid(entity_params, namespace=key_prefix)
                    content_count += 1
                else:
                    uid = _make_uid(f"{key_prefix}_{global_idx:06d}")
                    fallback_count += 1

                if uid in existing_uids:
                    global_idx += 1
                    continue

                entity_row = OrderedDict()
                entity_row["uid"] = uid
                entity_row["source_group"] = full_group
                entity_row.update(entity_params)

                ent_rows.append(entity_row)

                # Artifact rows
                for art in artifacts_cfg:
                    art_type = art["type"]
                    ds_path = art["dataset"].lstrip("/")
                    full_ds_path = f"/{full_group}/{ds_path}"

                    art_row = OrderedDict()
                    art_row["uid"] = uid
                    art_row["type"] = art_type
                    art_row["file"] = rel_path
                    art_row["dataset"] = full_ds_path
                    art_row["index"] = None
                    art_row["file_size"] = fsize
                    art_row["file_mtime"] = fmtime
                    art_rows.append(art_row)

                global_idx += 1

        print(f"  Processed {h5_path.name}: {len(group_keys)} entity groups (total: {global_idx})")

    _warn_mixed_uid_paths(content_count, fallback_count, key_prefix)
    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_uid(params_or_str, namespace=""):
    """Generate a deterministic UID.

    Content-addressed form (preferred): pass a params dict. The UID is a
    hash of the canonical JSON of the (namespace, params) pair, so the
    same parameter set produces the same UID regardless of position in
    the file, file order, or regeneration. Floats are rounded to 12
    decimal places before hashing to tolerate minor float-format drift.

    Positional fallback: pass a string. Used when no per-entity
    parameters are discoverable in the data (e.g. per-entity layout
    with parameters only in filenames).
    """
    if isinstance(params_or_str, dict):
        canonical = {
            k: round(v, 12) if isinstance(v, float) else v
            for k, v in sorted(params_or_str.items())
        }
        payload = json.dumps(
            {"ns": namespace, "params": canonical}, sort_keys=True
        )
    else:
        payload = params_or_str
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _to_python(val):
    """Convert numpy/HDF5 value to Python native type."""
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    if isinstance(val, np.generic):
        return val.item()
    if isinstance(val, np.ndarray):
        if val.size == 1:
            return val.item()
        return val.tolist()
    return val


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate Parquet manifests from a dataset YAML contract."
    )
    parser.add_argument("yaml_path", help="Path to the finalized dataset YAML config")
    parser.add_argument("--output-dir", "-o", help="Output directory for manifests")
    parser.add_argument(
        "--append", action="store_true",
        help="Append new entities to existing manifests (skip already-generated UIDs)",
    )
    args = parser.parse_args()

    try:
        generate_manifests(args.yaml_path, args.output_dir, append=args.append)
    except ValidationError as e:
        print(f"Validation failed:\n{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
