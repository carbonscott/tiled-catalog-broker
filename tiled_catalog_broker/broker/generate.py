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

Usage:
    python -m broker.generate datasets/edrixs_sbi.yml
"""

import os
import sys
import hashlib
from pathlib import Path
from collections import OrderedDict

import h5py
import numpy as np
import pandas as pd
from ruamel.yaml import YAML

from .schema import validate, ValidationError


def load_yaml(yaml_path):
    """Load and validate a dataset YAML config."""
    yaml = YAML()
    with open(yaml_path) as f:
        cfg = yaml.load(f)
    warnings = validate(cfg)
    for w in warnings:
        print(f"  Warning: {w}")
    return cfg


def generate_manifests(yaml_path, output_dir=None):
    """Generate entity and artifact manifests from a YAML config.

    Args:
        yaml_path: Path to the finalized YAML config.
        output_dir: Directory for output Parquet files (default: manifests/<label>/).

    Returns:
        (str, str): Paths to entities.parquet and artifacts.parquet.
    """
    cfg = load_yaml(yaml_path)

    label = cfg["label"]
    key_prefix = cfg.get("key", cfg.get("key_prefix", label))
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
            params_cfg, extra_meta_cfg, cfg,
        )
    elif layout == "batched":
        ent_rows, art_rows = _generate_batched(
            h5_files, root, key_prefix, artifacts_cfg, shared_cfg,
            params_cfg, extra_meta_cfg, cfg,
        )
    elif layout == "grouped":
        ent_rows, art_rows = _generate_grouped(
            h5_files, root, key_prefix, artifacts_cfg, shared_cfg,
            params_cfg, extra_meta_cfg, cfg,
        )
    else:
        print(f"Error: Unknown layout '{layout}'")
        sys.exit(1)

    # Write Parquet
    ent_df = pd.DataFrame(ent_rows)
    art_df = pd.DataFrame(art_rows)

    ent_path = os.path.join(output_dir, "entities.parquet")
    art_path = os.path.join(output_dir, "artifacts.parquet")
    ent_df.to_parquet(ent_path, index=False)
    art_df.to_parquet(art_path, index=False)

    print(f"Entities: {len(ent_df)} rows -> {ent_path}")
    print(f"Artifacts: {len(art_df)} rows -> {art_path}")

    return ent_path, art_path


# ---------------------------------------------------------------------------
# Per-entity layout
# ---------------------------------------------------------------------------

def _generate_per_entity(h5_files, root, key_prefix, artifacts_cfg,
                         shared_cfg, params_cfg, extra_meta_cfg, cfg):
    """One HDF5 file = one entity. Scalars at root are parameters."""
    ent_rows = []
    art_rows = []

    for i, h5_path in enumerate(h5_files):
        rel_path = str(h5_path.relative_to(root))
        file_stem = h5_path.stem
        uid = _make_uid(f"{key_prefix}_{file_stem}")
        entity_key = f"H_{uid[:8]}"

        entity_row = OrderedDict()
        entity_row["uid"] = uid
        entity_row["key"] = entity_key

        with h5py.File(h5_path, "r") as f:
            loc = params_cfg.get("location", "root_scalars")
            if loc == "root_scalars":
                for ds_name in sorted(f.keys()):
                    ds = f[ds_name]
                    if isinstance(ds, h5py.Dataset) and ds.ndim == 0:
                        entity_row[ds_name] = _to_python(ds[()])
            elif loc == "group":
                group_name = params_cfg["group"].lstrip("/")
                if group_name in f:
                    for pname in sorted(f[group_name].keys()):
                        ds = f[group_name][pname]
                        if isinstance(ds, h5py.Dataset):
                            entity_row[pname] = _to_python(ds[()])

            # Extra metadata datasets
            for extra in extra_meta_cfg:
                ds_path = extra["dataset"].lstrip("/")
                if ds_path in f:
                    ds = f[ds_path]
                    if isinstance(ds, h5py.Dataset):
                        if ds.ndim == 0:
                            entity_row[ds_path] = _to_python(ds[()])
                        elif ds.ndim == 1 and ds.size <= 10:
                            entity_row[ds_path] = ds[:].tolist()

        ent_rows.append(entity_row)

        # Artifact rows — uid matches entity uid for groupby in bulk_register
        for art in artifacts_cfg:
            art_row = OrderedDict()
            art_row["uid"] = uid
            art_row["type"] = art["type"]
            art_row["file"] = rel_path
            art_row["dataset"] = art["dataset"]
            art_row["index"] = None
            art_rows.append(art_row)

        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(h5_files)} entities...")

    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Batched layout
# ---------------------------------------------------------------------------

def _generate_batched(h5_files, root, key_prefix, artifacts_cfg,
                      shared_cfg, params_cfg, extra_meta_cfg, cfg):
    """Multiple entities stacked along axis-0 in each file."""
    ent_rows = []
    art_rows = []
    global_idx = 0

    for h5_path in h5_files:
        rel_path = str(h5_path.relative_to(root))

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

            # Read extra metadata arrays
            extra_arrays = {}
            for extra in extra_meta_cfg:
                ds_path = extra["dataset"].lstrip("/")
                if ds_path in f:
                    ds = f[ds_path]
                    if isinstance(ds, h5py.Dataset) and ds.ndim >= 1 and ds.shape[0] == batch_size:
                        extra_arrays[ds_path] = ds[:]

            for i in range(batch_size):
                uid = _make_uid(f"{key_prefix}_{global_idx:06d}")
                entity_key = f"H_{uid[:8]}"

                entity_row = OrderedDict()
                entity_row["uid"] = uid
                entity_row["key"] = entity_key

                for pname, arr in param_arrays.items():
                    entity_row[pname] = _to_python(arr[i])

                # Extra metadata
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
                    art_rows.append(art_row)

                global_idx += 1

        print(f"  Processed {h5_path.name}: {batch_size} entities (total: {global_idx})")

    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Grouped layout
# ---------------------------------------------------------------------------

def _generate_grouped(h5_files, root, key_prefix, artifacts_cfg,
                      shared_cfg, params_cfg, extra_meta_cfg, cfg):
    """One HDF5 group per entity inside a file."""
    ent_rows = []
    art_rows = []
    global_idx = 0

    entity_group = params_cfg.get("entity_group", "samples")

    for h5_path in h5_files:
        rel_path = str(h5_path.relative_to(root))

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

                uid = _make_uid(f"{key_prefix}_{global_idx:06d}")
                entity_key = f"H_{uid[:8]}"

                entity_row = OrderedDict()
                entity_row["uid"] = uid
                entity_row["key"] = entity_key
                entity_row["source_group"] = full_group

                # Read parameters from within the group
                loc = params_cfg.get("location", "group_scalars")
                if loc == "group_scalars":
                    param_group = params_cfg.get("group", "params")
                    param_path = param_group.lstrip("/")
                    if param_path in g and isinstance(g[param_path], h5py.Group):
                        for pname in sorted(g[param_path].keys()):
                            ds = g[param_path][pname]
                            if isinstance(ds, h5py.Dataset):
                                entity_row[pname] = _to_python(ds[()])
                    else:
                        for ds_name in sorted(g.keys()):
                            ds = g[ds_name]
                            if isinstance(ds, h5py.Dataset) and ds.ndim == 0:
                                entity_row[ds_name] = _to_python(ds[()])

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
                    art_rows.append(art_row)

                global_idx += 1

        print(f"  Processed {h5_path.name}: {len(group_keys)} entity groups (total: {global_idx})")

    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_uid(key_str):
    """Generate a deterministic UID from a key string."""
    return hashlib.sha256(key_str.encode()).hexdigest()[:16]


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
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate Parquet manifests from a dataset YAML contract."
    )
    parser.add_argument("yaml_path", help="Path to the finalized dataset YAML config")
    parser.add_argument("--output-dir", "-o", help="Output directory for manifests")
    args = parser.parse_args()

    try:
        generate_manifests(args.yaml_path, args.output_dir)
    except ValidationError as e:
        print(f"Validation failed:\n{e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
