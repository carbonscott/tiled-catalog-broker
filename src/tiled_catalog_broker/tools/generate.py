"""
Generic manifest generator.

Reads a finalized YAML contract and produces Parquet manifests
(entities.parquet and artifacts.parquet) for Tiled registration.

The output manifests follow the broker standard:
  Entity manifest:  uid, key, <param_1>, <param_2>, ...
  Artifact manifest: uid (= entity uid), type, file, dataset, [index], shape, dtype

`shape` and `dtype` are read from the HDF5 here, at generate time, so that
registration never has to open the files (ADR-0002 keeps one registration route;
this keeps that route free of HDF5 I/O). For batched layouts `shape` is the
per-entity shape — the leading axis is already dropped.

Handles three layout patterns:
  - per_entity: one HDF5 file per entity, scalars are parameters
  - batched: entities stacked along axis-0 of datasets in each file
  - grouped: one HDF5 group per entity inside a single file

Supports four parameter locations:
  - root_scalars: scalar HDF5 datasets at file root
  - root_attributes: HDF5 root-level file attributes (f.attrs)
  - group: datasets inside a named HDF5 group (e.g., /params)
  - group_scalars: scalars inside entity groups (grouped layout)

Usage:
    tcb generate datasets/edrixs_sbi.yml
    tcb generate datasets/edrixs_sbi.yml --append
"""

import os
import sys
import json
import hashlib
import argparse
import datetime
from pathlib import Path
from collections import OrderedDict

import h5py
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError
from ruamel.yaml import YAML

from ._models import ArtifactSpec, DatasetConfig, Layout, ParametersSection, ParamLocation
from .schema import validate


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
    config = DatasetConfig.model_validate(cfg)
    config_hash = compute_config_hash(yaml_path)

    key_prefix = config.key
    layout = config.data.layout

    if output_dir is None:
        output_dir = os.path.join(
            os.path.dirname(yaml_path) or ".", "manifests", config.label
        )
    os.makedirs(output_dir, exist_ok=True)

    # extra_metadata is not part of the structural model; read it from the raw config.
    extra_meta_cfg = cfg.get("extra_metadata", [])

    # Load existing UIDs for append mode
    existing_uids = set()
    if append:
        existing_ent_path = os.path.join(output_dir, "entities.parquet")
        if os.path.exists(existing_ent_path):
            existing_df = pd.read_parquet(existing_ent_path, columns=["uid"])
            existing_uids = set(existing_df["uid"])
            print(f"  Append mode: {len(existing_uids)} existing entities will be skipped")

    # Find HDF5 files
    root = Path(config.data.directory)
    h5_files = sorted(root.glob(config.data.file_pattern))
    if not h5_files:
        h5_files = sorted(root.rglob(config.data.file_pattern))
    if not h5_files:
        print(
            f"Error: No HDF5 files matching '{config.data.file_pattern}' "
            f"in {config.data.directory}"
        )
        sys.exit(1)
    print(f"Found {len(h5_files)} HDF5 files")

    generators = {
        Layout.per_entity: _generate_per_entity,
        Layout.batched: _generate_batched,
        Layout.grouped: _generate_grouped,
    }
    ent_rows, art_rows = generators[layout](
        h5_files, root, key_prefix, config.artifacts,
        config.parameters, extra_meta_cfg, existing_uids,
    )

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


# ---------------------------------------------------------------------------
# Shared row builders — the per_entity / batched / grouped generators below all
# resolve a UID, then emit one entity row and N artifact rows the same way.
# ---------------------------------------------------------------------------

def _attrs_params(f):
    """All root-level HDF5 attributes as native-typed params (sorted by name)."""
    return {name: _to_python(f.attrs[name]) for name in sorted(f.attrs.keys())}


def _scalar_params(group):
    """All scalar (0-dim) datasets directly under an HDF5 group (sorted by name)."""
    out = {}
    for name in sorted(group.keys()):
        ds = group[name]
        if isinstance(ds, h5py.Dataset) and ds.ndim == 0:
            out[name] = _to_python(ds[()])
    return out


def _resolve_uid(entity_params, key_prefix, fallback):
    """Return (uid, is_content_addressed) for one entity.

    Content-addressed when physical parameters were discovered; otherwise the
    deterministic positional `fallback` string (see _make_uid).
    """
    if entity_params:
        return _make_uid(entity_params, namespace=key_prefix), True
    return _make_uid(fallback), False


def _entity_row(uid, entity_params, extra=None, source_group=None):
    """Entity manifest row in canonical column order:
    uid, [source_group], <params...>, [extra...]."""
    row = OrderedDict()
    row["uid"] = uid
    if source_group is not None:
        row["source_group"] = source_group
    row.update(entity_params)
    if extra:
        row.update(extra)
    return row


def _artifact_row(uid, art_type, rel_path, dataset, index, fsize, fmtime,
                  shape, dtype):
    """Artifact manifest row (uid matches the parent entity uid).

    `shape` is the shape of the artifact *as registered* — for batched layouts the
    leading (entity) axis is already dropped, so it is what Tiled stores, not what
    the HDF5 dataset reports. It is JSON-encoded so it round-trips through Parquet
    exactly; `dtype` is the numpy dtype string (e.g. "float32").
    """
    row = OrderedDict()
    row["uid"] = uid
    row["type"] = art_type
    row["file"] = rel_path
    row["dataset"] = dataset
    row["index"] = index
    row["file_size"] = fsize
    row["file_mtime"] = fmtime
    row["shape"] = json.dumps(list(shape))
    row["dtype"] = dtype
    return row


def _generate_per_entity(h5_files, root, key_prefix, artifacts: list[ArtifactSpec],
                         params: ParametersSection, extra_meta_cfg, existing_uids):
    """One HDF5 file = one entity. Scalars at root are parameters."""
    ent_rows = []
    art_rows = []
    content_count = fallback_count = 0
    loc = params.location

    for i, h5_path in enumerate(h5_files):
        rel_path = str(h5_path.relative_to(root))
        file_stem = h5_path.stem
        entity_params = {}
        extra_meta = {}

        with h5py.File(h5_path, "r") as f:
            # Read the entity's physical parameters first — the UID is a
            # content-addressed hash of these, not of file position.
            if loc == ParamLocation.root_scalars:
                entity_params = _scalar_params(f)
            elif loc == ParamLocation.root_attributes:
                entity_params = _attrs_params(f)
            elif loc == ParamLocation.group:
                group_name = params.group.lstrip("/")
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

            uid, is_content = _resolve_uid(
                entity_params, key_prefix, f"{key_prefix}_{file_stem}")
            if is_content:
                content_count += 1
            else:
                fallback_count += 1

            if uid in existing_uids:
                continue

            fsize, fmtime = _file_fingerprint(h5_path)
            ent_rows.append(_entity_row(uid, entity_params, extra=extra_meta))

            # Artifact rows — only those whose datasets exist in this file.
            # Shape and dtype are captured here so registration never opens HDF5.
            for art in artifacts:
                if art.dataset.lstrip("/") not in f:
                    continue
                ds = f[art.dataset.lstrip("/")]
                art_rows.append(_artifact_row(
                    uid, art.type, rel_path, art.dataset,
                    None, fsize, fmtime, ds.shape, str(ds.dtype)))

        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(h5_files)} entities...")

    _warn_mixed_uid_paths(content_count, fallback_count, key_prefix)
    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Batched layout
# ---------------------------------------------------------------------------

def _generate_batched(h5_files, root, key_prefix, artifacts: list[ArtifactSpec],
                      params: ParametersSection, extra_meta_cfg, existing_uids):
    """Multiple entities stacked along axis-0 in each file."""
    ent_rows = []
    art_rows = []
    content_count = fallback_count = 0
    global_idx = 0
    loc = params.location

    for h5_path in h5_files:
        rel_path = str(h5_path.relative_to(root))
        fsize, fmtime = _file_fingerprint(h5_path)

        with h5py.File(h5_path, "r") as f:
            # Determine batch size from first artifact
            first_art_ds = artifacts[0].dataset.lstrip("/")
            batch_size = f[first_art_ds].shape[0]

            # Per-entity shape/dtype, read once per file rather than per entity.
            # Entity i is row i, so the registered shape drops the leading axis.
            art_info = {}
            for art in artifacts:
                ds = f[art.dataset.lstrip("/")]
                art_info[art.type] = (ds.shape[1:], str(ds.dtype))

            # Read all parameters at once. Attributes are scalars — the same
            # value for every entity in the batch.
            param_arrays = {}
            root_attr_params = {}
            if loc == ParamLocation.group:
                group_name = params.group.lstrip("/")
                if group_name in f:
                    for pname in sorted(f[group_name].keys()):
                        param_arrays[pname] = f[group_name][pname][:]
            elif loc == ParamLocation.root_scalars:
                for ds_name in sorted(f.keys()):
                    ds = f[ds_name]
                    if isinstance(ds, h5py.Dataset) and ds.ndim == 1 and ds.shape[0] == batch_size:
                        param_arrays[ds_name] = ds[:]
            elif loc == ParamLocation.root_attributes:
                root_attr_params = _attrs_params(f)

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
                if loc == ParamLocation.root_attributes:
                    entity_params = dict(root_attr_params)
                else:
                    entity_params = {pname: _to_python(arr[i])
                                     for pname, arr in param_arrays.items()}

                uid, is_content = _resolve_uid(
                    entity_params, key_prefix, f"{key_prefix}_{global_idx:06d}")
                if is_content:
                    content_count += 1
                else:
                    fallback_count += 1

                if uid in existing_uids:
                    global_idx += 1
                    continue

                # Extra metadata (stored per-entity, not part of the UID)
                extra = OrderedDict()
                for ds_path, arr in extra_arrays.items():
                    col_name = ds_path.rsplit("/", 1)[-1]
                    if arr.ndim == 1:
                        extra[col_name] = _to_python(arr[i])
                    elif arr.ndim > 1:
                        extra[col_name] = arr[i].tolist()

                ent_rows.append(_entity_row(uid, entity_params, extra=extra))

                # Artifact rows — uid matches entity uid
                for art in artifacts:
                    shape, dtype = art_info[art.type]
                    art_rows.append(_artifact_row(
                        uid, art.type, rel_path, art.dataset,
                        i, fsize, fmtime, shape, dtype))

                global_idx += 1

        print(f"  Processed {h5_path.name}: {batch_size} entities (total: {global_idx})")

    _warn_mixed_uid_paths(content_count, fallback_count, key_prefix)
    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Grouped layout
# ---------------------------------------------------------------------------

def _generate_grouped(h5_files, root, key_prefix, artifacts: list[ArtifactSpec],
                      params: ParametersSection, extra_meta_cfg, existing_uids):
    """One HDF5 group per entity inside a file."""
    ent_rows = []
    art_rows = []
    content_count = fallback_count = 0
    global_idx = 0
    entity_group = params.entity_group or "samples"
    loc = params.location

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
                if loc == ParamLocation.group_scalars:
                    param_path = (params.group or "params").lstrip("/")
                    if param_path in g and isinstance(g[param_path], h5py.Group):
                        for pname in sorted(g[param_path].keys()):
                            ds = g[param_path][pname]
                            if isinstance(ds, h5py.Dataset):
                                entity_params[pname] = _to_python(ds[()])
                    else:
                        entity_params = _scalar_params(g)
                elif loc == ParamLocation.root_attributes:
                    entity_params = _attrs_params(f)

                uid, is_content = _resolve_uid(
                    entity_params, key_prefix, f"{key_prefix}_{global_idx:06d}")
                if is_content:
                    content_count += 1
                else:
                    fallback_count += 1

                if uid in existing_uids:
                    global_idx += 1
                    continue

                ent_rows.append(_entity_row(
                    uid, entity_params, source_group=full_group))

                # Artifact rows — dataset path is resolved within the entity group
                for art in artifacts:
                    ds_path = art.dataset.lstrip("/")
                    full_ds_path = f"/{full_group}/{ds_path}"
                    if full_ds_path not in f:
                        raise KeyError(
                            f"{h5_path}: artifact type={art.type!r} dataset "
                            f"{full_ds_path!r} not found. In a grouped layout, "
                            f"'dataset' is resolved within each entity group — write "
                            f"it relative to the group (e.g. 'spectrum', not "
                            f"'/samples/sample_000/spectrum')."
                        )
                    ds = f[full_ds_path]
                    art_rows.append(_artifact_row(
                        uid, art.type, rel_path, full_ds_path,
                        None, fsize, fmtime, ds.shape, str(ds.dtype)))

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
