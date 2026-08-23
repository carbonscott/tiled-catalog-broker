"""
Generic manifest generator.

Reads a finalized YAML contract and produces Parquet manifests
(entities.parquet and artifacts.parquet) for Tiled registration.

The output manifests follow the broker standard:
  Entity manifest:  uid, key, <param_1>, <param_2>, ...
  Artifact manifest: uid (= entity uid), type, file, dataset, [index], shape, dtype
                     — a `shared:` axis is a row with uid None: an artifact of the
                     dataset container rather than of an entity.

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
  - group: datasets inside one named HDF5 group (`group: /params`, flat metadata) or
    several (`groups: {instrument: /entry/instrument, ...}`, nested under each name)
  - group_scalars: scalars inside entity groups (grouped layout)

A captured field's scalar HDF5 attributes (units, long_name, ...) ride along as
`<field>_<attr>` labels; an artifact's attributes become its array node's metadata.
Labels are stored, not hashed: the entity UID is content-addressed from the parameter
*values* only. Nested metadata is carried as dotted manifest columns
(`instrument.Ei`) that registration unflattens.

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

from ..utils import ARTIFACT_STANDARD_COLS
from ._models import (
    ArtifactSpec, DatasetConfig, Layout, ParametersSection, ParamLocation, SharedAxisSpec,
)
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

    if cfg.get("extra_metadata"):
        print("WARNING: `extra_metadata` is no longer supported and is ignored. Capture "
              "per-entity scalars with `parameters.groups` (docs/ONBOARDING.md §2).")

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
        config.parameters, existing_uids,
    )

    # Shared axes are artifacts of the *dataset*, not of an entity: one row each in
    # the artifact manifest with no uid. Resolved once against the files.
    shared_rows = _shared_axis_rows(h5_files, root, config.shared)

    # Build DataFrames
    ent_df = pd.DataFrame(ent_rows)
    art_df = pd.DataFrame(art_rows + shared_rows)

    # In append mode, merge with existing manifests. Shared axes are re-resolved
    # every run, so the old manifest's copies are dropped rather than duplicated.
    if append and existing_uids:
        old_ent_path = os.path.join(output_dir, "entities.parquet")
        old_art_path = os.path.join(output_dir, "artifacts.parquet")
        if os.path.exists(old_ent_path) and os.path.exists(old_art_path):
            old_ent = pd.read_parquet(old_ent_path)
            old_art = pd.read_parquet(old_art_path)
            old_art = old_art[old_art["uid"].notna()]
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
        b"artifact_count": str(len(art_rows)).encode(),
        b"shared_count": str(len(shared_rows)).encode(),
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
    print(f"Artifacts: {len(art_df)} rows ({len(shared_rows)} shared axes) -> {art_path}")

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

# HDF5's own bookkeeping attributes (the dimension-scale machinery h5py/netCDF writers
# attach to datasets). They describe the file, not the data, and are never carried along.
_HDF5_INTERNAL_ATTRS = frozenset(
    {"CLASS", "NAME", "REFERENCE_LIST", "DIMENSION_LIST", "DIMENSION_LABELS"}
)
# Artifact-manifest columns the broker itself writes. A dataset attribute named like one
# (`type`, `shape`, ...) would overwrite that column if carried as a bare label, so it is
# dropped with a warning — once per (HDF5 path, attribute), not once per file.
_ARTIFACT_RESERVED = ARTIFACT_STANDARD_COLS | {"file_size", "file_mtime"}
_reserved_warned = set()


def _attrs(obj, label_of=None):
    """HDF5 attributes of ``obj`` as native-typed values (sorted by name).

    Plain (``label_of`` None): every attribute of a file/group — the
    ``root_attributes`` parameters. With ``label_of``: the attributes of the dataset
    holding field ``label_of``, as *labels* of that field — whatever the producer
    attached (units, long_name, description, ...) keyed ``<label_of>_<attr>``
    (``Ei_units``); scalar string/number values only, HDF5-internal attributes
    skipped. ``label_of=""`` gives the bare attribute names — an array node's own
    metadata — minus any that names a manifest column (``type``, ``shape``, ...),
    which is dropped with a warning. Labels are stored beside the value but never
    enter the UID hash.
    """
    out = OrderedDict()
    for name in sorted(obj.attrs.keys()):
        val = _to_python(obj.attrs[name])
        if label_of is None:
            out[name] = val
        elif name in _HDF5_INTERNAL_ATTRS:
            continue
        elif label_of == "" and name in _ARTIFACT_RESERVED:
            if (obj.name, name) not in _reserved_warned:
                _reserved_warned.add((obj.name, name))
                print(f"  WARNING: {obj.name}@{name} is not carried as array metadata — "
                      f"'{name}' is a manifest column. Rename the attribute in the file "
                      f"if you need it (then regenerate, delete the dataset, re-register).")
        elif isinstance(val, (str, int, float)) and not isinstance(val, bool):
            out[f"{label_of}_{name}" if label_of else name] = val
    return out


def _scalar_params(container, params: ParametersSection, default=None, batch_size=None):
    """Read one entity's (or one batched file's) parameters from its parameter group(s).

    ``container`` is the File (per_entity/batched) or the entity Group (grouped).
    The group(s) come from ``parameters.group`` (one unnamed group → flat keys) or
    ``parameters.groups`` (named → ``<name>.<field>`` keys, one level deeper per
    subgroup when ``recursive``); ``default`` is the path used when the YAML names
    none (``"/"`` reads the container itself — the ``root_scalars`` case). Dotted keys
    are flat manifest columns that registration unflattens into nested metadata.
    ``parameters.exclude`` paths (same frame as the group paths) are skipped, datasets
    and subgroups alike.

    Scalar mode (``batch_size`` None): every 0-dim dataset is a parameter; arrays are
    artifacts, not parameters, and are skipped. Batched mode: a 1-D dataset of length
    ``batch_size`` is a per-entity column (entity *i* is row *i*), a 0-dim dataset is
    broadcast to every entity in the file, anything else is skipped.

    Returns ``(params, labels)``: ``labels`` are the fields' attribute labels
    (``<key>_units``, ...) — stored beside the value, excluded from the UID.
    """
    groups = params.groups or {"": params.group or default or "/"}
    exclude = {p.strip("/") for p in params.exclude}
    out, labels = OrderedDict(), OrderedDict()
    for gname, gpath in groups.items():
        gpath = gpath.strip("/")
        if gpath and not (gpath in container and isinstance(container[gpath], h5py.Group)):
            continue
        pending = [(container[gpath] if gpath else container, "")]
        while pending:
            g, prefix = pending.pop(0)
            for name in sorted(g.keys()):
                rel = f"{prefix}/{name}" if prefix else name
                if f"{gpath}/{rel}".strip("/") in exclude:
                    continue
                try:
                    obj = g[name]
                except KeyError as e:  # dangling soft/external link
                    print(f"  WARNING: {g.file.filename}:{g.name}/{name} unreadable ({e}); skipping")
                    continue
                if isinstance(obj, h5py.Group):
                    if params.recursive:
                        pending.append((obj, rel))
                    continue
                if not isinstance(obj, h5py.Dataset):
                    continue
                key = ".".join(p for p in (gname, *rel.split("/")) if p)
                if obj.ndim == 0:
                    val = _to_python(obj[()])
                    out[key] = np.full(batch_size, val) if batch_size else val
                elif batch_size and obj.ndim == 1 and obj.shape[0] == batch_size:
                    out[key] = obj[:]
                else:
                    continue
                labels.update(_attrs(obj, label_of=key))
    return out, labels


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
                  shape, dtype, labels=None):
    """Artifact manifest row (uid matches the parent entity uid).

    `shape` is the shape of the artifact *as registered* — for batched layouts the
    leading (entity) axis is already dropped, so it is what Tiled stores, not what
    the HDF5 dataset reports. It is JSON-encoded so it round-trips through Parquet
    exactly; `dtype` is the numpy dtype string (e.g. "float32"). `labels` are the
    dataset's own scalar HDF5 attributes (``_attrs(ds, label_of="")``); any extra
    column here becomes metadata on the artifact's array node at registration.
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
    row.update(labels or {})
    return row


def _shared_axis_rows(h5_files, root, shared: list[SharedAxisSpec]):
    """One artifact-manifest row per `shared:` axis, with no uid.

    A shared axis is an array that is the same for every entity (an energy grid,
    a field axis), so it is registered once as an array child of the *dataset*
    container rather than under each entity — an artifact whose parent is the
    dataset, hence a row in the artifact manifest with ``uid`` None. Its `dataset`
    is an absolute HDF5 path in *every* layout (not group-relative, even for
    `grouped`). It is recorded from the first file that holds it, and every other
    file that holds it is checked to agree — a differing axis is a contract
    violation (it varies per entity, so it is an artifact of the entity, not a
    shared axis). Shape, dtype and the dataset's own attributes are captured here
    so registration never opens HDF5.
    """
    rows, seen = [], {}  # type -> (file, reference array)
    if not shared:
        return rows
    for h5_path in h5_files:
        rel_path = str(h5_path.relative_to(root))
        with h5py.File(h5_path, "r") as f:
            for ax in shared:
                ds_path = ax.dataset.lstrip("/")
                if ds_path not in f or not isinstance(f[ds_path], h5py.Dataset):
                    continue
                ds = f[ds_path]
                if ax.type in seen:
                    ref_file, ref = seen[ax.type]
                    equal_nan = bool(np.issubdtype(ref.dtype, np.floating))
                    if ds.shape != ref.shape or not np.array_equal(ds[()], ref, equal_nan=equal_nan):
                        raise ValueError(
                            f"shared axis type={ax.type!r} ({ax.dataset}) differs between "
                            f"{ref_file} and {rel_path}. A shared axis must be identical in "
                            f"every file; if it varies per entity, list it under "
                            f"`artifacts:` instead."
                        )
                    continue
                seen[ax.type] = (rel_path, ds[()])
                fsize, fmtime = _file_fingerprint(h5_path)
                rows.append(_artifact_row(
                    None, ax.type, rel_path, ax.dataset, None, fsize, fmtime,
                    ds.shape, str(ds.dtype), labels=_attrs(ds, label_of="")))
    missing = [ax for ax in shared if ax.type not in seen]
    if missing:
        ax = missing[0]
        raise KeyError(
            f"shared axis type={ax.type!r}: dataset {ax.dataset!r} not found in "
            f"any of the {len(h5_files)} HDF5 files under {root}. A shared "
            f"axis is an absolute HDF5 path present in (at least) one file."
        )
    return rows


def _generate_per_entity(h5_files, root, key_prefix, artifacts: list[ArtifactSpec],
                         params: ParametersSection, existing_uids):
    """One HDF5 file = one entity. Scalars at root (or in the parameter groups) are
    parameters."""
    ent_rows = []
    art_rows = []
    content_count = fallback_count = 0

    for i, h5_path in enumerate(h5_files):
        rel_path = str(h5_path.relative_to(root))
        file_stem = h5_path.stem
        labels = {}

        with h5py.File(h5_path, "r") as f:
            # Read the entity's physical parameters first — the UID is a
            # content-addressed hash of these, not of file position.
            if params.location == ParamLocation.root_attributes:
                entity_params = _attrs(f)
            else:  # root_scalars (the file root) or group/groups
                entity_params, labels = _scalar_params(f, params, default="/")

            uid, is_content = _resolve_uid(
                entity_params, key_prefix, f"{key_prefix}_{file_stem}")
            if is_content:
                content_count += 1
            else:
                fallback_count += 1

            if uid in existing_uids:
                continue

            fsize, fmtime = _file_fingerprint(h5_path)
            ent_rows.append(_entity_row(uid, entity_params, extra=labels))

            # Artifact rows — only those whose datasets exist in this file.
            # Shape and dtype are captured here so registration never opens HDF5.
            for art in artifacts:
                if art.dataset.lstrip("/") not in f:
                    continue
                ds = f[art.dataset.lstrip("/")]
                art_rows.append(_artifact_row(
                    uid, art.type, rel_path, art.dataset,
                    None, fsize, fmtime, ds.shape, str(ds.dtype),
                    labels=_attrs(ds, label_of="")))

        if (i + 1) % 1000 == 0:
            print(f"  Processed {i + 1}/{len(h5_files)} entities...")

    _warn_mixed_uid_paths(content_count, fallback_count, key_prefix)
    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Batched layout
# ---------------------------------------------------------------------------

def _generate_batched(h5_files, root, key_prefix, artifacts: list[ArtifactSpec],
                      params: ParametersSection, existing_uids):
    """Multiple entities stacked along axis-0 in each file."""
    ent_rows = []
    art_rows = []
    content_count = fallback_count = 0
    global_idx = 0

    for h5_path in h5_files:
        rel_path = str(h5_path.relative_to(root))
        fsize, fmtime = _file_fingerprint(h5_path)

        with h5py.File(h5_path, "r") as f:
            # Determine batch size from first artifact
            first_art_ds = artifacts[0].dataset.lstrip("/")
            batch_size = f[first_art_ds].shape[0]

            # Per-entity shape/dtype (and the dataset's own attrs), read once per
            # file rather than per entity. Entity i is row i, so the registered
            # shape drops the leading axis.
            art_info = {}
            for art in artifacts:
                ds = f[art.dataset.lstrip("/")]
                art_info[art.type] = (ds.shape[1:], str(ds.dtype), _attrs(ds, label_of=""))

            # Read all parameters at once: one (N,) column per parameter. Root
            # attributes are scalars — the same value for every entity in the batch.
            param_arrays, labels = {}, {}
            root_attr_params = {}
            if params.location == ParamLocation.root_attributes:
                root_attr_params = _attrs(f)
            else:  # root_scalars (the file root) or group/groups
                param_arrays, labels = _scalar_params(
                    f, params, default="/", batch_size=batch_size)

            for i in range(batch_size):
                # Collect the entity's physical parameters FIRST; the UID is
                # a content-addressed hash of these, not of the global index.
                if params.location == ParamLocation.root_attributes:
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

                ent_rows.append(_entity_row(uid, entity_params, extra=labels))

                # Artifact rows — uid matches entity uid
                for art in artifacts:
                    shape, dtype, art_labels = art_info[art.type]
                    art_rows.append(_artifact_row(
                        uid, art.type, rel_path, art.dataset,
                        i, fsize, fmtime, shape, dtype, labels=art_labels))

                global_idx += 1

        print(f"  Processed {h5_path.name}: {batch_size} entities (total: {global_idx})")

    _warn_mixed_uid_paths(content_count, fallback_count, key_prefix)
    return ent_rows, art_rows


# ---------------------------------------------------------------------------
# Grouped layout
# ---------------------------------------------------------------------------

def _generate_grouped(h5_files, root, key_prefix, artifacts: list[ArtifactSpec],
                      params: ParametersSection, existing_uids):
    """One HDF5 group per entity inside a file."""
    ent_rows = []
    art_rows = []
    content_count = fallback_count = 0
    global_idx = 0
    entity_group = params.entity_group or "samples"

    for h5_path in h5_files:
        rel_path = str(h5_path.relative_to(root))
        fsize, fmtime = _file_fingerprint(h5_path)

        with h5py.File(h5_path, "r") as f:
            if entity_group in f and isinstance(f[entity_group], h5py.Group):
                # Only subgroups are entities; a dataset beside them (a NeXus
                # root-level `file_time`, say) is not one. "/" is the root itself.
                base_group = entity_group.rstrip("/")
                group_keys = sorted(k for k in f[entity_group].keys()
                                    if isinstance(f[entity_group][k], h5py.Group))
            else:
                group_keys = [k for k in sorted(f.keys()) if isinstance(f[k], h5py.Group)]
                base_group = ""

            for gkey in group_keys:
                full_group = f"{base_group}/{gkey}" if base_group else gkey
                g = f[full_group]

                # Read parameters first — UID is a content-addressed hash
                # of the params, not of the group's position in the file.
                entity_params, labels = {}, {}
                if params.location == ParamLocation.root_attributes:
                    entity_params = _attrs(f)
                else:  # group_scalars: group/groups relative to the entity group
                    entity_params, labels = _scalar_params(g, params, default="params")
                    if not entity_params and not (params.group or params.groups):
                        # No `params/` subgroup: the entity group's own scalars.
                        entity_params, labels = _scalar_params(g, params, default="/")

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
                    uid, entity_params, extra=labels, source_group=full_group))

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
                        None, fsize, fmtime, ds.shape, str(ds.dtype),
                        labels=_attrs(ds, label_of="")))

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
