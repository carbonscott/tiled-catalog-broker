"""
HTTP Registration via Tiled Client.

Registers entities with BOTH:
- Artifact locators in container metadata (for expert path-based access)
- Array children via DataSource adapters (for visualization/chunked access)

Dataset-agnostic: reads all metadata columns dynamically from manifests.
The manifest is the contract -- no hardcoded parameter names or artifact types.

This is the single registration route (ADR-0002), with two transports for the
artifact bytes:

- external (default): each artifact is a DataSource pointing at an HDF5 file
  the server reads from its own filesystem. Shape and dtype come from the
  manifest (captured by `tcb generate`), so this path never opens HDF5.
- upload (`upload=True`): each artifact's array is read from the *local* HDF5
  file and written through the server into its writable storage. For data the
  server cannot see on disk (e.g. remote users registering their own data).

Shared axes (the YAML's ``shared:`` block, captured by ``tcb generate`` as
artifact rows with no ``uid``) are registered once as array children of the
*dataset* container, through the same two transports and the same code path
as entity artifacts — so ``ds["eloss"][:]`` reads over HTTP whether the
server holds a pointer or the bytes.

Entity metadata is built from every entity-manifest column; a dotted column
(``instrument.Ei`` — from ``parameters.groups``) is nested on the way in, so
it is queryable as ``Key("instrument.Ei")``.

Registration is incremental in both modes: an entity (or shared axis) already
present on the server is skipped, so a re-run resumes rather than duplicates. An
entity a crashed run left without some of its artifacts gets the missing ones
registered into its existing container (with a WARNING); an artifact that exists
is never rewritten, so a changed path means delete + re-register.
Per-entity work runs on a ThreadPoolExecutor (see _DEFAULT_MAX_WORKERS).
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import h5py
import numpy as np
import pandas as pd
from dask.array.core import normalize_chunks
from tiled.client.utils import ClientError
from tiled.structures.array import ArrayStructure, BuiltinDtype
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management

from .utils import (
    make_artifact_key,
    make_entity_key,
    to_json_safe,
    ARTIFACT_STANDARD_COLS,
    STORAGE_KEY,
    STORAGE_EXTERNAL,
    STORAGE_UPLOADED,
)

# Default size of the per-entity ThreadPoolExecutor.  Each entity does
# one create_container HTTP call plus one .new() per artifact; the
# baseline profile showed ~80% of wall-clock in socket.recv across
# sequential httpx requests, so parallelizing per-entity work is the
# highest-leverage fix.  Tiled uses httpx, which is thread-safe.
# Callers can override via the max_workers kwarg on register_dataset_http.
_DEFAULT_MAX_WORKERS = 8

# Metadata keys propagated from the dataset YAML down to every entity
# and artifact node, so per-node consumers can read the flag without
# walking the hierarchy. Manifest columns of the same name win via
# setdefault.
INHERITED_KEYS = ("amsc_public",)


def require_shape_dtype(art_df):
    """Require the shape/dtype columns registration reads.

    Registration takes both from the manifest rather than opening HDF5, so a
    manifest lacking them cannot be registered.
    """
    missing = [c for c in ("shape", "dtype") if c not in art_df.columns]
    if missing:
        raise ValueError(
            f"artifact manifest is missing {', '.join(missing)}. "
            "Run `tcb generate <dataset>.yml` to rebuild the manifests, "
            "then register again."
        )


def create_data_source(art_row, base_dir, server_base_dir=None):
    """Create a Tiled DataSource for an artifact pointing to external HDF5.

    Everything comes from the manifest row — path, shape, and dtype. The HDF5
    file is referenced, never opened.

    Args:
        art_row: DataFrame row with artifact manifest columns. Must carry
            `shape` and `dtype`, written by `tcb generate`.
        base_dir: Base directory for resolving relative file paths on the
            authoring host (used by `tcb generate` and Mode A reads).
        server_base_dir: Optional server-side mount path. If set, becomes
            the asset `data_uri` base — needed when the Tiled server sees
            the filesystem at a different mount than the authoring host
            (K8s pod, reverse proxy). Set by the author in the YAML's
            `data.server_base_dir:` field when host and server mounts differ.

    Returns:
        The DataSource. Shape and dtype are read from the manifest row, so
        callers that need them read the same columns rather than taking them
        back out of here.
    """
    h5_rel_path = art_row["file"]
    dataset_path = art_row["dataset"]
    uri_base = server_base_dir if server_base_dir else base_dir
    h5_full_path = os.path.join(uri_base, h5_rel_path)

    # Determine index for batched files
    index = None
    if "index" in art_row.index and pd.notna(art_row.get("index")):
        index = int(art_row["index"])

    # Shape and dtype are captured at generate time — registration never opens
    # HDF5. For batched artifacts the manifest already stores the per-entity
    # shape (leading axis dropped), so no index adjustment is needed here.
    data_shape = json.loads(art_row["shape"])
    data_dtype = np.dtype(art_row["dtype"])

    # Create asset pointing to HDF5 file
    asset = Asset(
        data_uri=f"file://localhost{h5_full_path}",
        is_directory=False,
        parameter="data_uris",
    )

    # Built from shape/dtype directly: ArrayStructure.from_array needs a real
    # array, which would mean allocating one full-size buffer per artifact
    # purely to describe it. Chunking is dask's "auto", the same policy
    # from_array applies.
    structure = ArrayStructure(
        data_type=BuiltinDtype.from_numpy_dtype(data_dtype),
        shape=tuple(data_shape),
        chunks=normalize_chunks(
            ("auto",) * len(data_shape), shape=tuple(data_shape), dtype=data_dtype
        ),
    )

    # Build parameters
    ds_params = {"dataset": dataset_path}
    if index is not None:
        ds_params["slice"] = str(int(index))

    # Create data source. Default dispatches to the broker's
    # LazyHDF5ArrayAdapter (server config must map it) — reads only the
    # bytes a user slice asks for, unlike stock application/x-hdf5 which
    # pulls the whole dataset into dask before slicing.
    data_source = DataSource(
        mimetype=(
            to_json_safe(art_row["mimetype"])
            if "mimetype" in art_row.index
            and pd.notna(art_row.get("mimetype"))
            else "application/x-hdf5-broker"
        ),
        assets=[asset],
        structure_family=StructureFamily.array,
        structure=structure,
        parameters=ds_params,
        management=Management.external,
    )

    return data_source


def read_artifact_array(art_row, base_dir):
    """Read one artifact's array from a local HDF5 file (upload mode).

    For batched layouts the manifest ``index`` selects the entity's row, so
    the returned array matches the per-entity ``shape`` the manifest records
    (leading axis already dropped).
    """
    path = os.path.join(base_dir, art_row["file"])
    with h5py.File(path, "r") as f:
        ds = f[art_row["dataset"]]
        if "index" in art_row.index and pd.notna(art_row.get("index")):
            return ds[int(art_row["index"])]
        return ds[()]


def _register_one_entity(ent_row, ent_columns, art_grouped, art_columns,
                         parent_client, base_dir, server_base_dir,
                         dataset_key, inherited=None, upload=False):
    """Register a single entity container and its artifact children.

    Designed to be called from worker threads. Returns counter deltas so
    the main thread can aggregate across futures without shared state.

    Returns:
        (ent_added, art_added, skipped, art_failed) — exactly one of
        ent_added or skipped is 1; art_added counts artifacts successfully
        registered (for a skipped entity: the ones a crashed earlier run left
        missing), art_failed counts artifacts that raised during register.
    """
    if inherited is None:
        inherited = {}

    uid = str(ent_row["uid"])
    ent_key = make_entity_key(ent_row, dataset_key)
    art_rows = ([row for _, row in art_grouped.get_group(uid).iterrows()]
                if uid in art_grouped.groups else [])

    if ent_key in parent_client:
        # Incremental: an entity already on the server is skipped — unless an
        # earlier run died before all of its artifacts attached. A bare skip
        # would then report a complete dataset that serves nothing for this
        # entity, so only the missing artifacts are registered into the
        # existing container. (A *changed* path is not detected here: an
        # existing artifact is never rewritten — delete and re-register.)
        ent_container = parent_client[ent_key]
        present = set(ent_container.keys())
        art_rows = [row for row in art_rows if make_artifact_key(row) not in present]
        if not art_rows:
            return (0, 0, 1, 0)
        print(f"  WARNING ent={ent_key} half-registered: {len(present)} of "
              f"{len(present) + len(art_rows)} artifacts on the server; "
              f"registering the {len(art_rows)} missing")
        ent_added = 0
    else:
        # Build metadata dynamically from ALL manifest columns. A dotted column
        # (`instrument.Ei`, from parameters.groups) is nested: metadata["instrument"]["Ei"].
        metadata = {}
        for col in ent_columns:
            *parents, leaf = col.split(".")
            node = metadata
            for p in parents:
                node = node.setdefault(p, {})
            node[leaf] = to_json_safe(ent_row[col])

        # Attach artifact locators to metadata (for Mode A access)
        for art_row in art_rows:
            art_key = make_artifact_key(art_row)
            metadata[f"path_{art_key}"] = art_row["file"]
            metadata[f"dataset_{art_key}"] = art_row["dataset"]
            if "index" in art_row.index and pd.notna(art_row.get("index")):
                metadata[f"index_{art_key}"] = int(art_row["index"])

        for k, v in inherited.items():
            metadata.setdefault(k, v)

        # Create container with all metadata under dataset. A 409 here is
        # expected under the tiled client's transparent retry: if the first
        # POST commits server-side but its response is lost (transient 500,
        # e.g. SQLite "database is locked" or the documented remote
        # 500->retry->409 race), the retry collides with the row the first
        # attempt created. Giving up at that point is what used to leave
        # structure-less entities (container without artifact children) —
        # instead, resume: fetch the container that exists and register its
        # artifacts into it.
        try:
            ent_container = parent_client.create_container(
                key=ent_key, metadata=metadata,
            )
        except ClientError as e:
            if e.response.status_code != 409:
                raise
            ent_container = parent_client[ent_key]
        ent_added = 1

    art_added = 0
    art_failed = 0
    for art_row in art_rows:
        try:
            _register_artifact(ent_container, art_row, art_columns, base_dir,
                               server_base_dir, inherited, upload)
            art_added += 1
        except ClientError as e:
            if e.response.status_code == 409:
                # Same retry race as the container: the artifact landed on
                # the lost-response attempt; the retry collided.
                art_added += 1
            else:
                art_failed += 1
                print(f"  ERROR ent={ent_key} art={make_artifact_key(art_row)}: {e}")
        except Exception as e:
            art_failed += 1
            print(f"  ERROR ent={ent_key} art={make_artifact_key(art_row)}: {e}")

    return (ent_added, art_added, 1 - ent_added, art_failed)


def _register_artifact(container, art_row, art_columns, base_dir, server_base_dir,
                       inherited, upload, extra=None):
    """Register one artifact-manifest row as an array child of ``container``.

    The one place an array node is created: under an entity container for a
    per-entity artifact, under the dataset container for a shared axis (a row
    with no uid). Node metadata is ``type``/``shape``/``dtype`` plus every
    non-standard manifest column (the dataset's own HDF5 attributes — units,
    long_name, ... — skipping nulls), ``extra``, and the inherited keys. The
    transport is a DataSource pointing at the file, or with ``upload`` the
    array read locally and written through the server. Raises on failure;
    the caller counts.
    """
    art_key = make_artifact_key(art_row)
    data_shape = json.loads(art_row["shape"])
    data_dtype = np.dtype(art_row["dtype"])

    art_metadata = {
        "type": art_row["type"],
        "shape": list(data_shape),
        "dtype": str(data_dtype),
    }
    for col in art_columns:
        if col not in ARTIFACT_STANDARD_COLS:
            val = to_json_safe(art_row[col])
            if val is not None:
                art_metadata[col] = val
    art_metadata.update(extra or {})
    for k, v in inherited.items():
        art_metadata.setdefault(k, v)

    if upload:
        data = read_artifact_array(art_row, base_dir)
        if list(data.shape) != list(data_shape) or data.dtype != data_dtype:
            raise ValueError(
                f"manifest records shape={data_shape} dtype={data_dtype} "
                f"but the file holds shape={list(data.shape)} "
                f"dtype={data.dtype}; re-run `tcb generate`"
            )
        container.write_array(data, key=art_key, metadata=art_metadata)
    else:
        data_source = create_data_source(
            art_row, base_dir=base_dir, server_base_dir=server_base_dir,
        )
        container.new(
            structure_family=StructureFamily.array,
            data_sources=[data_source],
            key=art_key,
            metadata=art_metadata,
        )


def register_dataset_http(client, ent_df, art_df, base_dir, label,
                          dataset_key, dataset_metadata,
                          server_base_dir=None,
                          max_workers=_DEFAULT_MAX_WORKERS,
                          upload=False):
    """Register one dataset via HTTP through a running Tiled server.

    Creates a dataset container, its shared axes (artifact rows with no uid)
    as array children, then entity containers with locator metadata (Mode A)
    and array children (Mode B).

    Args:
        client: Tiled client connected to a running server.
        ent_df: Entity manifest DataFrame.
        art_df: Artifact manifest DataFrame. Rows with a null ``uid`` are the
            dataset's shared axes and are registered once under the dataset
            container, independent of which entities this run registers.
        base_dir: Base directory for resolving relative file paths (local).
        label: Dataset name (for logging).
        dataset_key: Key for the dataset container (e.g. "VDP").
        dataset_metadata: Metadata dict for the dataset container.
        server_base_dir: If provided, used for asset data_uri instead of
            base_dir.  Needed when the server sees the filesystem at a
            different mount point. Ignored in upload mode.
        max_workers: Size of the ThreadPoolExecutor for per-entity work.
            Defaults to ``_DEFAULT_MAX_WORKERS``; tune for server capacity.
        upload: If True, read each artifact's array from local HDF5 and
            write it through the server into its writable storage, instead
            of registering a DataSource pointing at the file. The dataset
            container is stamped ``storage: uploaded`` (vs ``external``);
            a dataset cannot mix the two.

    Returns:
        bool: True if any entities or shared axes were registered.
    """
    start_time = time.time()
    ent_count = 0
    art_count = 0
    skip_count = 0
    art_fail_count = 0

    storage = STORAGE_UPLOADED if upload else STORAGE_EXTERNAL

    # Create or reuse dataset container
    if dataset_key in client:
        parent_client = client[dataset_key]
        # Incremental re-runs must keep one transport per dataset: mixing
        # external pointers and uploaded arrays would make delete semantics
        # (which bytes does the server own?) ambiguous per-entity.
        existing = dict(parent_client.metadata).get(STORAGE_KEY, STORAGE_EXTERNAL)
        if existing != storage:
            raise ValueError(
                f"dataset '{dataset_key}' exists with storage='{existing}' but "
                f"this run would register storage='{storage}'. Delete the "
                f"dataset first (tcb delete {dataset_key}) or use a new label."
            )
        print(f"Using existing dataset container '{dataset_key}'")
    else:
        parent_client = client.create_container(
            key=dataset_key,
            metadata={**dataset_metadata, STORAGE_KEY: storage},
        )
        print(f"Created dataset container '{dataset_key}' (storage: {storage})")

    require_shape_dtype(art_df)
    ent_columns = list(ent_df.columns)
    art_columns = list(art_df.columns)
    inherited = {k: dataset_metadata[k] for k in INHERITED_KEYS if k in dataset_metadata}

    # Shared axes first: the artifact rows with no uid, one array node each
    # directly under the dataset container, same transport as the entity
    # artifacts. Independent of which entities this run registers (-n);
    # an axis whose key already exists is skipped.
    shared_added = shared_skipped = shared_failed = 0
    shared_rows = art_df[art_df["uid"].isna()]
    for _, row in shared_rows.iterrows():
        key = make_artifact_key(row)
        if key in parent_client:
            shared_skipped += 1
            continue
        try:
            _register_artifact(
                parent_client, row, art_columns, base_dir, server_base_dir,
                inherited, upload,
                extra={"shared_axis": True, "path": row["file"], "dataset": row["dataset"]},
            )
            shared_added += 1
            print(f"  Registered shared axis '{key}' shape={row['shape']} dtype={row['dtype']}")
        except Exception as e:
            shared_failed += 1
            print(f"  ERROR shared axis '{key}': {e}")

    # Pre-group entity artifacts by uid for O(1) lookup (groupby drops the
    # null-uid shared rows).
    print("Pre-grouping artifacts by uid...")
    art_grouped = art_df.groupby("uid")

    n = len(ent_df)
    mode = "upload" if upload else "external"
    print(f"\n--- Registering {label} ({n} entities via HTTP, {mode}, "
          f"pool={max_workers}) ---")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _register_one_entity,
                ent_row, ent_columns, art_grouped, art_columns,
                parent_client, base_dir, server_base_dir, dataset_key,
                inherited, upload,
            )
            for _, ent_row in ent_df.iterrows()
        ]

        for i, future in enumerate(as_completed(futures)):
            ent_added, art_added, skipped, art_failed = future.result()
            ent_count += ent_added
            art_count += art_added
            skip_count += skipped
            art_fail_count += art_failed

            if (i + 1) % 5 == 0 or (i + 1) == n:
                elapsed = time.time() - start_time
                # Rate counts only entities that did real work; skipped
                # futures finish in microseconds and would inflate it.
                rate = ent_count / elapsed if elapsed > 0 else 0
                print(f"  Progress: {i+1}/{n} entities ({rate:.1f}/sec)")

    elapsed_total = time.time() - start_time
    print(f"\nRegistration complete:")
    print(f"  Entities:        {ent_count}")
    print(f"  Artifacts:       {art_count}")
    print(f"  Skipped:         {skip_count}")
    print(f"  Artifact errors: {art_fail_count}")
    if len(shared_rows):
        print(f"  Shared axes:     {shared_added} registered, "
              f"{shared_skipped} skipped, {shared_failed} errors")
    print(f"  Time:            {elapsed_total:.1f} seconds")

    return ent_count > 0 or art_count > 0 or shared_added > 0


def verify_registration_http(client):
    """Smoke-probe registration via Tiled client.

    Samples the first dataset, first entity under it, and first artifact
    under that — root -> dataset -> entity -> artifact — and reports
    dual-mode access status (metadata locators + array children) on the
    sampled entity. Not a full per-row verification; the goal is to catch
    structural breakage (empty container, wrong hierarchy depth) post-
    register without iterating the whole catalog.
    """
    print("\n" + "=" * 50)
    print("Verification")
    print("=" * 50)

    dataset_keys = list(client.keys())
    print(f"Dataset containers at root: {len(dataset_keys)}")
    if not dataset_keys:
        print("No datasets registered yet.")
        return
    print(f"  {dataset_keys[:5]}")

    ds_key = dataset_keys[0]
    ds = client[ds_key]
    ds_meta = dict(ds.metadata)
    print(f"\nDataset '{ds_key}':")
    print(f"  metadata keys: {len(ds_meta)}")
    if ds_meta:
        print(f"    sample: {sorted(ds_meta.keys())[:8]}")

    # Shared axes are array children of the dataset container, keyed by
    # their type and announced in its metadata; everything else is an entity.
    shared_keys = {k.removeprefix("shared_dataset_")
                   for k in ds_meta if k.startswith("shared_dataset_")}
    all_keys = list(ds.keys())
    ent_keys = [k for k in all_keys if k not in shared_keys]
    found_shared = [k for k in all_keys if k in shared_keys]
    if shared_keys:
        print(f"  shared axes: {len(found_shared)}/{len(shared_keys)} registered "
              f"as array children {sorted(found_shared)}")
    print(f"  entity containers: {len(ent_keys)}")
    if not ent_keys:
        return
    print(f"    sample: {ent_keys[:3]}")

    ent_key = ent_keys[0]
    ent = ds[ent_key]
    ent_meta = dict(ent.metadata)
    path_keys = [k for k in ent_meta if k.startswith("path_")]
    print(f"\nEntity '{ent_key}':")
    print(f"  metadata keys: {len(ent_meta)} (locators: {len(path_keys)})")

    art_keys = list(ent.keys()) if hasattr(ent, "keys") else []
    print(f"  artifact children: {len(art_keys)}")
    if not art_keys:
        print("\n  WARNING: no array children — Mode B access will fail.")
        return
    print(f"    sample: {art_keys[:5]}")

    art = ent[art_keys[0]]
    shape = getattr(art, "shape", None)
    dtype = getattr(art, "dtype", None)
    if shape is not None:
        print(f"\nArtifact '{art_keys[0]}':")
        print(f"  shape: {shape}  dtype: {dtype}")
