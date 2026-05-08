"""
HTTP Registration via Tiled Client.

Registers entities with BOTH:
- Artifact locators in container metadata (for expert path-based access)
- Array children via DataSource adapters (for visualization/chunked access)

Dataset-agnostic: reads all metadata columns dynamically from manifests.
The manifest is the contract -- no hardcoded parameter names or artifact types.

When to use:
- Incremental updates to a running server
- Adding new datasets alongside existing ones
- Server is running and serving queries

When NOT to use:
- Initial bulk load of 1K+ entities (use bulk_register.py / ingest.py)
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from tiled.structures.array import ArrayStructure
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management

from .utils import (
    make_artifact_key,
    make_entity_key,
    to_json_safe,
    get_artifact_info,
    ARTIFACT_STANDARD_COLS,
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


def create_data_source(art_row, base_dir, server_base_dir=None):
    """Create a Tiled DataSource for an artifact pointing to external HDF5.

    Reads dataset path and shape from the manifest and HDF5 file directly.

    Args:
        art_row: DataFrame row with artifact manifest columns.
        base_dir: Base directory for resolving relative file paths on the
            authoring host (used by `tcb generate` and Mode A reads).
        server_base_dir: Optional server-side mount path. If set, becomes
            the asset `data_uri` base — needed when the Tiled server sees
            the filesystem at a different mount than the authoring host
            (K8s pod, reverse proxy). Pre-computed by `tcb inspect` from
            `TILED_HOST_DATA_ROOT` / `TILED_SERVER_DATA_ROOT` env vars
            and persisted in the YAML's `data.server_base_dir:` field.

    Returns:
        Tuple of (DataSource, data_shape, data_dtype).
    """
    h5_rel_path = art_row["file"]
    dataset_path = art_row["dataset"]
    uri_base = server_base_dir if server_base_dir else base_dir
    h5_full_path = os.path.join(uri_base, h5_rel_path)

    # Determine index for batched files
    index = None
    if "index" in art_row.index and pd.notna(art_row.get("index")):
        index = int(art_row["index"])

    # Get shape and dtype from HDF5 (cached by dataset path)
    data_shape, dtype_str, _, _ = get_artifact_info(base_dir, h5_rel_path, dataset_path, index)
    data_dtype = np.dtype(dtype_str)

    # Create asset pointing to HDF5 file
    asset = Asset(
        data_uri=f"file://localhost{h5_full_path}",
        is_directory=False,
        parameter="data_uris",
    )

    # Create array structure
    structure = ArrayStructure.from_array(
        np.empty(data_shape, dtype=data_dtype)
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

    return data_source, data_shape, data_dtype


def _register_one_entity(ent_row, ent_columns, art_grouped, art_columns,
                         parent_client, base_dir, server_base_dir,
                         dataset_key, inherited=None):
    """Register a single entity container and its artifact children.

    Designed to be called from worker threads. Returns counter deltas so
    the main thread can aggregate across futures without shared state.

    Returns:
        (ent_added, art_added, skipped, art_failed) — exactly one of
        ent_added or skipped is 1; art_added counts artifacts successfully
        registered, art_failed counts artifacts that raised during register.
    """
    if inherited is None:
        inherited = {}

    uid = str(ent_row["uid"])
    ent_key = make_entity_key(ent_row, dataset_key)

    # Skip if container already exists
    if ent_key in parent_client:
        return (0, 0, 1, 0)

    # Build metadata dynamically from ALL manifest columns
    metadata = {}
    for col in ent_columns:
        metadata[col] = to_json_safe(ent_row[col])

    # Attach artifact locators to metadata (for Mode A access)
    artifacts = None
    if uid in art_grouped.groups:
        artifacts = art_grouped.get_group(uid)
        for _, art_row in artifacts.iterrows():
            art_key = make_artifact_key(art_row)
            metadata[f"path_{art_key}"] = art_row["file"]
            metadata[f"dataset_{art_key}"] = art_row["dataset"]
            if "index" in art_row.index and pd.notna(art_row.get("index")):
                metadata[f"index_{art_key}"] = int(art_row["index"])

    for k, v in inherited.items():
        metadata.setdefault(k, v)

    # Create container with all metadata under dataset
    ent_container = parent_client.create_container(
        key=ent_key, metadata=metadata,
    )

    art_added = 0
    art_failed = 0
    if artifacts is not None:
        for _, art_row in artifacts.iterrows():
            art_key = make_artifact_key(art_row)
            try:
                data_source, data_shape, data_dtype = create_data_source(
                    art_row, base_dir=base_dir,
                    server_base_dir=server_base_dir,
                )

                art_metadata = {
                    "type": art_row["type"],
                    "shape": list(data_shape),
                    "dtype": str(data_dtype),
                }
                for col in art_columns:
                    if col not in ARTIFACT_STANDARD_COLS:
                        art_metadata[col] = to_json_safe(art_row[col])

                for k, v in inherited.items():
                    art_metadata.setdefault(k, v)

                ent_container.new(
                    structure_family=StructureFamily.array,
                    data_sources=[data_source],
                    key=art_key,
                    metadata=art_metadata,
                )
                art_added += 1

            except Exception as e:
                art_failed += 1
                print(f"  ERROR ent={ent_key} art={art_key}: {e}")

    return (1, art_added, 0, art_failed)


def _register_shared_arrays(parent_client, dataset_metadata, art_df,
                            base_dir, server_base_dir):
    """Register `shared:` axes as Tiled array children of the dataset container.

    Reads the dataset-level locator strings (``shared_dataset_<type>:
    <h5_path>``) that ``_build_dataset_metadata`` already wrote into
    ``dataset_metadata`` and creates one Tiled array node per entry under
    the dataset container — so HTTP-only consumers can fetch the shared
    axis via ``client[dataset_key][type].read()`` instead of having to
    open the source HDF5 file directly.

    The asset URI is built the same way as per-entity artifacts. The
    HDF5 file path is taken from the first artifact row (works for any
    layout where shared axes live in any of the registered files; for
    grouped layout that's the single file).

    Pre-existing array nodes with the same key are left alone — re-runs
    of register are safe.
    """
    shared = {k.removeprefix("shared_dataset_"): v
              for k, v in dataset_metadata.items()
              if k.startswith("shared_dataset_") and isinstance(v, str)}
    if not shared or len(art_df) == 0:
        return

    sample_file = art_df["file"].iloc[0]
    existing_keys = set(parent_client)
    inherited = {k: dataset_metadata[k] for k in INHERITED_KEYS
                 if k in dataset_metadata}

    for shared_type, h5_path in shared.items():
        if shared_type in existing_keys:
            continue
        synth_row = pd.Series({
            "file": sample_file,
            "dataset": h5_path,
            "type": shared_type,
        })
        try:
            data_source, shape, dtype = create_data_source(
                synth_row, base_dir=base_dir,
                server_base_dir=server_base_dir,
            )
            metadata = {
                "type": shared_type,
                "shape": list(shape),
                "dtype": str(dtype),
                "shared_axis": True,
                "source_dataset_path": h5_path,
            }
            for k, v in inherited.items():
                metadata.setdefault(k, v)
            parent_client.new(
                structure_family=StructureFamily.array,
                data_sources=[data_source],
                key=shared_type,
                metadata=metadata,
            )
            print(f"  Registered shared array '{shared_type}' "
                  f"shape={tuple(shape)} dtype={dtype}")
        except Exception as e:
            print(f"  WARNING: failed to register shared array "
                  f"'{shared_type}' (path={h5_path}): {e}")


def register_dataset_http(client, ent_df, art_df, base_dir, label,
                          dataset_key, dataset_metadata,
                          server_base_dir=None,
                          max_workers=_DEFAULT_MAX_WORKERS):
    """Register one dataset via HTTP through a running Tiled server.

    Creates a dataset container, then entity containers with locator
    metadata (Mode A) and array children via DataSource adapters (Mode B).

    Args:
        client: Tiled client connected to a running server.
        ent_df: Entity manifest DataFrame.
        art_df: Artifact manifest DataFrame.
        base_dir: Base directory for resolving relative file paths (local).
        label: Dataset name (for logging).
        dataset_key: Key for the dataset container (e.g. "VDP").
        dataset_metadata: Metadata dict for the dataset container.
        server_base_dir: If provided, used for asset data_uri instead of
            base_dir.  Needed when the server sees the filesystem at a
            different mount point.
        max_workers: Size of the ThreadPoolExecutor for per-entity work.
            Defaults to ``_DEFAULT_MAX_WORKERS``; tune for server capacity.

    Returns:
        bool: True if any entities were registered.
    """
    start_time = time.time()
    ent_count = 0
    art_count = 0
    skip_count = 0
    art_fail_count = 0

    # Create or reuse dataset container
    if dataset_key in client:
        parent_client = client[dataset_key]
        print(f"Using existing dataset container '{dataset_key}'")
    else:
        parent_client = client.create_container(
            key=dataset_key,
            metadata=dataset_metadata,
        )
        print(f"Created dataset container '{dataset_key}'")

    # Pre-group artifacts by uid for O(1) lookup
    print("Pre-grouping artifacts by uid...")
    art_grouped = art_df.groupby("uid")

    # Register shared axes (entries from the YAML's `shared:` block) as
    # actual Tiled array children of the dataset container, in addition to
    # the `shared_dataset_<type>` locator metadata that's already on the
    # container. Lets pure-HTTP consumers fetch them via Mode B without
    # filesystem access, and saves the per-entity duplication.
    _register_shared_arrays(parent_client, dataset_metadata, art_df,
                            base_dir, server_base_dir)

    n = len(ent_df)
    print(f"\n--- Registering {label} ({n} entities via HTTP, "
          f"pool={max_workers}) ---")

    ent_columns = list(ent_df.columns)
    art_columns = list(art_df.columns)

    inherited = {k: dataset_metadata[k] for k in INHERITED_KEYS if k in dataset_metadata}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _register_one_entity,
                ent_row, ent_columns, art_grouped, art_columns,
                parent_client, base_dir, server_base_dir, dataset_key,
                inherited,
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
    print(f"  Time:            {elapsed_total:.1f} seconds")

    return ent_count > 0


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

    ent_keys = list(ds.keys())
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
