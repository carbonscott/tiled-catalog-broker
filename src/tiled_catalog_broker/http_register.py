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

import numpy as np
import pandas as pd

from .utils import (
    make_artifact_key,
    make_entity_key,
    to_json_safe,
    get_artifact_info,
    ARTIFACT_STANDARD_COLS,
)


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
    from tiled.structures.core import StructureFamily
    from tiled.structures.array import ArrayStructure
    from tiled.structures.data_source import Asset, DataSource, Management

    h5_rel_path = art_row["file"]
    dataset_path = art_row["dataset"]
    uri_base = server_base_dir if server_base_dir else base_dir
    h5_full_path = os.path.join(uri_base, h5_rel_path)

    # Determine index for batched files
    index = None
    if "index" in art_row.index and pd.notna(art_row.get("index")):
        index = int(art_row["index"])

    # Get shape and dtype from HDF5 (cached by dataset path)
    data_shape, _, _, _ = get_artifact_info(base_dir, h5_rel_path, dataset_path, index)
    data_dtype = np.float64

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


def register_dataset_http(client, ent_df, art_df, base_dir, label,
                          dataset_key, dataset_metadata,
                          server_base_dir=None):
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

    Returns:
        bool: True if any entities were registered.
    """
    from tiled.structures.core import StructureFamily

    start_time = time.time()
    ent_count = 0
    art_count = 0
    skip_count = 0

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

    n = len(ent_df)
    print(f"\n--- Registering {label} ({n} entities via HTTP) ---")

    for i, (_, ent_row) in enumerate(ent_df.iterrows()):
        uid = str(ent_row["uid"])
        ent_key = make_entity_key(ent_row, dataset_key)

        # Skip if container already exists
        if ent_key in parent_client:
            skip_count += 1
            continue

        # Build metadata dynamically from ALL manifest columns
        metadata = {}
        for col in ent_df.columns:
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

        # Create container with all metadata under dataset
        ent_container = parent_client.create_container(key=ent_key, metadata=metadata)
        ent_count += 1

        # Register arrays as children (Mode B)
        if artifacts is not None:
            for _, art_row in artifacts.iterrows():
                try:
                    art_key = make_artifact_key(art_row)

                    # Create data source pointing to external HDF5
                    data_source, data_shape, data_dtype = create_data_source(
                        art_row, base_dir=base_dir,
                        server_base_dir=server_base_dir,
                    )

                    # Build artifact metadata dynamically from non-standard columns
                    art_metadata = {
                        "type": art_row["type"],
                        "shape": list(data_shape),
                        "dtype": str(data_dtype),
                    }
                    for col in art_df.columns:
                        if col not in ARTIFACT_STANDARD_COLS:
                            art_metadata[col] = to_json_safe(art_row[col])

                    # Register artifact as child of container
                    ent_container.new(
                        structure_family=StructureFamily.array,
                        data_sources=[data_source],
                        key=art_key,
                        metadata=art_metadata,
                    )
                    art_count += 1

                except Exception as e:
                    print(f"  ERROR registering artifact {art_key}: {e}")

        # Progress update
        if (i + 1) % 5 == 0 or (i + 1) == n:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            print(f"  Progress: {i+1}/{n} entities ({rate:.1f}/sec)")

    elapsed_total = time.time() - start_time
    print(f"\nRegistration complete:")
    print(f"  Entities:     {ent_count}")
    print(f"  Artifacts:    {art_count}")
    print(f"  Skipped:      {skip_count}")
    print(f"  Time:         {elapsed_total:.1f} seconds")

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
