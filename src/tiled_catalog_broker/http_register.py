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
from tiled.structures.array import ArrayStructure
from tiled.structures.core import StructureFamily
from tiled.structures.data_source import Asset, DataSource, Management

from .utils import (
    ARTIFACT_STANDARD_COLS,
    get_artifact_info,
    make_artifact_key,
    make_entity_key,
    split_constant_cols,
    to_json_safe,
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


def register_dataset_manifest_layout(
    client, cfg, ent_df, art_df, base_dir, label,
    dataset_key, dataset_metadata,
    server_base_dir=None,
):
    """Register a dataset whose YAML declares ``layout: manifest``.

    Interprets the producer's two parquets in place:
      - Provenance tiering: columns with a single unique value in either
        manifest are promoted to dataset-container metadata.
      - Column renames: ``entity_uid_column`` -> ``uid`` in both frames,
        ``artifact_uid_column`` -> ``auid``, ``file_column`` -> ``file``.
      - Fan-out: each producer artifact row expands to one broker row per
        HDF5 array path in ``cfg['artifact_datasets'][row.type]``, each
        carrying ``dataset`` and ``array_name`` so make_artifact_key can
        synthesize ``{array_name}_{auid[:8]}``.

    Then delegates the actual registration to ``register_dataset_http``.
    """
    data = cfg["data"]
    ent_uid_col = data["entity_uid_column"]
    art_uid_col = data["artifact_uid_column"]
    file_col = data["file_column"]
    artifact_datasets = cfg["artifact_datasets"]

    # Guard against rename collisions
    for col in ("uid", "file"):
        if col in ent_df.columns and col != ent_uid_col:
            raise ValueError(
                f"entity_manifest already has a '{col}' column; rename "
                "conflicts with broker-standard column names"
            )
        if col in art_df.columns and col not in (ent_uid_col, art_uid_col, file_col):
            raise ValueError(
                f"artifact_manifest already has a '{col}' column; rename "
                "conflicts with broker-standard column names"
            )

    # Provenance tiering: single-unique-value columns promote to dataset metadata
    ent_constants, _ = split_constant_cols(ent_df)
    art_constants, _ = split_constant_cols(art_df)

    # Identifier/locator columns are never promoted (keep per-row even if single-valued)
    for col in (ent_uid_col, art_uid_col, file_col, "type"):
        ent_constants.pop(col, None)
        art_constants.pop(col, None)

    # YAML-authored metadata wins over auto-promoted constants on conflict
    merged_metadata = {**ent_constants, **art_constants, **dataset_metadata}

    promoted = sorted(set(ent_constants) | set(art_constants))
    if promoted:
        print(f"Promoted {len(promoted)} constant column(s) to dataset metadata: {promoted}")

    # --- Build broker-shape entity DataFrame ---
    ent_keep = [c for c in ent_df.columns if c not in ent_constants]
    ent_broker = ent_df[ent_keep].rename(columns={ent_uid_col: "uid"})

    # --- Build broker-shape artifact DataFrame (fan-out) ---
    art_keep = [c for c in art_df.columns if c not in art_constants]
    art_rename = {ent_uid_col: "uid", file_col: "file"}
    if art_uid_col != "auid":
        art_rename[art_uid_col] = "auid"
    art_trimmed = art_df[art_keep].rename(columns=art_rename)

    # If ent_df was already limited by the caller (-n flag), narrow art_df
    # to matching uids so fan-out doesn't process orphan producer rows.
    ent_uid_set = set(ent_broker["uid"])
    art_trimmed = art_trimmed[art_trimmed["uid"].isin(ent_uid_set)]

    fanout_rows = []
    unknown_types = set()
    for _, row in art_trimmed.iterrows():
        atype = row["type"]
        paths = artifact_datasets.get(atype)
        if not paths:
            unknown_types.add(atype)
            continue
        row_dict = row.to_dict()
        for ds_path in paths:
            new_row = dict(row_dict)
            new_row["dataset"] = ds_path
            new_row["array_name"] = ds_path.rsplit("/", 1)[-1]
            new_row["index"] = None
            fanout_rows.append(new_row)

    if unknown_types:
        print(
            f"WARNING: {len(unknown_types)} artifact type(s) in manifest have "
            f"no entry in artifact_datasets - skipping: {sorted(unknown_types)}"
        )

    if not fanout_rows:
        print(f"ERROR: no artifact rows after fan-out for '{label}'; "
              "check artifact_datasets covers all types in the manifest")
        return False

    art_broker = pd.DataFrame(fanout_rows)

    print(
        f"Manifest layout: {len(ent_broker)} entities x "
        f"{len(art_broker) / max(len(ent_broker), 1):.1f} artifacts/entity (fan-out)"
    )

    return register_dataset_http(
        client, ent_broker, art_broker, base_dir, label,
        dataset_key=dataset_key,
        dataset_metadata=merged_metadata,
        server_base_dir=server_base_dir,
    )


def verify_registration_http(client):
    """Verify registration via Tiled client.

    Args:
        client: Tiled client connected to a running server.
    """
    print("\n" + "=" * 50)
    print("Verification")
    print("=" * 50)

    total = len(client)
    print(f"Total entity containers: {total}")

    if total == 0:
        print("No containers registered yet.")
        return

    keys = list(client.keys())[:10]
    print(f"First keys: {keys[:3]}")

    # Find a container node to verify (skip non-container nodes like arrays)
    # TODO: each client[k] is an HTTP request; could be slow on servers with
    # many non-container nodes at root level.
    h = None
    ent_key = None
    for k in keys:
        node = client[k]
        if hasattr(node, "keys"):
            h = node
            ent_key = k
            break

    if h is None:
        print("No container nodes found at root level.")
        return

    meta = dict(h.metadata)

    print(f"\nContainer '{ent_key}':")

    param_keys = [k for k in meta if not k.startswith(("path_", "dataset_", "index_"))]
    print(f"  Metadata keys: {param_keys}")

    path_keys = [k for k in meta if k.startswith("path_")]
    dataset_keys = [k for k in meta if k.startswith("dataset_")]
    index_keys = [k for k in meta if k.startswith("index_")]
    print(f"\n  Locators in metadata:")
    print(f"    path_*:    {len(path_keys)}")
    print(f"    dataset_*: {len(dataset_keys)}")
    print(f"    index_*:   {len(index_keys)}")
    for pk in path_keys[:3]:
        val = meta[pk]
        if isinstance(val, str) and len(val) > 50:
            val = "..." + val[-47:]
        print(f"    {pk}: {val}")

    children = list(h.keys())
    print(f"\n  Array children: {len(children)}")
    if children:
        print(f"    {children[:5]}")
        if len(children) > 5:
            print(f"    ... and {len(children) - 5} more")

        child_key = children[0]
        child = h[child_key]
        print(f"\n  Sample child '{child_key}':")
        print(f"    Shape: {child.shape}")
        print(f"    Dtype: {child.dtype}")

    if path_keys and children:
        print("\n  VERIFIED: Both locators AND array children present!")
    else:
        print("\n  WARNING: Dual-mode incomplete!")
        if not path_keys:
            print("    Missing: path_* metadata")
        if not children:
            print("    Missing: array children")
