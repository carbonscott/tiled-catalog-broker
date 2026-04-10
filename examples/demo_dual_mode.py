#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "tiled[server]",
#     "pandas",
#     "h5py",
#     "numpy",
#     "ruamel.yaml",
# ]
# ///
"""
V6 Dual-Mode Demo: Demonstrate both access patterns.

V6 provides two access modes for the SAME data:
  - Mode A (Expert): Path-based access via metadata -> direct HDF5 loading
  - Mode B (Visualizer): Tiled adapter access -> chunked slicing via HTTP

Users choose the pattern that fits their use case.

Usage:
    # Start the V6 server first:
    #   tiled serve config config.yml --api-key secret
    #
    # Run this demo:
    python demo_dual_mode.py
"""

import os
import sys
import time
from pathlib import Path

import numpy as np
from ruamel.yaml import YAML

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from tiled_catalog_broker.config import get_tiled_url, get_api_key, get_service_dir


def _load_base_dirs():
    """Load base_dir from each dataset YAML config in datasets/."""
    yaml = YAML()
    base_dirs = {}
    datasets_dir = Path(__file__).parent.parent / "datasets"
    for cfg_path in sorted(datasets_dir.glob("*.yaml")):
        with open(cfg_path) as f:
            cfg = yaml.load(f)
        base_dirs[cfg["key"]] = cfg["base_dir"]
    return base_dirs


def _find_dataset_container(client, artifact_type):
    """Find the first dataset-level container that has the given artifact type."""
    for key in client.keys():
        container = client[key]
        ents = list(container.keys())[:1]
        if ents and f"path_{artifact_type}" in dict(container[ents[0]].metadata):
            return key, container
    return None, None


def demo_mode_a_expert(client):
    """
    Mode A: Expert path-based access.

    Best for:
    - ML pipelines that need bulk data loading
    - Users who want to handle file I/O themselves
    - Maximum performance (direct HDF5, no HTTP overhead)
    """
    from tiled_catalog_broker.query_manifest import query_catalog, load_artifacts

    print("=" * 60)
    print("MODE A: Expert Path-Based Access")
    print("=" * 60)
    print("Best for: ML pipelines, bulk loading, maximum performance")
    print()

    # Navigate to dataset-level container (root -> dataset containers -> entities)
    artifact_type = "mh_powder_30T"
    dataset_key, dataset_client = _find_dataset_container(client, artifact_type)
    if dataset_client is None:
        print(f"  No dataset found with artifact_type={artifact_type}")
        return

    # Load base_dir for this dataset from its YAML config
    base_dirs = _load_base_dirs()
    base_dir = base_dirs.get(dataset_key)
    if base_dir is None:
        print(f"  No dataset config found for '{dataset_key}'")
        return

    # Step 1: Query to get manifest with all metadata
    print("Step 1: Query catalog (Tiled filters -> DataFrame with all metadata)")
    t0 = time.perf_counter()
    manifest = query_catalog(dataset_client, artifact_type=artifact_type)
    query_time = (time.perf_counter() - t0) * 1000
    print(f"  Found {len(manifest)} curves in {query_time:.1f} ms")
    print(f"  Metadata columns: {list(manifest.columns)}")

    if len(manifest) > 0:
        print(f"\n  Sample manifest row:")
        row = manifest.iloc[0]
        print(f"    ent_key: {row['ent_key']}")
        if "Ja_meV" in manifest.columns:
            print(f"    Ja_meV: {row['Ja_meV']:.3f}")
        print(f"    path_{artifact_type}: ...{str(row[f'path_{artifact_type}'])[-40:]}")

    # Step 2: Load data directly from HDF5
    print("\nStep 2: Load data directly from HDF5 (no Tiled)")
    t0 = time.perf_counter()
    arrays = load_artifacts(manifest, artifact_type=artifact_type, base_dir=base_dir)
    load_time = (time.perf_counter() - t0) * 1000
    print(f"  Loaded {len(arrays)} arrays in {load_time:.1f} ms")
    if arrays:
        print(f"  Array shape: {arrays[0].shape}")

    # Caller assembles X and Theta (normalization is the caller's job)
    X = np.stack(arrays, dtype=np.float32)
    if "g_factor" in manifest.columns and "spin_s" in manifest.columns:
        Msat = manifest["g_factor"].values * manifest["spin_s"].values
        X = X / Msat[:, None]
    Theta = manifest[["Ja_meV", "Jb_meV", "Jc_meV", "Dc_meV"]].to_numpy(dtype=np.float32)

    print(f"  X shape (normalized): {X.shape}")
    print(f"  Theta shape: {Theta.shape}")
    print(f"\n  Total time: {query_time + load_time:.1f} ms")


def demo_mode_b_visualizer(client):
    """
    Mode B: Tiled adapter access.

    Best for:
    - Visualization tools that need chunked access
    - Interactive exploration (load slices, not full arrays)
    - Remote users accessing via HTTP
    """
    print("\n" + "=" * 60)
    print("MODE B: Tiled Adapter Access (Visualization)")
    print("=" * 60)
    print("Best for: Visualization, interactive exploration, remote access")
    print()

    # Navigate to dataset-level container (root -> dataset containers -> entities)
    _, dataset_client = _find_dataset_container(client, "mh_powder_30T")
    if dataset_client is None:
        print("No dataset with mh_powder_30T found!")
        return

    ent_key = list(dataset_client.keys())[0]
    h = dataset_client[ent_key]

    print(f"Container: {ent_key}")
    meta = h.metadata
    if "Ja_meV" in meta and "Jb_meV" in meta:
        print(f"  Physics params: Ja={meta['Ja_meV']:.3f}, Jb={meta['Jb_meV']:.3f}")

    # List available arrays
    children = list(h.keys())
    print(f"\n  Available arrays: {children}")

    # Access INS spectrum (large array - chunked access is useful)
    if "ins_12meV" in children:
        print("\n  INS spectrum (600x400 = 240K points):")

        t0 = time.perf_counter()
        ins_full = h["ins_12meV"][:]
        full_time = (time.perf_counter() - t0) * 1000
        print(f"    Full array: shape={ins_full.shape}, time={full_time:.1f} ms")

        t0 = time.perf_counter()
        ins_slice = h["ins_12meV"][100:200, 50:150]
        slice_time = (time.perf_counter() - t0) * 1000
        print(f"    Q-slice [100:200, 50:150]: shape={ins_slice.shape}, time={slice_time:.1f} ms")

    # Access M(H) curve (small array)
    if "mh_powder_30T" in children:
        print("\n  M(H) curve (200 points):")

        t0 = time.perf_counter()
        mh_full = h["mh_powder_30T"][:]
        mh_time = (time.perf_counter() - t0) * 1000
        print(f"    Full curve: shape={mh_full.shape}, time={mh_time:.1f} ms")

        mh_slice = h["mh_powder_30T"][50:100]
        print(f"    Partial [50:100]: shape={mh_slice.shape}")

    # Access ground state (tiny array)
    if "gs_state" in children:
        print("\n  Ground state (3x8 = 24 elements):")
        gs = h["gs_state"][:]
        print(f"    Shape: {gs.shape}")


def demo_same_data_two_modes(client):
    """
    Show that both modes access the SAME underlying data.
    """
    import h5py

    print("\n" + "=" * 60)
    print("SAME DATA, TWO ACCESS PATTERNS")
    print("=" * 60)

    # Navigate to dataset-level container (root -> dataset containers -> entities)
    dataset_key, dataset_client = _find_dataset_container(client, "mh_powder_30T")
    if dataset_client is None:
        print("No dataset with mh_powder_30T found!")
        return

    # Load base_dir for this dataset
    base_dirs = _load_base_dirs()
    base_dir = base_dirs.get(dataset_key)
    if base_dir is None:
        print(f"  No dataset config found for '{dataset_key}'")
        return

    ent_key = list(dataset_client.keys())[0]
    h = dataset_client[ent_key]

    # Mode A: Get locators from metadata, load directly
    print(f"\nContainer: {ent_key}")
    print("\nMode A (Expert):")
    path_rel = h.metadata.get("path_mh_powder_30T")
    dataset_path = h.metadata.get("dataset_mh_powder_30T")
    if path_rel and dataset_path:
        path = os.path.join(base_dir, path_rel)
        print(f"  Path from metadata: ...{path_rel[-40:]}")
        print(f"  Dataset path: {dataset_path}")
        with h5py.File(path, "r") as f:
            data_a = f[dataset_path][:]
        print(f"  Loaded via h5py: shape={data_a.shape}")

    # Mode B: Access via Tiled adapter
    print("\nMode B (Visualizer):")
    if "mh_powder_30T" in h.keys():
        data_b = h["mh_powder_30T"][:]
        print(f"  Loaded via Tiled: shape={data_b.shape}")

    # Verify they're the same
    if path_rel and dataset_path and "mh_powder_30T" in h.keys():
        match = np.allclose(data_a, data_b)
        print(f"\nData matches: {match}")
        if match:
            print("Both modes access the SAME underlying HDF5 data!")


def main():
    from tiled.client import from_uri

    tiled_url = get_tiled_url()
    api_key = get_api_key()

    print("=" * 60)
    print("V6 UNIFIED DUAL-MODE CATALOG DEMO")
    print("=" * 60)
    print(f"Tiled URL: {tiled_url}")
    print()
    print("V6 provides TWO access modes for the SAME data:")
    print("  Mode A (Expert):     Paths in metadata -> direct HDF5")
    print("  Mode B (Visualizer): Tiled adapters -> chunked HTTP access")
    print()

    # Connect to Tiled
    try:
        client = from_uri(tiled_url, api_key=api_key)
    except Exception as e:
        service_dir = get_service_dir()
        print(f"ERROR: Cannot connect to Tiled server at {tiled_url}")
        print(f"  {e}")
        print("\nStart the server first:")
        print(f"  uv run --with 'tiled[server]' tiled serve config {service_dir}/config.yml --api-key secret")
        sys.exit(1)

    print(f"Connected! Catalog contains {len(client)} entities")

    if len(client) == 0:
        print("\nNo data registered yet. Run register_catalog.py first.")
        sys.exit(1)

    # Run demos
    demo_mode_a_expert(client)
    demo_mode_b_visualizer(client)
    demo_same_data_two_modes(client)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("V6 gives users the CHOICE:")
    print("  - ML experts: Use path-based access for fast bulk loading")
    print("  - Visualizers: Use Tiled adapters for interactive exploration")
    print("  - Same catalog serves both use cases!")
    print("=" * 60)


if __name__ == "__main__":
    main()
