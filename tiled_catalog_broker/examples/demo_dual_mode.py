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

# Add tiled_poc directory to path for broker package imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from broker.config import get_tiled_url, get_api_key, get_base_dir, get_service_dir


def demo_mode_a_expert(client):
    """
    Mode A: Expert path-based access.

    Best for:
    - ML pipelines that need bulk data loading
    - Users who want to handle file I/O themselves
    - Maximum performance (direct HDF5, no HTTP overhead)
    """
    from broker.query_manifest import query_manifest, load_from_manifest, build_mh_dataset

    print("=" * 60)
    print("MODE A: Expert Path-Based Access")
    print("=" * 60)
    print("Best for: ML pipelines, bulk loading, maximum performance")
    print()

    # Step 1: Query to get manifest with paths
    print("Step 1: Query manifest (Tiled filters -> DataFrame with paths)")
    t0 = time.perf_counter()
    manifest = query_manifest(client, axis="powder", Hmax_T=30)
    query_time = (time.perf_counter() - t0) * 1000
    print(f"  Found {len(manifest)} curves in {query_time:.1f} ms")

    if len(manifest) > 0:
        print(f"\n  Sample manifest row:")
        row = manifest.iloc[0]
        print(f"    ent_key: {row['ent_key']}")
        print(f"    Ja_meV: {row['Ja_meV']:.3f}")
        print(f"    path_rel: ...{row['path_rel'][-40:]}")

    # Step 2: Load data directly from HDF5
    print("\nStep 2: Load data directly from HDF5 (no Tiled)")
    t0 = time.perf_counter()
    X, Theta = load_from_manifest(manifest)
    load_time = (time.perf_counter() - t0) * 1000
    print(f"  Loaded {len(X)} curves in {load_time:.1f} ms")
    print(f"  X shape: {X.shape}")
    print(f"  Theta shape: {Theta.shape}")

    # Total time
    total = query_time + load_time
    print(f"\n  Total time: {total:.1f} ms")

    # Alternative: One-step API
    print("\nAlternative: One-step build_mh_dataset() (Julia-equivalent)")
    t0 = time.perf_counter()
    X2, h_grid, Theta2, manifest2 = build_mh_dataset(client, axis="powder", Hmax_T=30)
    total_time = (time.perf_counter() - t0) * 1000
    print(f"  Loaded {len(X2)} curves in {total_time:.1f} ms")


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

    # Get first container
    keys = list(client.keys())
    if not keys:
        print("No containers found!")
        return

    ent_key = keys[0]
    h = client[ent_key]

    print(f"Container: {ent_key}")
    print(f"  Physics params: Ja={h.metadata['Ja_meV']:.3f}, Jb={h.metadata['Jb_meV']:.3f}")

    # List available arrays
    children = list(h.keys())
    print(f"\n  Available arrays: {children}")

    # Access INS spectrum (large array - chunked access is useful)
    if "ins_12meV" in children:
        print("\n  INS spectrum (600x400 = 240K points):")

        # Full array
        t0 = time.perf_counter()
        ins_full = h["ins_12meV"][:]
        full_time = (time.perf_counter() - t0) * 1000
        print(f"    Full array: shape={ins_full.shape}, time={full_time:.1f} ms")

        # Sliced access (useful for visualization)
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

        # Slice works too (though less useful for small arrays)
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
    import numpy as np

    base_dir = get_base_dir()

    print("\n" + "=" * 60)
    print("SAME DATA, TWO ACCESS PATTERNS")
    print("=" * 60)

    # Get first container
    keys = list(client.keys())
    if not keys:
        print("No containers found!")
        return

    ent_key = keys[0]
    h = client[ent_key]

    # Mode A: Get path from metadata, load directly
    print(f"\nContainer: {ent_key}")
    print("\nMode A (Expert):")
    path_rel = h.metadata.get("path_mh_powder_30T")
    if path_rel:
        path = os.path.join(base_dir, path_rel)
        print(f"  Path from metadata: ...{path_rel[-40:]}")
        with h5py.File(path, "r") as f:
            data_a = f["/curve/M_parallel"][:]
        print(f"  Loaded via h5py: shape={data_a.shape}")

    # Mode B: Access via Tiled adapter
    print("\nMode B (Visualizer):")
    if "mh_powder_30T" in h.keys():
        data_b = h["mh_powder_30T"][:]
        print(f"  Loaded via Tiled: shape={data_b.shape}")

    # Verify they're the same
    if path_rel and "mh_powder_30T" in h.keys():
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
