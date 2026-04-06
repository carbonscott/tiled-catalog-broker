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
V6 Discovery API: Query Tiled, get manifest DataFrame, load data directly.

This is the "expert" access mode in V6. For visualization/chunked access,
use the Tiled adapter pattern instead (h["mh_powder_30T"][:]).

This module provides:
- query_manifest(): Query Tiled, returns DataFrame with paths + physics params
- load_from_manifest(): Load data directly from HDF5 (no Tiled)
- build_mh_dataset(): Convenience wrapper for M(H) curves (matching Julia API)
- build_ins_dataset(): Convenience wrapper for INS spectra

Usage:
    from tiled.client import from_uri
    from query_manifest import query_manifest, load_from_manifest, build_mh_dataset

    client = from_uri("http://localhost:8005", api_key="secret")

    # Option 1: Two-step (query then load)
    manifest = query_manifest(client, axis="powder", Hmax_T=30, Ja_min=0)
    X, Theta = load_from_manifest(manifest)

    # Option 2: One-step (Julia-equivalent API)
    X, h_grid, Theta, manifest = build_mh_dataset(client, axis="powder", Hmax_T=30)
"""

import os
import h5py
import numpy as np
import pandas as pd
from tiled.queries import Key

from .config import get_base_dir, get_dataset_paths, get_tiled_url, get_api_key


def query_manifest(client, *, artifact_type="mh_curve", axis=None, Hmax_T=None,
                   Ei_meV=None, Ja_min=None, Ja_max=None, Jb_min=None, Jb_max=None,
                   Jc_min=None, Jc_max=None, Dc_min=None, Dc_max=None):
    """
    Query Tiled and return filtered manifest as DataFrame.

    Args:
        client: Tiled client connected to V6 catalog
        artifact_type: "mh_curve", "gs_state", or "ins_powder"
        axis: For mh_curve - "powder", "x", "y", "z"
        Hmax_T: For mh_curve - 7 or 30
        Ei_meV: For ins_powder - 12 or 25
        Ja_min/Ja_max: Filter by Ja_meV range
        Jb_min/Jb_max: Filter by Jb_meV range
        Jc_min/Jc_max: Filter by Jc_meV range
        Dc_min/Dc_max: Filter by Dc_meV range

    Returns:
        DataFrame with columns:
            uid, ent_key, Ja_meV, Jb_meV, Jc_meV, Dc_meV, spin_s, g_factor, path_rel
    """
    # Build Tiled query from physics parameters
    results = client

    if Ja_min is not None:
        results = results.search(Key("Ja_meV") >= Ja_min)
    if Ja_max is not None:
        results = results.search(Key("Ja_meV") <= Ja_max)
    if Jb_min is not None:
        results = results.search(Key("Jb_meV") >= Jb_min)
    if Jb_max is not None:
        results = results.search(Key("Jb_meV") <= Jb_max)
    if Jc_min is not None:
        results = results.search(Key("Jc_meV") >= Jc_min)
    if Jc_max is not None:
        results = results.search(Key("Jc_meV") <= Jc_max)
    if Dc_min is not None:
        results = results.search(Key("Dc_meV") >= Dc_min)
    if Dc_max is not None:
        results = results.search(Key("Dc_meV") <= Dc_max)

    # Determine path key based on artifact type
    if artifact_type == "mh_curve":
        if axis is None or Hmax_T is None:
            raise ValueError("mh_curve requires axis and Hmax_T")
        path_key = f"path_mh_{axis}_{int(Hmax_T)}T"
    elif artifact_type == "gs_state":
        path_key = "path_gs_state"
    elif artifact_type == "ins_powder":
        if Ei_meV is None:
            raise ValueError("ins_powder requires Ei_meV")
        path_key = f"path_ins_{int(Ei_meV)}meV"
    else:
        raise ValueError(f"Unknown artifact_type: {artifact_type}")

    # Extract manifest rows from query results
    # Use .items() for batch fetching (9-12x faster than per-key lookup)
    rows = []
    for ent_key, h in results.items():
        meta = h.metadata
        path_rel = meta.get(path_key)

        if path_rel is None:
            continue  # Skip if this artifact type doesn't exist

        rows.append({
            "uid": meta["uid"],
            "ent_key": ent_key,
            "Ja_meV": meta["Ja_meV"],
            "Jb_meV": meta["Jb_meV"],
            "Jc_meV": meta["Jc_meV"],
            "Dc_meV": meta["Dc_meV"],
            "spin_s": meta.get("spin_s", 2.5),
            "g_factor": meta.get("g_factor", 2.0),
            "path_rel": path_rel,
        })

    return pd.DataFrame(rows)


def load_from_manifest(manifest_df, *, artifact_type="mh_curve", clamp_H0=True,
                       base_dir=None):
    """
    Load data directly from manifest paths (no Tiled involved).

    Args:
        manifest_df: DataFrame from query_manifest() with path_rel column
        artifact_type: "mh_curve", "gs_state", or "ins_powder"
        clamp_H0: For mh_curve - set M(H=0) to zero
        base_dir: Base directory for HDF5 files (default from config)

    Returns:
        X: (n_samples, ...) array data
        Theta: (n_samples, 6) parameters [Ja, Jb, Jc, Dc, spin_s, g_factor]
    """
    if base_dir is None:
        base_dir = get_base_dir()

    dataset_paths = get_dataset_paths()
    dataset_path = dataset_paths.get(artifact_type)
    if dataset_path is None:
        raise ValueError(f"Unknown artifact_type: {artifact_type}")

    X_list = []
    Theta_list = []

    for _, row in manifest_df.iterrows():
        path_rel = row.get("path_rel")
        if path_rel is None:
            continue

        path = os.path.join(base_dir, path_rel)
        if not os.path.exists(path):
            continue

        with h5py.File(path, "r") as f:
            data = f[dataset_path][:]

        # Normalize for mh_curve
        if artifact_type == "mh_curve":
            spin_s = row.get("spin_s", 2.5)
            g_factor = row.get("g_factor", 2.0)
            Msat = g_factor * spin_s

            if clamp_H0:
                data = data.copy()
                data[0] = 0.0

            data = data / Msat

        X_list.append(data)
        Theta_list.append([
            row["Ja_meV"],
            row["Jb_meV"],
            row["Jc_meV"],
            row["Dc_meV"],
            row.get("spin_s", 2.5),
            row.get("g_factor", 2.0),
        ])

    if not X_list:
        raise ValueError("No data loaded from manifest")

    X = np.stack(X_list, dtype=np.float32)
    Theta = np.array(Theta_list, dtype=np.float32)

    return X, Theta


def build_mh_dataset(client, *, axis="powder", Hmax_T=30, clamp_H0=True, **filters):
    """
    Build M(H) dataset - Julia-equivalent API.

    Args:
        client: Tiled client
        axis: "powder", "x", "y", "z"
        Hmax_T: 7 or 30
        clamp_H0: Set M(H=0) to zero
        **filters: Physics filters (Ja_min, Ja_max, Jb_min, Jb_max, etc.)

    Returns:
        X: (n_curves, n_points) normalized magnetization
        h_grid: (n_points,) reduced field [0, 1]
        Theta: (n_curves, 6) parameters [Ja, Jb, Jc, Dc, spin_s, g_factor]
        manifest: DataFrame with metadata

    Example:
        # Load all powder M(H) at 30T
        X, h_grid, Theta, manifest = build_mh_dataset(client, axis="powder", Hmax_T=30)

        # Load only ferromagnetic (Ja > 0)
        X, h_grid, Theta, manifest = build_mh_dataset(
            client, axis="powder", Hmax_T=30, Ja_min=0
        )
    """
    manifest = query_manifest(
        client, artifact_type="mh_curve", axis=axis, Hmax_T=Hmax_T, **filters
    )

    if manifest.empty:
        raise ValueError(f"No curves found for axis={axis}, Hmax_T={Hmax_T}")

    X, Theta = load_from_manifest(manifest, artifact_type="mh_curve", clamp_H0=clamp_H0)
    h_grid = np.linspace(0, 1, X.shape[1], dtype=np.float32)

    return X, h_grid, Theta, manifest


def build_ins_dataset(client, *, Ei_meV=12, **filters):
    """
    Build INS dataset - convenience wrapper for INS spectra.

    Args:
        client: Tiled client
        Ei_meV: Incident energy (12 or 25)
        **filters: Physics filters (Ja_min, Ja_max, Jb_min, Jb_max, etc.)

    Returns:
        spectra: (n_samples, nq, nw) INS spectra array
        Theta: (n_samples, 6) parameters [Ja, Jb, Jc, Dc, spin_s, g_factor]
        manifest: DataFrame with metadata

    Example:
        # Load all INS spectra at Ei=12 meV
        spectra, Theta, manifest = build_ins_dataset(client, Ei_meV=12)

        # Load only ferromagnetic (Ja > 0)
        spectra, Theta, manifest = build_ins_dataset(client, Ei_meV=12, Ja_min=0)
    """
    manifest = query_manifest(
        client, artifact_type="ins_powder", Ei_meV=Ei_meV, **filters
    )

    if manifest.empty:
        raise ValueError(f"No INS spectra found for Ei_meV={Ei_meV}")

    spectra, Theta = load_from_manifest(manifest, artifact_type="ins_powder")

    return spectra, Theta, manifest


def main():
    """Demo: Query and load M(H) dataset."""
    import time
    from tiled.client import from_uri

    tiled_url = get_tiled_url()
    api_key = get_api_key()

    print("=" * 60)
    print("V6 Discovery API Demo (Expert Mode)")
    print("=" * 60)

    # Connect to Tiled
    print(f"Connecting to {tiled_url}...")
    client = from_uri(tiled_url, api_key=api_key)
    print(f"Catalog contains {len(client)} entities")

    # Query manifest
    print("\nQuerying manifest (axis=powder, Hmax_T=30)...")
    t0 = time.perf_counter()
    manifest = query_manifest(client, axis="powder", Hmax_T=30)
    query_time = (time.perf_counter() - t0) * 1000
    print(f"  Found {len(manifest)} curves in {query_time:.1f} ms")

    if len(manifest) > 0:
        print(f"\nManifest preview (first 3 rows):")
        print(manifest[["ent_key", "Ja_meV", "Jb_meV", "Jc_meV", "Dc_meV"]].head(3).to_string())

    # Load data
    print("\nLoading data from HDF5...")
    t0 = time.perf_counter()
    X, Theta = load_from_manifest(manifest)
    load_time = (time.perf_counter() - t0) * 1000
    print(f"  Loaded {len(X)} curves in {load_time:.1f} ms")
    print(f"  X shape: {X.shape}")
    print(f"  Theta shape: {Theta.shape}")

    # Test filtered query
    print("\nFiltered query (Ja > 0, ferromagnetic)...")
    t0 = time.perf_counter()
    X_fm, h_grid, Theta_fm, manifest_fm = build_mh_dataset(
        client, axis="powder", Hmax_T=30, Ja_min=0
    )
    total_time = (time.perf_counter() - t0) * 1000
    print(f"  Found {len(X_fm)} ferromagnetic curves in {total_time:.1f} ms")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == "__main__":
    main()
