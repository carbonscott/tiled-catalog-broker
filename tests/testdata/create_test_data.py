#!/usr/bin/env python3
"""
Generate synthetic test data for VDP and NiPS3 datasets.

Creates small Parquet manifests and HDF5 files that follow the generic
manifest standard (uid + dynamic columns for entities; uid, type,
file, dataset, index + dynamic columns for artifacts).

Usage:
    python tests/testdata/create_test_data.py
"""

import os
from pathlib import Path

import h5py
import numpy as np
import pandas as pd


TESTDATA_DIR = Path(__file__).parent


def create_vdp_data():
    """Create VDP-style test data: many small HDF5 files, 1 artifact per file."""
    vdp_dir = TESTDATA_DIR / "vdp"
    art_dir = vdp_dir / "artifacts"
    art_dir.mkdir(parents=True, exist_ok=True)

    # --- entity manifest ---
    uids = [f"aaaa{i:04d}" for i in range(5)]
    ent_data = {
        "uid": uids,
        "key": [f"H_{h[:8]}" for h in uids],
        "Ja_meV": [1.0, 2.0, 0.5, 3.0, 1.5],
        "Jb_meV": [0.5, 1.0, 0.3, 1.5, 0.7],
        "Jc_meV": [-0.3, -0.5, -0.1, -1.0, -0.4],
        "Dc_meV": [0.1, 0.2, 0.05, 0.5, 0.15],
        "spin_s": [2.5, 2.5, 2.5, 2.5, 2.5],
        "g_factor": [2.0, 2.0, 2.0, 2.0, 2.0],
    }
    ent_df = pd.DataFrame(ent_data)
    ent_df.to_parquet(vdp_dir / "vdp_entities.parquet", index=False)

    # --- Artifact manifest + HDF5 files ---
    # 3 artifact types per entity
    artifact_specs = [
        {"type": "mh_powder_30T", "dataset": "/curve/M_parallel", "shape": (10,)},
        {"type": "gs_state", "dataset": "/gs/spin_dir", "shape": (3, 4)},
        {"type": "ins_12meV", "dataset": "/ins/broadened", "shape": (6, 5)},
    ]

    art_rows = []
    for uid in uids:
        for spec in artifact_specs:
            filename = f"{uid}_{spec['type']}.h5"
            filepath = art_dir / filename
            rel_path = f"artifacts/{filename}"

            # Create HDF5 file
            with h5py.File(filepath, "w") as f:
                rng = np.random.default_rng(hash(uid + spec["type"]) % 2**32)
                data = rng.standard_normal(spec["shape"])
                f.create_dataset(spec["dataset"], data=data)

            art_rows.append({
                "uid": uid,
                "type": spec["type"],
                "file": rel_path,
                "dataset": spec["dataset"],
            })

    art_df = pd.DataFrame(art_rows)
    art_df.to_parquet(vdp_dir / "vdp_artifacts.parquet", index=False)

    print(f"VDP: {len(ent_df)} entities, {len(art_df)} artifacts")
    print(f"  Dir: {vdp_dir}")


def create_nips3_data():
    """Create NiPS3-style test data: batched HDF5 files, many entities per file."""
    nips3_dir = TESTDATA_DIR / "nips3"
    nips3_dir.mkdir(parents=True, exist_ok=True)

    n_ents = 5

    # --- entity manifest ---
    uids = [f"rank0000_{i:04d}" for i in range(n_ents)]
    ent_data = {
        "uid": uids,
        "key": [f"H_{h[:8]}" for h in uids],
        "F2_dd": [100.0 + i * 10 for i in range(n_ents)],
        "F2_dp": [50.0 + i * 5 for i in range(n_ents)],
        "F4_dd": [200.0 + i * 10 for i in range(n_ents)],
        "G1_dp": [30.0 + i * 3 for i in range(n_ents)],
        "G3_dp": [15.0 + i * 1 for i in range(n_ents)],
    }
    ent_df = pd.DataFrame(ent_data)
    ent_df.to_parquet(nips3_dir / "nips3_entities.parquet", index=False)

    # --- Create batched HDF5 file ---
    batch_file = "NiPS3_rank0000.h5"
    batch_path = nips3_dir / batch_file

    rixs_shape = (n_ents, 6, 5)  # batch x energy x momentum
    mag_shape = (n_ents, 10)  # batch x field_points

    rng = np.random.default_rng(42)
    with h5py.File(batch_path, "w") as f:
        f.create_dataset("/RIXS", data=rng.standard_normal(rixs_shape))
        f.create_dataset("/MAG", data=rng.standard_normal(mag_shape))

    # --- Artifact manifest (exploded: one row per entity) ---
    art_rows = []
    for i, uid in enumerate(uids):
        art_rows.append({
            "uid": uid,
            "type": "rixs",
            "file": batch_file,
            "dataset": "/RIXS",
            "index": i,
        })
        art_rows.append({
            "uid": uid,
            "type": "mag",
            "file": batch_file,
            "dataset": "/MAG",
            "index": i,
        })

    art_df = pd.DataFrame(art_rows)
    art_df.to_parquet(nips3_dir / "nips3_artifacts.parquet", index=False)

    print(f"NiPS3: {len(ent_df)} entities, {len(art_df)} artifacts")
    print(f"  Dir: {nips3_dir}")


def main():
    print("Generating synthetic test data...")
    print()
    create_vdp_data()
    print()
    create_nips3_data()
    print()
    print("Done.")


if __name__ == "__main__":
    main()
