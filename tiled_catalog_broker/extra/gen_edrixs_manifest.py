"""
Generate EDRIXS manifests in the generic broker standard.

Reads a monolithic HDF5 file with 10K RIXS spectra:
  - 12 parameter arrays in ``params/{name}``, each shape (10000,)
  - Spectra in ``spectra``, shape (10000, 151, 40)

Each entity gets one ``rixs`` artifact with an ``index`` into the
spectra array.  UID format: ``edx00000``, ``edx00001``, ...
(3-char prefix + 5-digit index, 8 chars total for unique keys.)

Interface:
    generate(output_dir, n_entities=10) → (ent_df, art_df)

Source data:
    /sdf/.../tlinker/data/EDRIXS/NiPS3_combined_2.h5
"""

from pathlib import Path

import h5py
import pandas as pd


EDRIXS_H5 = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/tlinker/data/EDRIXS/NiPS3_combined_2.h5"

# Relative path from the readable_storage root to the H5 file
EDRIXS_FILE_REL = "NiPS3_combined_2.h5"

# Parameter names in the H5 params/ group
PARAM_NAMES = [
    "F2_dd", "F4_dd", "F2_dp", "G1_dp", "G3_dp",
    "tenDq", "soc_v_i", "soc_v_n", "soc_c",
    "Gam_c", "sigma", "xoffset",
]


def generate(output_dir, n_entities=10):
    """Generate EDRIXS manifests in the generic broker standard.

    Args:
        output_dir: Directory to write Parquet files.
        n_entities: Number of entities to include.

    Returns:
        (ent_df, art_df): Entity and artifact DataFrames.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with h5py.File(EDRIXS_H5, "r") as f:
        n_total = f["spectra"].shape[0]
        n = min(n_entities, n_total)

        # Read parameter arrays (first n entries)
        params = {}
        for name in PARAM_NAMES:
            params[name] = f[f"params/{name}"][:n]

    # Build entity manifest
    ent_records = []
    for i in range(n):
        uid = f"edx{i:05d}"
        record = {"uid": uid, "key": f"H_{uid[:8]}"}
        for name in PARAM_NAMES:
            record[name] = float(params[name][i])
        ent_records.append(record)

    ent_df = pd.DataFrame(ent_records)

    # Build artifact manifest — one rixs spectrum per entity
    art_records = []
    for i in range(n):
        uid = f"edx{i:05d}"
        art_records.append({
            "uid": uid,
            "type": "rixs",
            "file": EDRIXS_FILE_REL,
            "dataset": "spectra",
            "index": i,
        })

    art_df = pd.DataFrame(art_records)

    # Write Parquet files
    ent_out = output_dir / "edrixs_entities.parquet"
    art_out = output_dir / "edrixs_artifacts.parquet"
    ent_df.to_parquet(ent_out, index=False)
    art_df.to_parquet(art_out, index=False)

    print(f"  EDRIXS source: {n_total} total spectra in {Path(EDRIXS_H5).name}")
    print(f"  EDRIXS output: {len(ent_df)} entities, {len(art_df)} artifacts")
    print(f"  Written to: {output_dir}")

    return ent_df, art_df
