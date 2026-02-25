"""
Generate NiPS3 Multimodal manifests in the generic broker standard.

Reads individual HDF5 files, each containing 9 scalar parameters and
6 artifact datasets:

  Parameters (scalar): J1a, J1b, J2a, J2b, J3a, J3b, J4, Ax, Az
  Artifacts:
    mag_a       — Ma magnetization curve, shape (51,)
    mag_b       — Mb magnetization curve, shape (51,)
    mag_cs      — Mcs magnetization curve, shape (51,)
    ins_hisym   — S(Q,w) high-symmetry path, shape (384, 384)
    ins_powder  — Powder S(|Q|,w), shape (512, 256)
    ins_powder_mask — Powder mask, shape (512, 256)

UID format: ``mm_{file_id}`` (e.g., ``mm_401``)

Interface:
    generate(output_dir, n_entities=10) → (ent_df, art_df)

Source data:
    /sdf/.../tlinker/data/NiPS3_Multimodal_Synthetic/data/*.h5
"""

import os
from pathlib import Path

import h5py
import pandas as pd


MULTIMODAL_DIR = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/tlinker/data/NiPS3_Multimodal_Synthetic/data"

# Scalar parameters to read from each file
PARAM_NAMES = ["J1a", "J1b", "J2a", "J2b", "J3a", "J3b", "J4", "Ax", "Az"]

# Artifacts to register (key → HDF5 dataset name)
ARTIFACT_MAP = {
    "mag_a":          "Ma",
    "mag_b":          "Mb",
    "mag_cs":         "Mcs",
    "ins_hisym":      "hisym",
    "ins_powder":     "powder",
    "ins_powder_mask": "powder_mask",
}


def generate(output_dir, n_entities=10):
    """Generate Multimodal manifests in the generic broker standard.

    Args:
        output_dir: Directory to write Parquet files.
        n_entities: Number of entities to include.

    Returns:
        (ent_df, art_df): Entity and artifact DataFrames.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find H5 files sorted by numeric ID
    h5_files = sorted(
        Path(MULTIMODAL_DIR).glob("*.h5"),
        key=lambda p: int(p.stem),
    )
    n = min(n_entities, len(h5_files))
    h5_files = h5_files[:n]

    print(f"  Multimodal source: {len(list(Path(MULTIMODAL_DIR).glob('*.h5')))} files in {MULTIMODAL_DIR}")

    ent_records = []
    art_records = []

    for h5_path in h5_files:
        file_id = h5_path.stem  # e.g., "401"
        uid = f"mm_{file_id}"
        # Relative path from the readable_storage root
        file_rel = h5_path.name  # e.g., "401.h5"

        with h5py.File(h5_path, "r") as f:
            # Read scalar parameters
            record = {"uid": uid, "key": f"H_{uid[:8]}"}
            for name in PARAM_NAMES:
                record[name] = float(f[name][()])
            ent_records.append(record)

            # Register artifacts
            for art_key, ds_name in ARTIFACT_MAP.items():
                art_records.append({
                    "uid": uid,
                    "type": art_key,
                    "file": file_rel,
                    "dataset": ds_name,
                })

    ent_df = pd.DataFrame(ent_records)
    art_df = pd.DataFrame(art_records)

    # Write Parquet files
    ent_out = output_dir / "multimodal_entities.parquet"
    art_out = output_dir / "multimodal_artifacts.parquet"
    ent_df.to_parquet(ent_out, index=False)
    art_df.to_parquet(art_out, index=False)

    print(f"  Multimodal output: {len(ent_df)} entities, {len(art_df)} artifacts")
    print(f"  Written to: {output_dir}")

    return ent_df, art_df
