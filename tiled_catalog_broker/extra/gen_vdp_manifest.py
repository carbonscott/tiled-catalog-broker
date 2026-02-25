"""
Generate VDP manifests in the generic broker standard.

Reads existing VDP Parquet manifests and transforms them:
  - Rename ``path_rel`` → ``file``
  - Add ``dataset`` column mapped from artifact type
  - Make ``type`` unique per entity (e.g., mh_powder_30T, ins_12meV)
  - Preserve all original columns as extra metadata

Interface:
    generate(output_dir, n_entities=10) → (ent_df, art_df)

Source data:
    $VDP_DATA/data/schema_v1/manifest_*.parquet
"""

import os
from pathlib import Path

import pandas as pd


# Where VDP data lives
VDP_DATA_DIR = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/vdp"
VDP_BASE_DIR = f"{VDP_DATA_DIR}/data/schema_v1"

# HDF5 dataset paths for each artifact type
DATASET_MAP = {
    "gs_state":  "/gs/spin_dir",
    "mh_curve":  "/curve/M_parallel",
    "ins_powder": "/ins/broadened",
}


def _find_latest(pattern):
    """Find latest file matching a glob pattern."""
    import glob
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matching: {pattern}")
    return files[-1]


def _make_unique_type(row):
    """Create a unique artifact type key from the raw type + parameters.

    Examples:
        gs_state                         → gs_state
        mh_curve + axis=powder + Hmax=30 → mh_powder_30T
        mh_curve + axis=x + Hmax=7      → mh_x_7T
        ins_powder + Ei=12               → ins_12meV
    """
    t = row["type"]
    if t == "gs_state":
        return "gs_state"
    if t == "mh_curve":
        axis = row.get("axis", "powder")
        hmax = int(row.get("Hmax_T", 7))
        return f"mh_{axis}_{hmax}T"
    if t == "ins_powder":
        ei = int(row.get("Ei_meV", 12))
        return f"ins_{ei}meV"
    return t


def generate(output_dir, n_entities=10):
    """Generate VDP manifests in the generic broker standard.

    Args:
        output_dir: Directory to write Parquet files.
        n_entities: Number of entities to include.

    Returns:
        (ent_df, art_df): Entity and artifact DataFrames.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load raw VDP manifests
    ent_path = _find_latest(f"{VDP_BASE_DIR}/manifest_hamiltonians_*.parquet")
    art_path = _find_latest(f"{VDP_BASE_DIR}/manifest_artifacts_*.parquet")

    ent_raw = pd.read_parquet(ent_path)
    ent_raw = ent_raw.rename(columns={"huid": "uid"})
    art_raw = pd.read_parquet(art_path)
    art_raw = art_raw.rename(columns={"huid": "uid"})

    print(f"  VDP source: {len(ent_raw)} entities, {len(art_raw)} artifacts")

    # Subset entities
    ent_df = ent_raw.head(n_entities).copy()
    selected_uids = set(ent_df["uid"])

    # Filter artifacts to selected entities
    art_df = art_raw[art_raw["uid"].isin(selected_uids)].copy()

    # Transform artifact manifest to generic standard
    # 1. Rename path_rel → file
    art_df = art_df.rename(columns={"path_rel": "file"})

    # 2. Add dataset column from type mapping
    art_df["dataset"] = art_df["type"].map(DATASET_MAP)

    # 3. Make type unique per entity
    art_df["type"] = art_df.apply(_make_unique_type, axis=1)

    # Add key column (Tiled catalog key for each entity)
    ent_df["key"] = ent_df["uid"].apply(lambda h: f"H_{h[:8]}")

    # Write Parquet files
    ent_out = output_dir / "vdp_entities.parquet"
    art_out = output_dir / "vdp_artifacts.parquet"
    ent_df.to_parquet(ent_out, index=False)
    art_df.to_parquet(art_out, index=False)

    print(f"  VDP output: {len(ent_df)} entities, {len(art_df)} artifacts")
    print(f"  Written to: {output_dir}")

    return ent_df, art_df
