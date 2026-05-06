"""Generate Zhantao_C entity + artifact Parquet manifests.

Source: a single ~19.5 GB HDF5 file with 20,000 ``sample_<N>`` groups, each
holding 7 array datasets and a ``params/`` subgroup with 9 float32 scalars.
The locator pattern is a single file with per-sample HDF5 paths (the
``index`` column is always null — this is not axis-0 batching).

Output:
  datasets/manifests/Zhantao_C/entities.parquet
  datasets/manifests/Zhantao_C/artifacts.parquet
"""

import argparse
import re
from pathlib import Path

import h5py
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path("/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/Zhantao")
H5_FILENAME = "nips3_fwhm4_9dof_20000_20260303_0537.h5"
OUTPUT_DIR = REPO_ROOT / "datasets" / "manifests" / "Zhantao_C"

PARAM_NAMES = ["Ax", "Az", "J1a", "J1b", "J2a", "J2b", "J3a", "J3b", "J4"]
ARTIFACT_TYPES = [
    "data",
    "energies",
    "qs_rlu",
    "qs_lab",
    "powder_data",
    "powder_energies",
    "powder_qs_lab",
]

_SAMPLE_NUM_RE = re.compile(r"^sample_(\d+)$")


def _sample_index(key):
    m = _SAMPLE_NUM_RE.match(key)
    return int(m.group(1)) if m else None


def generate(output_dir=OUTPUT_DIR, n_entities=None):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    h5_path = DATA_DIR / H5_FILENAME

    ent_rows = []
    art_rows = []

    with h5py.File(h5_path, "r") as f:
        sample_keys = sorted(
            (k for k in f.keys() if _SAMPLE_NUM_RE.match(k)),
            key=_sample_index,
        )
        if n_entities is not None:
            sample_keys = sample_keys[:n_entities]

        for sample_key in sample_keys:
            grp = f[sample_key]
            params = {p: float(grp["params"][p][()]) for p in PARAM_NAMES}

            uid = sample_key  # e.g. "sample_42" — uid[:13] keeps numeric tail unique
            ent_rows.append({
                "uid": uid,
                "key": f"H_{uid}",
                **params,
            })

            for art_type in ARTIFACT_TYPES:
                art_rows.append({
                    "uid": uid,
                    "type": art_type,
                    "file": H5_FILENAME,
                    "dataset": f"/{sample_key}/{art_type}",
                    "index": pd.NA,
                })

    ent_df = pd.DataFrame(ent_rows)
    art_df = pd.DataFrame(art_rows)

    # Force `index` to a nullable integer dtype so Parquet round-trips a
    # well-typed column (instead of object) and pd.notna() in the broker
    # gives the right answer for missing values.
    art_df["index"] = art_df["index"].astype("Int64")

    ent_path = output_dir / "entities.parquet"
    art_path = output_dir / "artifacts.parquet"
    ent_df.to_parquet(ent_path, index=False)
    art_df.to_parquet(art_path, index=False)

    print(f"Wrote {len(ent_df)} entities → {ent_path}")
    print(f"Wrote {len(art_df)} artifacts → {art_path}")
    return ent_df, art_df


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "-n", "--n-entities", type=int, default=None,
        help="limit to first N entities (default: all 20,000)",
    )
    ap.add_argument(
        "-o", "--output-dir", type=Path, default=OUTPUT_DIR,
        help=f"output directory (default: {OUTPUT_DIR})",
    )
    args = ap.parse_args()
    generate(output_dir=args.output_dir, n_entities=args.n_entities)


if __name__ == "__main__":
    main()
