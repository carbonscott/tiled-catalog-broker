"""Write a small synthetic HDF5 dataset, for following the tutorial without your own data.

Produces the `per_entity` layout: one file per entity, that entity's parameters as
scalar datasets at the file root, plus two artifact arrays and a shared axis.

    python examples/make_demo_dataset.py            # 12 entities into ./demo-data
    python examples/make_demo_dataset.py -n 50 -o /tmp/mydata

Each file looks like:

    entity_0001.h5
    ├── /spectrum   (64, 16) float32   ← artifact
    ├── /curve      (64,)    float32   ← artifact
    ├── /energies   (64,)    float64   ← shared axis (identical in every file)
    ├── /sigma      scalar            ┐
    ├── /gamma      scalar            │ parameters, queryable in the catalog
    └── /tenDq      scalar            ┘
"""

import argparse
from pathlib import Path

import h5py
import numpy as np


def make_dataset(out_dir, n_entities=12, seed=0):
    """Write `n_entities` HDF5 files into `out_dir`. Returns the list of paths."""
    rng = np.random.default_rng(seed)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    energies = np.linspace(0.0, 8.0, 64)
    written = []

    for i in range(n_entities):
        # Parameters vary per entity; the arrays are a smooth function of them, so a
        # query on sigma picks out visibly different spectra.
        sigma = float(rng.uniform(0.02, 0.08))
        gamma = float(rng.choice([0.1, 0.2, 0.4]))
        ten_dq = float(rng.uniform(1.0, 2.5))

        peak = np.exp(-((energies - ten_dq) ** 2) / (2 * sigma**2 + 0.05))
        angles = np.linspace(0, np.pi, 16)
        spectrum = np.outer(peak, np.cos(angles) ** 2 + gamma).astype(np.float32)
        curve = (peak * (1.0 - gamma)).astype(np.float32)

        path = out_dir / f"entity_{i + 1:04d}.h5"
        with h5py.File(path, "w") as f:
            f["spectrum"] = spectrum
            f["curve"] = curve
            f["energies"] = energies
            f["sigma"] = sigma
            f["gamma"] = gamma
            f["tenDq"] = ten_dq
        written.append(path)

    return written


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-n", "--n-entities", type=int, default=12)
    parser.add_argument("-o", "--out-dir", default="demo-data")
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    written = make_dataset(args.out_dir, args.n_entities, args.seed)
    print(f"Wrote {len(written)} files to {Path(args.out_dir).resolve()}")
    print(f"  e.g. {written[0].name}: /spectrum (64, 16), /curve (64,), "
          "params sigma, gamma, tenDq")


if __name__ == "__main__":
    main()
