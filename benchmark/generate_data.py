"""
generate_data.py
================
Generates synthetic MAIQMag catalog data and inserts identical logical records
into all four schemas (A, B, C, D).

The 6 datasets mirror the real MAIQMag catalog structure exactly:
  - VDP          : per-entity, 110K files, hex-sharded, parquet manifest, 4 free + 2 fixed params
  - NiPS3        : per-entity, 7.6K files, root HDF5 scalars, 9 params
  - EDRIXS_Sam   : batched (5 files x 2K), /params/ group in HDF5, 12 params
  - EDRIXS_Tlink : batched (2 files x 10K), /params/ group in HDF5, 12 params
  - SUNNY_10K    : monolithic (1 file, 10K), /params/ group, 9 params
  - SUNNY_EXP    : per-entity-in-groups (12.8K groups), per-group scalars, 9 params

Usage:
    python generate_data.py --dsn "postgresql://user:pass@localhost/bench"

Scales (controlled by benchmark_runner.py):
    Small  : --entities 100  --artifacts 10
    Medium : --entities 1000 --artifacts 10
    Large  : --entities 10000 --artifacts 50
"""

import argparse
import json
import random
import time
import uuid
from typing import Any

import numpy as np
import psycopg2
import psycopg2.extras

RNG = np.random.default_rng(42)
random.seed(42)

# ---------------------------------------------------------------------------
# Dataset catalogue — 6 real MAIQMag datasets
# ---------------------------------------------------------------------------
# Each dataset carries:
#   name, dtype, material, producer, facility, instrument,
#   layout         : how files are organised on disk
#   params_location: where params live in the file
#   file_format    : HDF5 / CSV / NetCDF
#   size_gb        : approximate on-disk size
#   n_files        : approximate file count (for documentation)
#   param_schema   : key used to pick the right param generator below
# ---------------------------------------------------------------------------
DATASETS = [
    {
        "name":            "VDP",
        "dtype":           "simulation",
        "material":        "NiPS3",
        "producer":        "MAIQ_team",
        "facility":        None,
        "instrument":      "VDP_solver",
        "layout":          "per-entity",
        "params_location": "external_parquet_manifest",
        "file_format":     "HDF5",
        "size_gb":         112.0,
        "n_files":         110_000,
        "param_schema":    "vdp",
    },
    {
        "name":            "NiPS3_multimodal",
        "dtype":           "experimental",
        "material":        "NiPS3",
        "producer":        "MAIQ_team",
        "facility":        None,
        "instrument":      "multimodal",
        "layout":          "per-entity",
        "params_location": "root_scalars_hdf5",
        "file_format":     "HDF5",
        "size_gb":         24.0,
        "n_files":         7_600,
        "param_schema":    "nips3",
    },
    {
        "name":            "EDRIXS_Sam",
        "dtype":           "simulation",
        "material":        "NiPS3",
        "producer":        "Sam",
        "facility":        None,
        "instrument":      "EDRIXS_solver",
        "layout":          "batched",
        "params_location": "params_group_hdf5",
        "file_format":     "HDF5",
        "size_gb":         0.445,
        "n_files":         5,
        "param_schema":    "edrixs",
    },
    {
        "name":            "EDRIXS_Tlinker",
        "dtype":           "simulation",
        "material":        "NiPS3",
        "producer":        "Tlinker",
        "facility":        None,
        "instrument":      "EDRIXS_solver",
        "layout":          "batched",
        "params_location": "params_group_hdf5",
        "file_format":     "HDF5",
        "size_gb":         0.924,
        "n_files":         2,
        "param_schema":    "edrixs",
    },
    {
        "name":            "SUNNY_10K",
        "dtype":           "simulation",
        "material":        "NiPS3",
        "producer":        "MAIQ_team",
        "facility":        None,
        "instrument":      "SUNNY_solver",
        "layout":          "monolithic",
        "params_location": "params_group_hdf5",
        "file_format":     "HDF5",
        "size_gb":         11.0,
        "n_files":         1,
        "param_schema":    "sunny",
    },
    {
        "name":            "SUNNY_EXP_mesh",
        "dtype":           "simulation",
        "material":        "NiPS3",
        "producer":        "MAIQ_team",
        "facility":        None,
        "instrument":      "SUNNY_solver",
        "layout":          "per-entity-in-groups",
        "params_location": "per_group_scalars_hdf5",
        "file_format":     "HDF5",
        "size_gb":         90.0,
        "n_files":         1,
        "param_schema":    "sunny",
    },
]

# ---------------------------------------------------------------------------
# Artifact definitions — per dataset, exactly as in the real catalog
# ---------------------------------------------------------------------------
DATASET_ARTIFACTS = {
    # VDP: gs_state + 8 mh_curves + 2 ins_powder = 11 artifacts
    # Shared axes inside each file: H_T, q_Ainv, hw_meV
    "VDP": [
        {"name": "gs_state",      "shape": [384, 384],   "axes": ["q_Ainv", "hw_meV"]},
        {"name": "mh_curve_0T",   "shape": [200],        "axes": ["H_T"]},
        {"name": "mh_curve_5T",   "shape": [200],        "axes": ["H_T"]},
        {"name": "mh_curve_10T",  "shape": [200],        "axes": ["H_T"]},
        {"name": "mh_curve_15T",  "shape": [200],        "axes": ["H_T"]},
        {"name": "mh_curve_20T",  "shape": [200],        "axes": ["H_T"]},
        {"name": "mh_curve_25T",  "shape": [200],        "axes": ["H_T"]},
        {"name": "mh_curve_30T",  "shape": [200],        "axes": ["H_T"]},
        {"name": "mh_curve_35T",  "shape": [200],        "axes": ["H_T"]},
        {"name": "ins_powder_low_E",  "shape": [512, 256], "axes": ["q_Ainv", "hw_meV"]},
        {"name": "ins_powder_high_E", "shape": [512, 256], "axes": ["q_Ainv", "hw_meV"]},
    ],
    # NiPS3: hisym + powder + powder_mask + Ma + Mb + Mcs = 6 artifacts
    # Shared axes: H, energies, energies_powder, qs, radii
    "NiPS3_multimodal": [
        {"name": "hisym",        "shape": [300, 150],   "axes": ["H", "energies"]},
        {"name": "powder",       "shape": [512, 200],   "axes": ["qs", "energies_powder"]},
        {"name": "powder_mask",  "shape": [512, 200],   "axes": ["qs", "energies_powder"]},
        {"name": "Ma",           "shape": [100],        "axes": ["radii"]},
        {"name": "Mb",           "shape": [100],        "axes": ["radii"]},
        {"name": "Mcs",          "shape": [100],        "axes": ["radii"]},
    ],
    # EDRIXS: spectra (151×40) per entity. Shared axes: eloss, omega_bounds
    "EDRIXS_Sam": [
        {"name": "spectra", "shape": [151, 40], "axes": ["eloss", "omega_bounds"]},
    ],
    "EDRIXS_Tlinker": [
        {"name": "spectra", "shape": [151, 40], "axes": ["eloss", "omega_bounds"]},
    ],
    # SUNNY: data (384×384). Shared axes: E_axis, Q_axis
    "SUNNY_10K": [
        {"name": "data", "shape": [384, 384], "axes": ["Q_axis", "E_axis"]},
    ],
    "SUNNY_EXP_mesh": [
        {"name": "data", "shape": [2100, 450], "axes": ["HKL", "energies"]},
    ],
}

# ---------------------------------------------------------------------------
# Parameter generators — one per param_schema
# ---------------------------------------------------------------------------

def _uid() -> str:
    return uuid.uuid4().hex[:8]


def gen_vdp_params() -> dict[str, Any]:
    """
    VDP simulation: 4 free + 2 fixed Hamiltonian params.
    Free:  Ja, Jb, D, g_factor
    Fixed: spin_s=1.0, material=NiPS3 (captured at dataset level)
    Shared axes inside each file: H_T, q_Ainv, hw_meV
    """
    return {
        "Ja_mev":   round(float(RNG.uniform(0.5, 15.0)), 4),   # nearest-neighbour exchange
        "Jb_mev":   round(float(RNG.uniform(0.0, 5.0)),  4),   # next-nearest exchange
        "D_mev":    round(float(RNG.uniform(0.0, 2.0)),  4),   # single-ion anisotropy
        "g_factor": round(float(RNG.uniform(1.8, 2.5)),  4),   # Landé g-factor
        # fixed
        "spin_s":   1.0,
        "material_param": "NiPS3",
        # shared axes ranges (stored as metadata for catalog queries)
        "H_T_max":    round(float(RNG.uniform(20.0, 45.0)), 1),
        "q_Ainv_max": round(float(RNG.uniform(2.0, 6.0)),   3),
        "hw_meV_max": round(float(RNG.uniform(50.0, 200.0)), 1),
    }


def gen_nips3_params() -> dict[str, Any]:
    """
    NiPS3 multimodal: 9 params stored as root scalars in HDF5.
    Shared axes: H, energies, energies_powder, qs, radii
    """
    return {
        "Ja_mev":       round(float(RNG.uniform(1.0, 12.0)),  4),
        "Jb_mev":       round(float(RNG.uniform(0.0, 4.0)),   4),
        "Jc_mev":       round(float(RNG.uniform(0.0, 2.0)),   4),
        "D_mev":        round(float(RNG.uniform(0.0, 1.5)),   4),
        "Gamma_mev":    round(float(RNG.uniform(0.0, 0.5)),   4),
        "spin_s":       1.0,
        "g_factor":     round(float(RNG.uniform(1.8, 2.5)),   4),
        "temperature_K":round(float(RNG.uniform(1.5, 300.0)), 2),
        "field_T":      round(float(RNG.uniform(0.0, 14.0)),  2),
    }


def gen_edrixs_params() -> dict[str, Any]:
    """
    EDRIXS (Sam & Tlinker): 12 params stored in /params/ HDF5 group.
    Artifact: spectra (151×40). Shared axes: eloss, omega_bounds
    """
    return {
        "Udd":          round(float(RNG.uniform(5.0, 9.0)),   3),   # Coulomb repulsion d-d
        "Upd":          round(float(RNG.uniform(6.0, 10.0)),  3),   # Coulomb repulsion p-d
        "Delta":        round(float(RNG.uniform(2.0, 6.0)),   3),   # charge transfer energy
        "crystal_10Dq": round(float(RNG.uniform(0.5, 2.5)),   3),   # crystal field splitting
        "zeta_d":       round(float(RNG.uniform(0.05, 0.15)), 4),   # d spin-orbit coupling
        "zeta_p":       round(float(RNG.uniform(5.0, 12.0)),  3),   # p spin-orbit coupling
        "Ds":           round(float(RNG.uniform(-0.2, 0.2)),  4),   # tetragonal distortion
        "Dt":           round(float(RNG.uniform(-0.1, 0.1)),  4),   # tetragonal distortion
        "temperature_K":round(float(RNG.uniform(1.5, 300.0)), 2),
        "incident_energy_eV": round(float(RNG.uniform(850.0, 870.0)), 3),
        "eloss_min_eV": round(float(RNG.uniform(-2.0, 0.0)),  3),
        "eloss_max_eV": round(float(RNG.uniform(5.0, 15.0)),  3),
    }


def gen_sunny_params() -> dict[str, Any]:
    """
    SUNNY 10K and EXP mesh: 9 params in /params/ group.
    Artifacts: data (384×384 or 2100×450). Shared axes: E_axis, Q_axis / HKL, energies
    """
    return {
        "Ja_mev":        round(float(RNG.uniform(0.5, 15.0)), 4),
        "Jb_mev":        round(float(RNG.uniform(0.0, 5.0)),  4),
        "Jc_mev":        round(float(RNG.uniform(0.0, 2.0)),  4),
        "D_mev":         round(float(RNG.uniform(0.0, 2.0)),  4),
        "Gamma_mev":     round(float(RNG.uniform(0.0, 0.5)),  4),
        "spin_s":        1.0,
        "g_factor":      round(float(RNG.uniform(1.8, 2.5)),  4),
        "temperature_K": round(float(RNG.uniform(1.5, 300.0)), 2),
        "broadening_mev":round(float(RNG.uniform(0.1, 5.0)),  3),
    }


PARAM_GENERATORS = {
    "vdp":      gen_vdp_params,
    "nips3":    gen_nips3_params,
    "edrixs":   gen_edrixs_params,
    "sunny":    gen_sunny_params,
}


def gen_params(dataset: dict) -> dict[str, Any]:
    return PARAM_GENERATORS[dataset["param_schema"]]()


def jitter_shape(base_shape: list[int]) -> list[int]:
    """Add small random variation to array shapes (±5%)."""
    return [max(1, int(s * RNG.uniform(0.95, 1.05))) for s in base_shape]


def make_data_ref(dataset_name: str, entity_name: str, artifact_name: str, file_format: str) -> str:
    ext = {"HDF5": "h5", "CSV": "csv", "NetCDF": "nc"}.get(file_format, "bin")
    return f"gs://maiqmag/{dataset_name}/{entity_name}/{artifact_name}.{ext}"


# ---------------------------------------------------------------------------
# Unified superset of all param keys across all datasets (for native schemas A/B)
# Every entity gets all columns; missing params are NULL.
# ---------------------------------------------------------------------------
ALL_PARAM_KEYS = sorted({
    k for gen in PARAM_GENERATORS.values()
    for k in gen().keys()
})


def params_to_native(params: dict) -> dict:
    """Return params dict with None for any key not present in this entity."""
    return {k: params.get(k) for k in ALL_PARAM_KEYS}


def insert_schema_a(cur, datasets_meta: list[dict], entities: list[dict]) -> None:
    """Schema A — hierarchical + native columns (superset of all param keys)."""
    ds_id_map: dict[str, int] = {}

    for ds in datasets_meta:
        cur.execute(
            """INSERT INTO a_datasets
               (name, dtype, material, producer, facility, instrument,
                layout, params_location, file_format, size_gb)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (ds["name"], ds["dtype"], ds["material"], ds["producer"],
             ds["facility"], ds["instrument"],
             ds["layout"], ds["params_location"], ds["file_format"], ds["size_gb"]),
        )
        ds_id_map[ds["name"]] = cur.fetchone()[0]

    col_list = ", ".join(ALL_PARAM_KEYS)
    placeholders = ", ".join(["%s"] * len(ALL_PARAM_KEYS))

    for ent in entities:
        p = params_to_native(ent["params"])
        vals = [p[k] for k in ALL_PARAM_KEYS]
        cur.execute(
            f"""INSERT INTO a_entities (dataset_id, name, {col_list})
                VALUES (%s, %s, {placeholders}) RETURNING id""",
            [ds_id_map[ent["dataset_name"]], ent["entity_name"]] + vals,
        )
        ent_id = cur.fetchone()[0]
        for art in ent["artifacts"]:
            cur.execute(
                """INSERT INTO a_artifacts (entity_id, name, array_shape, shared_axes, data_ref)
                   VALUES (%s,%s,%s,%s,%s)""",
                (ent_id, art["name"], art["array_shape"], art["shared_axes"], art["data_ref"]),
            )


def insert_schema_b(cur, datasets_meta: list[dict], entities: list[dict]) -> None:
    """Schema B — flat + native columns."""
    ds_map = {ds["name"]: ds for ds in datasets_meta}
    rows = []
    for ent in entities:
        ds = ds_map[ent["dataset_name"]]
        p = params_to_native(ent["params"])
        for art in ent["artifacts"]:
            row = [
                ds["name"], ds["dtype"], ds["material"], ds["producer"],
                ds["facility"], ds["instrument"],
                ds["layout"], ds["params_location"], ds["file_format"], ds["size_gb"],
                ent["entity_name"],
            ] + [p[k] for k in ALL_PARAM_KEYS] + [
                art["name"], art["array_shape"], art["shared_axes"], art["data_ref"],
            ]
            rows.append(tuple(row))

    ds_cols = "dataset_name,dtype,material,producer,facility,instrument,layout,params_location,file_format,size_gb"
    param_cols = ",".join(ALL_PARAM_KEYS)
    art_cols = "artifact_name,array_shape,shared_axes,data_ref"
    psycopg2.extras.execute_values(
        cur,
        f"INSERT INTO b_flat (entity_name,{ds_cols},{param_cols},{art_cols}) VALUES %s",
        [(r[10], *r[:10], *r[11:]) for r in rows],  # reorder: entity_name first
        page_size=500,
    )


def insert_schema_c(cur, datasets_meta: list[dict], entities: list[dict]) -> None:
    """Schema C — hierarchical + JSONB (current design)."""
    ds_id_map: dict[str, int] = {}
    for ds in datasets_meta:
        meta = {k: v for k, v in ds.items()
                if k not in ("name",) and v is not None}
        cur.execute(
            "INSERT INTO c_datasets (name, meta) VALUES (%s,%s) RETURNING id",
            (ds["name"], json.dumps(meta)),
        )
        ds_id_map[ds["name"]] = cur.fetchone()[0]

    for ent in entities:
        meta = {k: v for k, v in ent["params"].items() if v is not None}
        cur.execute(
            "INSERT INTO c_entities (dataset_id, name, meta) VALUES (%s,%s,%s) RETURNING id",
            (ds_id_map[ent["dataset_name"]], ent["entity_name"], json.dumps(meta)),
        )
        ent_id = cur.fetchone()[0]
        for art in ent["artifacts"]:
            cur.execute(
                """INSERT INTO c_artifacts (entity_id, name, array_shape, shared_axes, data_ref)
                   VALUES (%s,%s,%s,%s,%s)""",
                (ent_id, art["name"], art["array_shape"], art["shared_axes"], art["data_ref"]),
            )


def insert_schema_d(cur, datasets_meta: list[dict], entities: list[dict]) -> None:
    """Schema D — flat + JSONB."""
    ds_map = {ds["name"]: ds for ds in datasets_meta}
    rows = []
    for ent in entities:
        ds = ds_map[ent["dataset_name"]]
        ds_meta = json.dumps({k: v for k, v in ds.items()
                               if k not in ("name",) and v is not None})
        ent_meta = json.dumps({k: v for k, v in ent["params"].items() if v is not None})
        for art in ent["artifacts"]:
            rows.append((
                ds["name"], ds_meta,
                ent["entity_name"], ent_meta,
                art["name"], art["array_shape"], art["shared_axes"], art["data_ref"],
            ))

    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO d_flat
           (dataset_name,dataset_meta,entity_name,entity_meta,
            artifact_name,array_shape,shared_axes,data_ref)
           VALUES %s""",
        rows,
        page_size=500,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_entities(n_entities: int, n_artifacts_override: int | None = None) -> list[dict]:
    """
    Generate entity + artifact records distributed proportionally across all 8 datasets.

    Artifacts per entity follow the real catalog structure by default
    (e.g. VDP always has 11, NiPS3 always has 6, etc.).
    Pass n_artifacts_override to cap or expand artifact count for stress-testing.
    """
    entities = []
    per_ds = n_entities // len(DATASETS)
    remainder = n_entities % len(DATASETS)

    for i, ds in enumerate(DATASETS):
        count = per_ds + (1 if i < remainder else 0)
        canonical_arts = DATASET_ARTIFACTS[ds["name"]]

        for _ in range(count):
            uid = _uid()
            entity_name = f"E_{uid}"
            params = gen_params(ds)

            # Use real artifact list; override count only if requested
            if n_artifacts_override is not None:
                # repeat or truncate canonical list to reach the override count
                art_pool = (canonical_arts * (n_artifacts_override // len(canonical_arts) + 1))
                art_defs = art_pool[:n_artifacts_override]
            else:
                art_defs = canonical_arts

            artifacts = []
            for art in art_defs:
                artifacts.append({
                    "name":        art["name"],
                    "array_shape": jitter_shape(art["shape"]),
                    "shared_axes": art["axes"],           # stored as JSONB extra / text[]
                    "data_ref":    make_data_ref(ds["name"], entity_name, art["name"], ds["file_format"]),
                })

            entities.append({
                "dataset_name": ds["name"],
                "entity_name":  entity_name,
                "params":       params,
                "artifacts":    artifacts,
            })
    return entities


def run(dsn: str, n_entities: int, n_artifacts: int | None, schemas: list[str], verbose: bool = True) -> dict:
    """Populate selected schemas and return insertion timing."""
    entities = build_entities(n_entities, n_artifacts_override=n_artifacts if n_artifacts else None)
    total_artifacts = sum(len(e["artifacts"]) for e in entities)

    if verbose:
        print(f"Generated {len(entities)} entities → {total_artifacts} artifact rows across {len(DATASETS)} datasets")

    timings = {}
    conn = psycopg2.connect(dsn)
    conn.autocommit = False

    #Update this path to point to the directory containing the SQL schema files (schema_a.sql, schema_b.sql, etc.)
    schema_sql = {
        "A": "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/benchmark_json/benchmark/schema_a.sql",
        "B": "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/benchmark_json/benchmark/schema_b.sql",
        "C": "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/benchmark_json/benchmark/schema_c.sql",
        "D": "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/benchmark_json/benchmark/schema_d.sql",
    }
    inserters = {
        "A": insert_schema_a,
        "B": insert_schema_b,
        "C": insert_schema_c,
        "D": insert_schema_d,
    }

    with conn:
        with conn.cursor() as cur:
            for schema in schemas:
                if verbose:
                    print(f"  Creating schema {schema}...", end=" ", flush=True)
                with open(schema_sql[schema]) as f:
                    cur.execute(f.read())
                conn.commit()

                if verbose:
                    print(f"inserting...", end=" ", flush=True)
                t0 = time.perf_counter()
                inserters[schema](cur, DATASETS, entities)
                conn.commit()
                elapsed = time.perf_counter() - t0
                timings[schema] = elapsed
                if verbose:
                    print(f"done ({elapsed:.2f}s)")

    # VACUUM ANALYZE
    conn.autocommit = True
    with conn.cursor() as cur:
        for schema in schemas:
            tables = {
                "A": ["a_datasets", "a_entities", "a_artifacts"],
                "B": ["b_flat"],
                "C": ["c_datasets", "c_entities", "c_artifacts"],
                "D": ["d_flat"],
            }[schema]
            for t in tables:
                if verbose:
                    print(f"  VACUUM ANALYZE {t}...")
                cur.execute(f"VACUUM ANALYZE {t}")
    conn.close()
    return timings


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate MAIQMag benchmark schemas A–D")
    parser.add_argument("--dsn", required=True, help="PostgreSQL DSN e.g. postgresql://user:pass@localhost/bench")
    parser.add_argument("--entities", type=int, default=1000, help="Total entities across all datasets (default 1000)")
    parser.add_argument("--artifacts", type=int, default=10, help="Artifacts per entity (default 10)")
    parser.add_argument("--schemas", nargs="+", default=["A","B","C","D"], choices=["A","B","C","D"],
                        help="Which schemas to populate (default: all)")
    args = parser.parse_args()

    timings = run(args.dsn, args.entities, args.artifacts, args.schemas)
    print("\nInsertion timings:")
    for s, t in timings.items():
        print(f"  Schema {s}: {t:.3f}s")
