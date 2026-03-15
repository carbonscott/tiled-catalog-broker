# Plan: Split VDP into Three Separate Datasets

**Date:** 2026-03-14
**Status:** Approved, ready for implementation

## Background

The VDP dataset currently lives as a single top-level container in the catalog
with 10,000 entities and 110,000 artifacts. Investigation revealed that it
contains three physically distinct data products:

| Category | Artifact Types | Count | HDF5 Path | Shape | Physics |
|----------|---------------|-------|-----------|-------|---------|
| Ground state | `gs_state` | 10K | `/gs/spin_dir` | (3, 8) | Spin configuration |
| Magnetization curves | `mh_{x,y,z,powder}_{7,30}T` | 80K | `/curve/M_parallel` | (200,) | M(H) curves |
| INS spectra | `ins_{12,25}meV` | 20K | `/ins/broadened` | (600, 400) | Inelastic neutron scattering |

These differ in physical observable, array shape, HDF5 internal path, and
simulation parameters.

## Key Finding: Generic Spin Model, Not NiPS3

Investigation of the Julia source code (`generate_synthetic_lhs.jl`) confirmed:

- **Lattice:** Simple orthorhombic (a=4.1, b=4.2, c=4.3 A, all angles 90 deg)
  with one atom per unit cell
- **Site type:** `"Fe"` (not Ni)
- **Spin:** S=5/2, g=2.0 (consistent with Fe3+, not Ni2+)
- **Spacegroup:** 47 (Pmmm), not monoclinic C2/m like NiPS3
- **Parameters:** Ja, Jb, Jc, Dc swept uniformly over [-1, 1] meV via Latin
  Hypercube Sampling -- a generic parameter scan
- **Existing metadata** already says `material: generic spin model`

VDP is a coworker's name (the person who ran the simulations), not a material.

## Database vs. Code Separation (Confirmed)

- **Source code:** This repo (`tiled-catalog-broker`)
- **Production database + manifests:** `$DATA_BROKER_DIR`
  (`/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/cwang31-data-broker/`)
  - `catalog.db` (437 MB)
  - `manifests/` (Parquet files for all 6 datasets)
  - `datasets/` (YAML configs)
  - Separate git repo
- **VDP raw data:** `/sdf/data/lcls/ds/prj/prjmaiqmag01/results/vdp/data/schema_v1/`
  - `artifacts/` -- 110K HDF5 files
  - `manifest_hamiltonians_*.parquet` and `manifest_artifacts_*.parquet`

## New Dataset Names

| Old | New |
|-----|-----|
| VDP (single dataset, 110K artifacts) | `GenericSpin_Sunny_GS` (10K ground states) |
| | `GenericSpin_Sunny_MH` (80K magnetization curves) |
| | `GenericSpin_Sunny_INS` (20K INS spectra) |

## Implementation Plan

### Step 1: Create Three Manifest Generators

New files in `tiled_poc/extra/`:
- `gen_genericspin_sunny_gs_manifest.py` -- filters to `gs_state` only
- `gen_genericspin_sunny_mh_manifest.py` -- filters to `mh_*` (8 per entity)
- `gen_genericspin_sunny_ins_manifest.py` -- filters to `ins_*` (2 per entity)

Each generator:
- Reads the same raw VDP Parquet manifests
- Filters artifacts by type
- Writes `{name}_entities.parquet` and `{name}_artifacts.parquet`

### Step 2: Create Dataset YAML Configs

New files in `$DATA_BROKER_DIR/datasets/`:
- `genericspin_sunny_gs.yaml`
- `genericspin_sunny_mh.yaml`
- `genericspin_sunny_ins.yaml`

All share:
- `base_dir: /sdf/.../vdp/data/schema_v1`
- `metadata.organization: MAIQMag`
- `metadata.material: generic spin model`
- `metadata.producer: Sunny.jl`

Differentiated by `metadata.data_type` and `metadata.measurement`.

### Step 3: Generate Manifests

Run `generate.py` for each config to produce 6 Parquet files in
`$DATA_BROKER_DIR/manifests/`.

### Step 4: Ingest into Production catalog.db

Run `ingest.py` for each config. The existing `ensure_catalog()` connects to
the DB without reinitializing. New dataset containers are added alongside
existing ones.

### Step 5: Validate

For each new dataset:
1. Match entities by physics parameters (Ja, Jb, Jc, Dc, spin_s, g_factor)
   to old VDP entities
2. Confirm artifact arrays are identical (same file path, same HDF5 dataset)

### Step 6: Remove Old VDP

Direct SQLite operations on `catalog.db`:
1. Find VDP container node
2. Delete all descendant nodes (entities + artifacts)
3. Delete associated data_sources, assets, closure table entries
4. Rebuild closure table
5. Delete VDP container node

### Step 7: Update Configs

- Add new dataset YAML configs to demo (`tiled_poc/demo/datasets/`)
- Remove or update old `vdp.yml` / `vdp.yaml`

## Design Notes

- **Entity keys remain `H_{hash}`** -- no collision since they live under
  different parent containers
- **Same `base_dir`** for all three -- the `readable_storage` in production
  `config.yml` already includes the VDP data path
- **Generator output filename must match config YAML stem** -- the CLI
  resolves manifests as `{stem}_entities.parquet`
