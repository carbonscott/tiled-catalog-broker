# Plan: Split VDP into Three Separate Datasets

**Date:** 2026-03-14
**Status:** Complete

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

## Implementation (completed 2026-03-14)

Generators were placed in `$DATA_BROKER_DIR/generators/` (not in the broker
repo) per the separation established in commit `d5acd61`.

### Files created in `$DATA_BROKER_DIR`

**Generators:**
- `generators/_vdp_common.py` -- shared logic (load raw manifests, rename
  columns, filter, transform)
- `generators/gen_genericspin_sunny_gs_manifest.py` -- filters to `gs_state`
- `generators/gen_genericspin_sunny_mh_manifest.py` -- filters to `mh_curve`
- `generators/gen_genericspin_sunny_ins_manifest.py` -- filters to `ins_powder`

**Dataset configs:**
- `datasets/genericspin_sunny_gs.yaml` -- key: `GenericSpin_Sunny_GS`, measurement: `ground_state`
- `datasets/genericspin_sunny_mh.yaml` -- key: `GenericSpin_Sunny_MH`, measurement: `magnetization`
- `datasets/genericspin_sunny_ins.yaml` -- key: `GenericSpin_Sunny_INS`, measurement: `inelastic_neutron_scattering`

**Generated manifests:**
- `manifests/genericspin_sunny_gs_{entities,artifacts}.parquet`
- `manifests/genericspin_sunny_mh_{entities,artifacts}.parquet`
- `manifests/genericspin_sunny_ins_{entities,artifacts}.parquet`

### Validation results

- **Entity match:** All 10,000 entities matched by UID; physics parameters
  (Ja, Jb, Jc, Dc, spin_s, g_factor) identical across old VDP and all three
  new datasets
- **Artifact match:** All 110,000 (uid, type, file, dataset) tuples match
  exactly between old VDP and the union of three new datasets
- **Database counts:** GS=10K/10K, MH=10K/80K, INS=10K/20K

### VDP removal

Old VDP container (id=1) removed via set-based SQLite deletes:
- Deleted 10,000 entities + 110,000 artifacts + associated data_sources
  and associations
- Zero orphaned assets (HDF5 files shared with new datasets)
- Closure table rebuilt (806,190 rows)

### Final catalog state (8 datasets)

| Dataset | Entities | Artifacts |
|---------|----------|-----------|
| GenericSpin_Sunny_GS | 10,000 | 10,000 |
| GenericSpin_Sunny_MH | 10,000 | 80,000 |
| GenericSpin_Sunny_INS | 10,000 | 20,000 |
| EDRIXS | 10,000 | 10,000 |
| NiPS3_Multimodal | 7,616 | 45,696 |
| RIXS | 7 | 42 |
| Challenge | 1 | 9 |
| SEQUOIA | 3 | 76 |

## Design Notes

- **Entity keys remain `H_{hash}`** -- no collision since they live under
  different parent containers
- **Same `base_dir`** for all three -- the `readable_storage` in production
  `config.yml` already includes the VDP data path
- **Generator output filename must match config YAML stem** -- the CLI
  resolves manifests as `{stem}_entities.parquet`
- **Shared helper `_vdp_common.py`** avoids code duplication across three
  generators while keeping each generator a standalone file with the
  standard `generate()` interface
