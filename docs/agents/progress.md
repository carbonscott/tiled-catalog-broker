# Progress Log

## 2026-03-15: Register into centralized Tiled server (PostgreSQL)

**Branch:** `feature/centralized-catalog`
**Based on:** `refactor/remove-generators`

### Goal

Register all 8 datasets into the centralized Tiled server at
`https://lcls-data-portal.slac.stanford.edu/tiled-dev`, which uses PostgreSQL
(instead of our local SQLite `catalog.db`). This gives us JSONB-backed metadata
queries and a shared, always-on catalog for the team.

**Datasets to register (8 total):**

| Dataset | Entities | Artifacts | Source |
|---------|----------|-----------|--------|
| GenericSpin_Sunny_GS | 10,000 | ground states | Sunny.jl |
| GenericSpin_Sunny_MH | 10,000 | magnetization | Sunny.jl |
| GenericSpin_Sunny_INS | 10,000 | INS spectra | Sunny.jl |
| EDRIXS | 10,000 | RIXS spectra | EDRIXS |
| NiPS3_Multimodal | 7,616 | multi-modal | Synthetic |
| RIXS | 7 | experimental | LCLS/qRIXS |
| Challenge | 1 | benchmark | - |
| SEQUOIA | 3 | neutron | SNS/SEQUOIA |

### Research findings

**1. Tiled version mismatch (not blocking)**
- Local: Tiled 0.2.8
- Server: Tiled 0.2.3
- Client 0.2.8 can create containers, register arrays, and query metadata on
  the 0.2.3 server. One known issue: `client.delete()` fails due to a parameter
  mismatch, but the REST API `DELETE ...?recursive=true` works.

**2. PostgreSQL is transparent to HTTP clients**
- Our `register.py` (HTTP registration) path uses the Tiled client API, which
  is database-agnostic. No code changes needed for the PostgreSQL backend
  itself.
- Our `ingest.py` (bulk SQL) path is SQLite-specific (raw SQL, triggers,
  `INSERT OR IGNORE`). This path won't work and isn't needed -- HTTP
  registration is the correct approach for a remote server.

**3. Filesystem path mismatch (requires fix)**
- The centralized server runs in a Kubernetes pod where the S3DF filesystem is
  mounted at a different path:
  - S3DF (local): `/sdf/data/lcls/ds/prj/prjmaiqmag01/results/...`
  - K8s pod (server): `/prjmaiqmag01/...`
- When registering arrays, the `data_uri` must use the pod path so the server
  can find the HDF5 files.
- Currently, `base_dir` in the dataset YAML serves double duty:
  (a) local filesystem path for reading HDF5 shapes during registration, and
  (b) path embedded in `data_uri` for the server.
- These two uses must be decoupled.

**4. Server already has stale data**
- Root level contains flat `H_*` entity keys (old VDP test data) plus sample
  files (`a`, `b`, `c`, `tables`, etc.).
- All existing array reads return 500 -- the registered `data_uri` paths
  use S3DF paths that don't exist inside the pod.

### Approach

**Code change (~7 lines):** Add optional `server_base_dir` to dataset YAML
configs. When present, it is used for building `data_uri` in assets; `base_dir`
continues to be used for local HDF5 reads. Fully backward-compatible.

Files to modify:
- `tiled_poc/broker/http_register.py` -- `create_data_source()` accepts
  `server_base_dir` param, uses it for `data_uri` construction
- `tiled_poc/broker/http_register.py` -- `register_dataset_http()` passes
  `server_base_dir` through
- `tiled_poc/broker/cli.py` -- `register_main()` reads `server_base_dir`
  from config

**Config change:** New dataset YAMLs with `server_base_dir` field:
```yaml
key: GenericSpin_Sunny_GS
base_dir: /sdf/data/lcls/ds/prj/prjmaiqmag01/results/vdp/data/schema_v1
server_base_dir: /prjmaiqmag01/vdp/data/schema_v1
metadata:
  organization: MAIQMag
  ...
```

**Trial plan:** Register 2-3 entities per dataset, verify metadata queries
work, verify array reads work (after path fix), then compare with catalog.db.

### Tasks

- [x] Explore `$DATA_BROKER_DIR` structure and catalog.db
- [x] Review ingestion code for SQLite-specific patterns
- [x] Research Tiled PostgreSQL support and version compatibility
- [x] Create branch
- [x] Implement `server_base_dir` support
- [x] Trial registration of all 8 datasets (2-3 entities each)
- [x] Compare with existing catalog.db
- [x] Report findings

## 2026-03-14: Remove generators and demo from broker repo

**Branch:** `refactor/remove-generators`
**Commit:** `d5acd61`

### What changed

Separated dataset-specific manifest generators from the generic broker
library. The Parquet manifest is now the sole API boundary between data
preparation and the broker.

**Deleted (876 lines removed):**
- `tiled_poc/generate.py` -- CLI wrapper for running generators
- `tiled_poc/extra/` -- 3 dataset-specific generator scripts
  (gen_vdp_manifest.py, gen_edrixs_manifest.py, gen_multimodal_manifest.py)
- `tiled_poc/demo/` -- self-contained demo directory (config.yml, explore.py,
  3 dataset YAML configs)

**Modified:**
- `tiled_poc/broker/cli.py` -- removed `generate_main()` function and
  `import importlib`; updated docstring from "three commands" to "two commands"
- `tiled_poc/pyproject.toml` -- removed `broker-generate` entry point
- `CLAUDE.md` -- updated directory structure and usage examples
- `tiled_poc/README.md` -- rewrote quickstart, workflow overview, "Adding
  Your Own Dataset" section, and directory listing
- `docs/INGESTION-GUIDE.md` -- reframed Steps 4-5 so generators are the
  dataset owner's responsibility; removed `extra/` references
- `docs/SCHEMA-DESIGN.md` -- removed `generator:` field from YAML examples
  and `generators/*.py` row from impact table
- `.gitignore` -- updated paths (demo/ -> top-level manifests/storage/)
- `tiled_poc/tests/test_config.py` -- updated skip reason

### Why

Generators are inherently dataset-specific ETL (hard-coded source paths,
column mappings, HDF5 dataset paths). They don't belong in a reusable
library. The application directory (`$DATA_BROKER_DIR`) already had its own
`generators/` directory with 6 production generators, making the 3 in
`extra/` redundant.

The `lcls-data-broker` fork had already made this separation successfully.

### New workflow

```
(dataset owner writes generator) -> manifests/ -> ingest.py -> catalog.db -> tiled serve
                                                  register.py -> running server
```

The broker's public CLI surface is now just `broker-ingest` and
`broker-register`. How manifests are produced is the dataset owner's concern.

### Verification

- All 47 unit tests pass (1 skipped -- requires pre-built manifests)
- `broker-ingest` and `broker-register` CLI entry points import correctly
