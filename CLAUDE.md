## Environment Setup

Set these environment variables before running any commands:

```bash
export PROJ_DIR=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cwang31/codes/tiled-catalog-broker
export DATA_BROKER_DIR=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/cwang31-data-broker
export UV_CACHE_DIR=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cwang31/.UV_CACHE
```

Use `uv` to run python programs. The UV_CACHE_DIR avoids repeated package downloads.

## Project Overview

**Tiled Catalog Broker** — a config-driven system for registering multi-modal
scientific HDF5 datasets into a [Tiled](https://blueskyproject.io/tiled/)
catalog. Data model inspired by [ArrayLake](https://docs.earthmover.io/concepts/data-model)
(Organization → Repo → Group → Array), adapted for many-entity scientific data
with queryable metadata.

**Hierarchy:** Dataset → Entity → Artifact
- **Datasets** are top-level containers (VDP, EDRIXS, RIXS, SEQUOIA, etc.)
  with provenance metadata (material, producer, facility)
- **Entities** are containers with physics parameters as queryable metadata
- **Artifacts** are array children of their parent entity
- **Keys are human-readable**: `client["EDRIXS"]["H_edx00000"]["rixs"]`

**Dual-mode access:**
- **Mode A (Expert):** Query metadata for HDF5 paths, load directly with h5py
- **Mode B (Visualizer):** Access arrays via Tiled HTTP adapters (chunked)

The broker is **dataset-agnostic**. The Parquet manifest is the contract: no
parameter names, artifact types, or file layouts are hardcoded.

## Directory Structure

```
tiled-catalog-broker/
├── CLAUDE.md              # This file
├── .gitignore
├── docs/                  # Design docs, handoffs, lessons learned
│   ├── SCHEMA-DESIGN.md   # Data model and hierarchy rationale
│   ├── DESIGN-GENERIC-BROKER.md
│   ├── INGESTION-GUIDE.md
│   └── ...
├── externals/             # Reference materials (PDFs, diagrams)
└── tiled_poc/             # Main implementation
    ├── config.yml         # Server configuration (port 8005)
    ├── ingest.py          # CLI: bulk ingest into catalog.db
    ├── register.py        # CLI: HTTP register into running server
    ├── broker/            # Core library (1,800+ LOC)
    │   ├── config.py      # YAML config loading
    │   ├── utils.py       # Shared helpers
    │   ├── bulk_register.py   # SQLAlchemy bulk registration
    │   ├── http_register.py   # HTTP registration via Tiled client
    │   ├── catalog.py     # Catalog creation + dataset containers
    │   └── query_manifest.py  # Mode A discovery API
    ├── examples/          # Standalone example scripts
    └── tests/             # Test suite
```

## How to Run

See `tiled_poc/README.md` for the full quickstart. Summary:

```bash
cd $PROJ_DIR/tiled_poc

# Common deps shorthand
UV_DEPS="--with 'tiled[server]' --with pandas --with pyarrow --with h5py --with 'ruamel.yaml' --with canonicaljson"

# Pipeline: (pre-built manifests in manifests/) → ingest → serve
uv run $UV_DEPS python ingest.py datasets/mydata.yml
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
```

## Running Tests

```bash
cd $PROJ_DIR/tiled_poc

# Unit tests (no server required)
uv run --with pytest $UV_DEPS \
  pytest tests/test_config.py tests/test_utils.py tests/test_generic_registration.py -v

# Integration tests (requires running server with data)
uv run --with pytest $UV_DEPS pytest tests/ -v
```

## Architecture

```
/ (root)
├── VDP/                         ← dataset container
│   metadata: {organization, data_type, producer, material, ...}
│   ├── H_636ce3e4/              ← entity container
│   │   metadata: {Ja_meV, Jb_meV, Jc_meV, Dc_meV, spin_s, g_factor}
│   │   ├── mh_powder_30T        ← array artifact (200,)
│   │   ├── ins_12meV            ← array artifact (600, 400)
│   │   └── ...
│   └── ...
├── EDRIXS/                      ← dataset container
│   ├── H_edx00000/
│   │   metadata: {tenDq, F2_dd, ...}
│   │   └── rixs
│   └── ...
├── RIXS/                        ← experimental dataset
├── SEQUOIA/
└── ...
```

## Ingested Datasets

| Dataset | Type | Entities | Artifacts | Producer/Facility |
|---------|------|----------|-----------|-------------------|
| VDP | simulation | 10,000 | 110,000 | Sunny.jl |
| EDRIXS | simulation | 10,000 | 10,000 | EDRIXS |
| NiPS3 Multimodal | simulation | 7,616 | 45,696 | Synthetic |
| RIXS | experimental | 7 | 42 | LCLS / qRIXS |
| Challenge | benchmark | 1 | 9 | - |
| SEQUOIA | experimental | 3 | 76 | SNS / SEQUOIA |

## Related Documentation

| Document | Description |
|----------|-------------|
| `docs/SCHEMA-DESIGN.md` | Data model, hierarchy rationale, ArrayLake comparison |
| `docs/DESIGN-GENERIC-BROKER.md` | Generic broker architecture |
| `docs/INGESTION-GUIDE.md` | How to add new datasets |
| `docs/LOCATOR-AND-MANIFEST-CONTRACT.md` | Manifest contract specification |
| `docs/LESSONS_LEARNED.md` | Lessons learned |
| `tiled_poc/README.md` | Full quickstart and API reference |
