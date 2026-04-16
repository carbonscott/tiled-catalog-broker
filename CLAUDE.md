## Environment Setup

Set these environment variables before running any commands:

```bash
export UV_CACHE_DIR=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cwang31/.UV_CACHE
```

Use `uv` to run python programs. The UV_CACHE_DIR avoids repeated package downloads.

## Project Overview

**Tiled Catalog Broker** — a config-driven system for registering
multi-modal scientific HDF5 datasets into a
[Tiled](https://blueskyproject.io/tiled/) catalog. Data model inspired by
[ArrayLake](https://docs.earthmover.io/concepts/data-model) (Organization →
Repo → Group → Array), adapted for many-entity scientific data with queryable
metadata.

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
├── CLAUDE.md                  # This file
├── pyproject.toml             # Package definition (tiled-catalog-broker)
├── config.yml                 # Tiled server configuration
├── src/
│   └── tiled_catalog_broker/  # Installable Python package
│       ├── cli.py             # CLI: tcb {inspect,generate,ingest,register}
│       ├── config.py          # Environment/config loading
│       ├── bulk_register.py   # Bulk SQL registration (deprecated, local testing only)
│       ├── http_register.py   # HTTP registration via Tiled client
│       ├── utils.py           # Shared helpers
│       ├── adapters/          # Tiled array adapters
│       ├── tools/             # Data-prep tools
│       │   ├── inspect.py     # Auto-generate draft YAML from HDF5
│       │   ├── generate.py    # Generate Parquet manifests from YAML
│       │   └── schema.py      # YAML contract validation
│       └── clients/           # Client-side utilities
│           ├── tiled_cache.py # Disk-backed cache + PyTorch Dataset
│           └── query_manifest.py  # Mode A discovery API
├── examples/                  # Standalone examples and marimo demos
├── tests/                     # Test suite
└── docs/                      # Design docs, handoffs, lessons learned
```

## How to Run

```bash
# Install in development mode
uv pip install -e .

# Or run directly with uv
uv run tcb --help

# Pipeline: ingest → serve
tcb ingest datasets/my_dataset.yml
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
```

## Running Tests

```bash
# Unit tests (no server required)
uv run --with pytest pytest tests/test_config.py tests/test_utils.py tests/test_generic_registration.py -v

# Integration tests (requires running server with data)
uv run --with pytest pytest tests/ -v
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
