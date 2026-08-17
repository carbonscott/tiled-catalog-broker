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
- **Datasets** are top-level containers (BROAD_SIGMA, LCLS_RIXS_STATIC, etc.)
  with provenance metadata (method, material, producer, facility)
- **Entities** are containers with physics parameters as queryable metadata
- **Artifacts** are array children of their parent entity
- **Keys are human-readable**: `client["BROAD_SIGMA"][entity_key]["rixs_spectrum"]`

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
│       ├── cli.py             # CLI: tcb {generate,stamp-key,register,delete}
│       ├── config.py          # Server connection settings from the environment
│       ├── http_register.py   # HTTP registration via Tiled client (the single route)
│       ├── utils.py           # Shared helpers
│       ├── adapters/          # Tiled array adapters
│       ├── tools/             # Data-prep tools
│       │   ├── _models.py     # Pydantic dataset YAML contract (the contract surface)
│       │   ├── generate.py    # Generate Parquet manifests from YAML
│       │   └── schema.py      # YAML contract validation + soft vocab checks
│       └── clients/           # Client-side utilities
│           ├── tiled_cache.py # Disk-backed cache + PyTorch Dataset
│           └── query_manifest.py  # Mode A discovery API
├── examples/                  # demo_query.py — marimo notebook of the read path
├── tests/                     # Test suite
└── docs/                      # Design docs, handoffs, lessons learned
```

## How to Run

```bash
# Install in development mode
uv pip install -e .

# Or run directly with uv
uv run tcb --help

# Pipeline: author YAML → stamp-key → generate → register
tcb stamp-key datasets/my_dataset.yml
tcb generate datasets/my_dataset.yml
tcb register datasets/my_dataset.yml     # needs a running server (TILED_URL, TILED_API_KEY)
tcb register --upload datasets/my_dataset.yml  # stream arrays into server storage (server can't see the files)

# Serve
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

Entity keys are `{dataset_key}_{uid[:13]}`, derived at registration from the dataset key and
the manifest uid. Artifact keys are the manifest's `type` verbatim.

```
/ (root)
├── BROAD_SIGMA/                     ← dataset container
│   metadata: {method, data_type, material, producer, ...}
│   ├── BROAD_SIGMA_1a2b3c4d5e6f7/   ← entity container
│   │   metadata: {sigma, gamma, ...} + path_/dataset_/index_ locators
│   │   └── rixs_spectrum            ← array artifact (151, 40)
│   └── ...
├── CONCATENATED_MULTIMODAL/         ← dataset container
│   ├── CONCATENATED_MULTIMODAL_.../
│   │   metadata: {J1a, J1b, ...}
│   │   ├── hisym                    ← array artifact (384, 384)
│   │   ├── powder                   ← array artifact (512, 256)
│   │   └── ...
│   └── ...
├── LCLS_RIXS_STATIC/                ← experimental dataset
└── ...
```

The dataset YAMLs this repo has onboarded are in `datasets/`; each one's `key:` is the
container key it registers into. For what is actually registered on a given server, ask the
server — `list(from_uri(url, api_key=key))`.

## Related Documentation

| Document | Description |
|----------|-------------|
| `CONTEXT.md` | Domain language + the implementation-vs-contract principle |
| `docs/ONBOARDING.md` | How to onboard a dataset (the contract-surface walkthrough) |
| `docs/using-the-catalog.md` | How to *read* a registered dataset (Mode A + Mode B) |
| `docs/remote-onboarding.md` | Registering data the server cannot see (`tcb register --upload`) |
| `docs/adr/` | Architecture Decision Records (frozen layouts, single register route, soft vocab, hierarchical containers) |
| `docs/SLICING-EXPLAINER.md` | How batched arrays are served slice-by-slice over Tiled |
