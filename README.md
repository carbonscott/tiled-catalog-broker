# Data Catalog Service (DCS)

A config-driven system for registering scientific HDF5 datasets into a
[Tiled](https://blueskyproject.io/tiled/) catalog and retrieving them via two
access modes:

- **Mode A (Expert):** Query metadata for HDF5 paths, load directly with `h5py` -- fast, ideal for ML pipelines.
- **Mode B (Visualizer):** Access arrays as Tiled children via HTTP -- chunked, interactive.

The service is **dataset-agnostic**. The Parquet manifest is the contract: no
parameter names, artifact types, or file layouts are hardcoded.

---

## Prerequisites

- Python >= 3.10
- [`uv`](https://docs.astral.sh/uv/)

Set the cache directory so `uv` doesn't re-download packages every run:

```bash
export UV_CACHE_DIR=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cwang31/.UV_CACHE
```

Install the package in development mode:

```bash
uv pip install -e .
```

---

## Quickstart

The `datasets/` directory contains YAML contracts for each dataset. This
walkthrough goes from raw HDF5 files to querying data in Python.

### Step 1: Inspect HDF5 Data

`dcs inspect` scans a directory of HDF5 files and generates a draft YAML
contract:

```bash
dcs inspect /path/to/hdf5/data/
```

Edit the draft YAML to refine entity parameters, artifact types, and metadata.
Save it to `datasets/mydata.yml`.

### Step 2: Generate Manifests

`dcs generate` reads the YAML contract and produces Parquet manifests:

```bash
# Generate manifests (optionally limit to N entities with -n)
dcs generate datasets/mydata.yml -n 10
```

This creates entity and artifact Parquet files in the manifest output directory
specified by the YAML contract.

### Step 3: Ingest into Catalog

`dcs ingest` bulk-loads manifests into a SQLite catalog database using direct
SQLAlchemy (no running server needed).

```bash
dcs ingest datasets/mydata.yml
```

This creates `catalog.db` with all entities and their artifacts.

### Step 4: Start the Tiled Server

```bash
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
```

### Step 5: Retrieve Data

Open a new terminal (keep the server running) and start Python:

```bash
uv run python
```

**Mode B -- Array access via Tiled (simplest):**

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8005", api_key="secret")

# Browse datasets
print(list(client))
# ['VDP', 'EDRIXS', ...]

# Pick a dataset, list entities
vdp = client["VDP"]
print(list(vdp)[:5])
# ['H_636ce3e4', 'H_7a1b2c3d', ...]

# Pick an entity, list its artifacts
h = vdp[list(vdp)[0]]
print(list(h))
# ['mh_powder_30T', 'gs_state', 'ins_12meV']

# Read an array
curve = h["mh_powder_30T"][:]
print(curve.shape)  # (200,)
```

**Mode A -- Expert path-based access (fast, for ML pipelines):**

```python
import h5py

h = vdp[list(vdp)[0]]

# Metadata contains HDF5 locators
rel_path = h.metadata["path_mh_powder_30T"]
dataset  = h.metadata["dataset_mh_powder_30T"]

# Load directly from HDF5
base_dir = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/vdp/data/schema_v1"
with h5py.File(f"{base_dir}/{rel_path}") as f:
    curve = f[dataset][:]
```

### Step 6: Interactive Exploration (Optional)

```bash
uv run --with marimo --with matplotlib \
  marimo edit notebooks/explore.py
```

---

## Workflow Overview

The `dcs` CLI subcommands form a pipeline:

```
dcs inspect          dcs generate         dcs ingest            tiled serve
  (draft YAML)  --->   (manifests)   --->   (catalog.db)   --->   (HTTP API)
                                            [offline bulk]        [serve queries]

                                          dcs register
                                            [online HTTP]  --->   (running server)
```

| Subcommand | Purpose | Server needed? |
|------------|---------|----------------|
| `dcs inspect` | Scan HDF5 directories, generate draft YAML contract | No |
| `dcs generate` | Read YAML contract, produce Parquet manifests | No |
| `dcs ingest` | Bulk-load manifests into `catalog.db` (SQLAlchemy) | No |
| `dcs register` | Register manifests into a running server (HTTP) | Yes |

**When to use which registration method:**

| Scenario | Use | Speed |
|----------|-----|-------|
| Initial load of 1K+ entities | `dcs ingest` | ~2,250 nodes/sec |
| Incremental updates to a live server | `dcs register` | ~5 nodes/sec |

---

## HTTP Registration (Incremental)

`dcs register` registers data into a **running** Tiled server. It is
incremental: entities that already exist (by key) are skipped.

```bash
# Register a dataset into the already-running server
dcs register datasets/mydata.yml

# Limit to 5 entities
dcs register datasets/mydata.yml -n 5

# Register multiple datasets at once
dcs register datasets/vdp.yml datasets/edrixs.yml
```

---

## Adding Your Own Dataset

Two things are needed:

### 1. Inspect Your Data

Use `dcs inspect` to scan your HDF5 directory and generate a draft YAML
contract:

```bash
dcs inspect /path/to/hdf5/data/
```

Review and edit the draft. Save it to `datasets/mydata.yml`.

### 2. Dataset Contract (`datasets/mydata.yml`)

The YAML contract describes your dataset's structure. Key fields:

```yaml
label: MyData
base_dir: /path/to/hdf5/root
```

- `label` -- Human-readable name (becomes the Tiled key).
- `base_dir` -- Root directory. All HDF5 `file` paths in the manifest are
  relative to this.

The manifest produced by `dcs generate` contains two DataFrames:

**Entity DataFrame** -- one row per entity:

| Column | Required | Description |
|--------|----------|-------------|
| `uid` | Yes | Unique identifier (first 8 chars become the Tiled key) |
| *(any others)* | No | Become container metadata automatically |

**Artifact DataFrame** -- one row per artifact:

| Column | Required | Description |
|--------|----------|-------------|
| `uid` | Yes | Links to parent entity |
| `type` | Yes | Artifact key (e.g. `rixs`, `mh_powder_30T`) |
| `file` | Yes | Relative path to HDF5 file (from `base_dir`) |
| `dataset` | Yes | HDF5 internal dataset path (e.g. `/spectra`) |
| `index` | No | Row index for batched arrays |
| *(any others)* | No | Become artifact metadata automatically |

### 3. Server Config

Add your `base_dir` to `readable_storage` in `config.yml`:

```yaml
readable_storage:
  - "/existing/path"
  - "/path/to/hdf5/root"   # <-- add this
```

### Run It

```bash
# Generate manifests
dcs generate datasets/mydata.yml -n 100

# Bulk ingest (offline)
dcs ingest datasets/mydata.yml

# Or HTTP register (live server)
dcs register datasets/mydata.yml
```

---

## Running Tests

### Unit Tests (no server required)

```bash
uv run --with pytest pytest tests/test_config.py tests/test_utils.py tests/test_generic_registration.py -v
```

### Integration Tests (require running server with data)

```bash
# Terminal 1: start server
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret

# Terminal 2: run tests
uv run --with pytest pytest tests/ -v
```

| Test File | Type | What It Covers |
|-----------|------|----------------|
| `test_config.py` | Unit | Configuration loading, path resolution |
| `test_utils.py` | Unit | Artifact key generation, shared helpers |
| `test_schema.py` | Unit | YAML contract validation |
| `test_inspect.py` | Unit | HDF5 directory inspection |
| `test_generate.py` | Unit | Manifest generation |
| `test_generic_registration.py` | Unit | Node preparation for VDP + NiPS3 datasets |
| `test_registration.py` | Integration | HTTP and bulk registration |
| `test_data_retrieval.py` | Integration | Mode A/B data access |

---

## Directory Structure

```
tiled-catalog-broker/
├── pyproject.toml             # Package definition (data-catalog-service)
├── config.yml                 # Tiled server configuration
├── README.md                  # This file
│
├── src/
│   └── data_catalog_service/  # Installable Python package
│       ├── cli.py             # CLI: dcs {inspect,generate,ingest,register}
│       ├── config.py          # YAML config loading
│       ├── inspect.py         # HDF5 directory inspection & draft YAML generation
│       ├── generate.py        # Parquet manifest generation from YAML contracts
│       ├── schema.py          # YAML contract validation
│       ├── schema/            # Semantic model (catalog_model.yml)
│       ├── catalog.py         # Catalog creation + dataset containers
│       ├── register.py        # SQLAlchemy bulk registration
│       ├── http_register.py   # HTTP registration via Tiled client
│       ├── query_manifest.py  # Mode A discovery API
│       └── utils.py           # Shared helpers
│
├── datasets/                  # Dataset YAML contracts
│
├── notebooks/                 # Marimo notebooks (demos, exploration)
│
├── examples/                  # Standalone example scripts
│
├── tests/                     # Test suite
│   ├── conftest.py
│   ├── test_config.py
│   ├── test_utils.py
│   ├── test_schema.py
│   ├── test_inspect.py
│   ├── test_generate.py
│   ├── test_generic_registration.py
│   ├── test_registration.py
│   ├── test_data_retrieval.py
│   └── testdata/              # Synthetic test data
│
└── docs/                      # Design docs, handoffs, lessons learned
```

---

## Troubleshooting

### "Server not running" error
Start the server first, then run `dcs register`.

### Port already in use
```bash
lsof -ti :8005 | xargs kill
```

### "Server error 500" during registration
The database may be corrupted. Stop the server, delete `catalog.db`, and
restart (the server creates a fresh database on startup).

### Re-ingesting data
`dcs ingest` is **additive** -- running it twice creates duplicates. To
re-ingest, delete `catalog.db` first. `dcs register` is **incremental** and
safe to run multiple times.

---

## Performance Reference

### Bulk Registration (`dcs ingest`)

| Entities | Approx. Time | DB Size |
|--------------|-------------|---------|
| 10 | < 1 sec | ~300 KB |
| 1,000 | ~5 sec | ~20 MB |
| 10,000 | ~53 sec | ~192 MB |

### PostgreSQL Backend

For concurrent access or very large catalogs, use PostgreSQL instead of SQLite.
See `docs/V6A-POSTGRES-NOTES.md` for setup and configuration.
