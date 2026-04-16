# Tiled Catalog Broker

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

### Step 1: Prepare Manifests

Each dataset needs a YAML contract (`datasets/mydata.yml`) and Parquet manifests
(entity + artifact DataFrames). See [Adding Your Own Dataset](#adding-your-own-dataset)
for the manifest format.

### Step 2: Ingest into Catalog

`tcb ingest` bulk-loads manifests into a SQLite catalog database using direct
SQLAlchemy (no running server needed).

```bash
tcb ingest datasets/mydata.yml
```

This creates `catalog.db` with all entities and their artifacts.

### Step 3: Start the Tiled Server

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

The `tcb` CLI subcommands form a pipeline:

```
manifests   --->   tcb ingest   --->   tiled serve
                   (catalog.db)        (HTTP API)
                   [offline bulk]      [serve queries]

                 tcb register
                   [online HTTP]  ---> (running server)
```

| Subcommand | Purpose | Server needed? |
|------------|---------|----------------|
| `tcb ingest` | Bulk-load manifests into `catalog.db` (SQLAlchemy) | No |
| `tcb register` | Register manifests into a running server (HTTP) | Yes |

**When to use which registration method:**

| Scenario | Use | Speed |
|----------|-----|-------|
| Initial load of 1K+ entities | `tcb ingest` | ~2,250 nodes/sec |
| Incremental updates to a live server | `tcb register` | ~5 nodes/sec |

---

## HTTP Registration (Incremental)

`tcb register` registers data into a **running** Tiled server. It is
incremental: entities that already exist (by key) are skipped.

```bash
# Register a dataset into the already-running server
tcb register datasets/mydata.yml

# Limit to 5 entities
tcb register datasets/mydata.yml -n 5

# Register multiple datasets at once
tcb register datasets/vdp.yml datasets/edrixs.yml
```

---

## Adding Your Own Dataset

Two things are needed:

### 1. Dataset Contract (`datasets/mydata.yml`)

The YAML contract describes your dataset's structure. Key fields:

```yaml
label: MyData
base_dir: /path/to/hdf5/root
```

- `label` -- Human-readable name (becomes the Tiled key).
- `base_dir` -- Root directory. All HDF5 `file` paths in the manifest are
  relative to this.

### 2. Parquet Manifests

The manifest contains two DataFrames:

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
# Bulk ingest (offline)
tcb ingest datasets/mydata.yml

# Or HTTP register (live server)
tcb register datasets/mydata.yml
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
| `test_generic_registration.py` | Unit | Node preparation for VDP + NiPS3 datasets |
| `test_registration.py` | Integration | HTTP and bulk registration |
| `test_data_retrieval.py` | Integration | Mode A/B data access |

---

## Directory Structure

```
tiled-catalog-broker/
├── pyproject.toml             # Package definition (tiled-catalog-broker)
├── config.yml                 # Tiled server configuration
├── README.md                  # This file
│
├── src/
│   └── tiled_catalog_broker/  # Installable Python package
│       ├── cli.py             # CLI: tcb {ingest,register}
│       ├── config.py          # YAML config loading
│       ├── catalog.py         # Catalog creation + dataset containers
│       ├── register.py        # SQLAlchemy bulk registration
│       ├── http_register.py   # HTTP registration via Tiled client
│       ├── query_manifest.py  # Mode A discovery API
│       └── utils.py           # Shared helpers
│
├── notebooks/                 # Marimo notebooks (demos, exploration)
│
├── examples/                  # Standalone example scripts
│
├── tests/                     # Test suite
│   ├── conftest.py
│   ├── test_config.py
│   ├── test_utils.py
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
Start the server first, then run `tcb register`.

### Port already in use
```bash
lsof -ti :8005 | xargs kill
```

### "Server error 500" during registration
The database may be corrupted. Stop the server, delete `catalog.db`, and
restart (the server creates a fresh database on startup).

### Re-ingesting data
`tcb ingest` is **additive** -- running it twice creates duplicates. To
re-ingest, delete `catalog.db` first. `tcb register` is **incremental** and
safe to run multiple times.

---

## Performance Reference

### Bulk Registration (`tcb ingest`)

| Entities | Approx. Time | DB Size |
|--------------|-------------|---------|
| 10 | < 1 sec | ~300 KB |
| 1,000 | ~5 sec | ~20 MB |
| 10,000 | ~53 sec | ~192 MB |

### PostgreSQL Backend

For concurrent access or very large catalogs, use PostgreSQL instead of SQLite.
See `docs/V6A-POSTGRES-NOTES.md` for setup and configuration.
