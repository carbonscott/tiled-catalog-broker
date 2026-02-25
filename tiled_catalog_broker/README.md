# Generic Tiled Broker

A config-driven system for registering scientific HDF5 datasets into a
[Tiled](https://blueskyproject.io/tiled/) catalog and retrieving them via two
access modes:

- **Mode A (Expert):** Query metadata for HDF5 paths, load directly with `h5py` -- fast, ideal for ML pipelines.
- **Mode B (Visualizer):** Access arrays as Tiled children via HTTP -- chunked, interactive.

The broker is **dataset-agnostic**. The Parquet manifest is the contract: no
parameter names, artifact types, or file layouts are hardcoded.

---

## Prerequisites

- Python >= 3.11
- [`uv`](https://docs.astral.sh/uv/) (manages dependencies inline -- no install step)

Set the cache directory so `uv` doesn't re-download packages every run:

```bash
export UV_CACHE_DIR=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/cwang31/.UV_CACHE
```

For convenience, define a variable with the common dependencies used across
commands:

```bash
UV_DEPS="--with 'tiled[server]' --with pandas --with pyarrow --with h5py --with 'ruamel.yaml' --with canonicaljson"
```

---

## Quickstart (Demo Walkthrough)

The `demo/` directory contains a self-contained example with three datasets
(VDP, EDRIXS, Multimodal). This walkthrough goes from raw HDF5 files to
querying data in Python.

### Step 1: Generate Manifests

Manifest generators in `extra/` scan HDF5 source data and produce Parquet
manifests. Each dataset has a YAML config in `demo/datasets/`:

```bash
cd demo

# Generate manifests for VDP and EDRIXS (10 entities each)
uv run $UV_DEPS python ../generate.py datasets/vdp.yml datasets/edrixs.yml -n 10
```

This creates:
```
demo/manifests/
  vdp_entities.parquet
  vdp_artifacts.parquet
  edrixs_entities.parquet
  edrixs_artifacts.parquet
```

### Step 2: Ingest into Catalog

`ingest.py` bulk-loads manifests into a SQLite catalog database using direct
SQLAlchemy (no running server needed).

```bash
# Still in demo/
uv run $UV_DEPS python ../ingest.py datasets/vdp.yml datasets/edrixs.yml
```

This creates `demo/catalog.db` with all entities and their artifacts.

### Step 3: Start the Tiled Server

```bash
# Still in demo/
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
```

The demo server runs on **port 8006** (production uses 8005).

### Step 4: Retrieve Data

Open a new terminal (keep the server running) and start Python:

```bash
cd demo
uv run $UV_DEPS python
```

**Mode B -- Array access via Tiled (simplest):**

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8006", api_key="secret")

# List entities
print(list(client)[:5])
# ['H_636ce3e4', 'H_7a1b2c3d', ...]

# Pick one, list its children
h = client[list(client)[0]]
print(list(h))
# ['mh_powder_30T', 'gs_state', 'ins_12meV']

# Read an array
curve = h["mh_powder_30T"][:]
print(curve.shape)  # (200,)
```

**Mode A -- Expert path-based access (fast, for ML pipelines):**

```python
import h5py

h = client[list(client)[0]]

# Metadata contains HDF5 locators
rel_path = h.metadata["path_mh_powder_30T"]
dataset  = h.metadata["dataset_mh_powder_30T"]

# Load directly from HDF5
base_dir = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/vdp/data/schema_v1"
with h5py.File(f"{base_dir}/{rel_path}") as f:
    curve = f[dataset][:]
```

### Step 5: Interactive Exploration (Optional)

```bash
cd demo
uv run $UV_DEPS --with marimo --with matplotlib \
  marimo edit explore.py
```

---

## Workflow Overview

The three CLI scripts form a pipeline:

```
generate.py          ingest.py             tiled serve
  (manifests)   --->   (catalog.db)   --->   (HTTP API)
                       [offline bulk]        [serve queries]

                     register.py
                       [online HTTP]  --->   (running server)
```

| Script | Purpose | Server needed? |
|--------|---------|----------------|
| `generate.py` | Scan HDF5 data, produce Parquet manifests | No |
| `ingest.py` | Bulk-load manifests into `catalog.db` (SQLAlchemy) | No |
| `register.py` | Register manifests into a running server (HTTP) | Yes |

**When to use which registration method:**

| Scenario | Use | Speed |
|----------|-----|-------|
| Initial load of 1K+ entities | `ingest.py` | ~2,250 nodes/sec |
| Incremental updates to a live server | `register.py` | ~5 nodes/sec |

---

## HTTP Registration (Incremental)

`register.py` registers data into a **running** Tiled server. It is
incremental: entities that already exist (by key) are skipped.

```bash
cd demo

# Register EDRIXS into the already-running server
uv run $UV_DEPS python ../register.py datasets/edrixs.yml

# Limit to 5 entities
uv run $UV_DEPS python ../register.py datasets/edrixs.yml -n 5

# Register multiple datasets at once
uv run $UV_DEPS python ../register.py datasets/vdp.yml datasets/edrixs.yml
```

---

## Adding Your Own Dataset

Three things are needed:

### 1. Dataset Config (`datasets/mydata.yml`)

```yaml
label: MyData
generator: gen_mydata_manifest
base_dir: /path/to/hdf5/root
```

- `label` -- Human-readable name (for logging).
- `generator` -- Python module name in `extra/` that generates manifests.
- `base_dir` -- Root directory. All HDF5 `file` paths in the manifest are
  relative to this.

### 2. Manifest Generator (`extra/gen_mydata_manifest.py`)

Must expose one function:

```python
def generate(output_dir, n_entities=10):
    """
    Returns:
        (ent_df, art_df): Two DataFrames written as Parquet.
    """
```

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
cd demo

# Generate manifests
uv run $UV_DEPS python ../generate.py datasets/mydata.yml -n 100

# Bulk ingest (offline)
uv run $UV_DEPS python ../ingest.py datasets/mydata.yml

# Or HTTP register (live server)
uv run $UV_DEPS python ../register.py datasets/mydata.yml
```

---

## Running Tests

### Unit Tests (no server required)

```bash
uv run --with pytest $UV_DEPS \
  pytest tests/test_config.py tests/test_utils.py tests/test_generic_registration.py -v
```

### Integration Tests (require running server with data)

```bash
# Terminal 1: start server
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret

# Terminal 2: run tests
uv run --with pytest --with 'ruamel.yaml' pytest tests/ -v
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
tiled_poc/
├── config.yml                  # Server config (port 8005)
├── generate.py                 # CLI: generate Parquet manifests
├── ingest.py                   # CLI: bulk ingest into catalog.db
├── register.py                 # CLI: HTTP register into running server
│
├── broker/                     # Core library
│   ├── config.py               # YAML config loading + accessors
│   ├── utils.py                # Shared helpers (JSON safe, shape cache)
│   ├── bulk_register.py        # SQLAlchemy bulk registration
│   ├── http_register.py        # HTTP registration via Tiled client
│   ├── catalog.py              # Catalog creation + dataset registration
│   └── query_manifest.py       # Mode A discovery API
│
├── extra/                      # Manifest generators (one per dataset)
│   ├── gen_vdp_manifest.py
│   ├── gen_edrixs_manifest.py
│   └── gen_multimodal_manifest.py
│
├── demo/                       # Self-contained multi-dataset demo
│   ├── config.yml              # Demo server config (port 8006)
│   ├── explore.py              # Marimo notebook
│   └── datasets/               # Dataset YAML configs
│       ├── vdp.yml
│       ├── edrixs.yml
│       └── multimodal.yml
│
├── examples/                   # Standalone example scripts
│   ├── demo_dual_mode.py
│   ├── demo_mh_dataset.py
│   └── demo_mh_dataset_with_query.py
│
└── tests/                      # Test suite
    ├── conftest.py
    ├── test_config.py
    ├── test_utils.py
    ├── test_generic_registration.py
    ├── test_registration.py
    ├── test_data_retrieval.py
    └── testdata/               # Synthetic test data (VDP + NiPS3)
```

---

## Troubleshooting

### "Server not running" error
Start the server first, then run `register.py`.

### Port already in use
```bash
lsof -ti :8005 | xargs kill
```

### "Server error 500" during registration
The database may be corrupted. Stop the server, delete `catalog.db`, and
restart (the server creates a fresh database on startup).

### Re-ingesting data
`ingest.py` is **additive** -- running it twice creates duplicates. To
re-ingest, delete `catalog.db` first. `register.py` is **incremental** and
safe to run multiple times.

---

## Performance Reference

### Bulk Registration (`ingest.py`)

| Entities | Approx. Time | DB Size |
|--------------|-------------|---------|
| 10 | < 1 sec | ~300 KB |
| 1,000 | ~5 sec | ~20 MB |
| 10,000 | ~53 sec | ~192 MB |

### PostgreSQL Backend

For concurrent access or very large catalogs, use PostgreSQL instead of SQLite.
See `docs/V6A-POSTGRES-NOTES.md` for setup and configuration.
