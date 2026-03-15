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

## Quickstart

The broker consumes pre-built Parquet manifests. Manifest generators are
dataset-specific scripts that live in your application directory (not in
this repo). See [`docs/INGESTION-GUIDE.md`](../docs/INGESTION-GUIDE.md)
for how to create manifests and onboard new datasets.

### Step 1: Place Manifests

Place your pre-built Parquet manifests in `manifests/`:

```
manifests/
  mydata_entities.parquet
  mydata_artifacts.parquet
```

### Step 2: Ingest into Catalog

`ingest.py` bulk-loads manifests into a SQLite catalog database using direct
SQLAlchemy (no running server needed).

```bash
uv run $UV_DEPS python ingest.py datasets/mydata.yml
```

This creates `catalog.db` with all entities and their artifacts.

### Step 3: Start the Tiled Server

```bash
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
```

### Step 4: Retrieve Data

Open a new terminal (keep the server running) and start Python:

```bash
uv run $UV_DEPS python
```

**Mode B -- Array access via Tiled (simplest):**

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8005", api_key="secret")

# List dataset containers
print(list(client))
# ['VDP', 'EDRIXS', ...]

# Navigate into a dataset, list entities
vdp = client["VDP"]
print(list(vdp)[:5])

# Pick one, list its children
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

h = client["VDP"][list(client["VDP"])[0]]

# Metadata contains HDF5 locators
rel_path = h.metadata["path_mh_powder_30T"]
dataset  = h.metadata["dataset_mh_powder_30T"]

# Load directly from HDF5
base_dir = "/sdf/data/lcls/ds/prj/prjmaiqmag01/results/vdp/data/schema_v1"
with h5py.File(f"{base_dir}/{rel_path}") as f:
    curve = f[dataset][:]
```

---

## Workflow Overview

The two CLI scripts form a pipeline:

```
(pre-built manifests)     ingest.py             tiled serve
  in manifests/      --->   (catalog.db)   --->   (HTTP API)
                            [offline bulk]        [serve queries]

                          register.py
                            [online HTTP]  --->   (running server)
```

| Script | Purpose | Server needed? |
|--------|---------|----------------|
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

Two things are needed:

### 1. Parquet Manifests

Write a dataset-specific generator script (in your application directory)
that produces two Parquet files in `manifests/`. See
[`docs/INGESTION-GUIDE.md`](../docs/INGESTION-GUIDE.md) for the manifest
contract and worked examples.

**Entity manifest** (`manifests/{name}_entities.parquet`) -- one row per entity:

| Column | Required | Description |
|--------|----------|-------------|
| `uid` | Yes | Unique identifier |
| `key` | Yes | Tiled catalog key (must be unique) |
| *(any others)* | No | Become container metadata automatically |

**Artifact manifest** (`manifests/{name}_artifacts.parquet`) -- one row per artifact:

| Column | Required | Description |
|--------|----------|-------------|
| `uid` | Yes | Links to parent entity |
| `type` | Yes | Artifact key (e.g. `rixs`, `mh_powder_30T`) -- must be unique per uid |
| `file` | Yes | Relative path to HDF5 file (from `base_dir`) |
| `dataset` | Yes | HDF5 internal dataset path (e.g. `/spectra`) |
| `index` | No | Row index for batched arrays |
| *(any others)* | No | Become artifact metadata automatically |

### 2. Dataset Config (`datasets/mydata.yml`)

```yaml
key: MyData
label: My Dataset
base_dir: /path/to/hdf5/root
metadata:
  organization: MAIQMag
  data_type: simulation
  producer: MyCode
```

- `key` -- Tiled container key (immutable after first ingestion).
- `label` -- Human-readable name (for logging).
- `base_dir` -- Root directory. All HDF5 `file` paths in the manifest are
  relative to this.
- `metadata` -- Optional provenance metadata for the dataset container.

Add your `base_dir` to `readable_storage` in `config.yml`.

### Run It

```bash
# Bulk ingest (offline)
uv run $UV_DEPS python ingest.py datasets/mydata.yml

# Or HTTP register (live server)
uv run $UV_DEPS python register.py datasets/mydata.yml
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
