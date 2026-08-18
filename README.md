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

- Python >= 3.12
- [`uv`](https://docs.astral.sh/uv/)

Optionally set `UV_CACHE_DIR` to avoid re-downloading packages every run.

Install the package in development mode:

```bash
uv pip install -e .
```

---

## Quickstart

### Step 1: Author a Dataset YAML

Write a dataset YAML against the contract surface — see
[docs/ONBOARDING.md](docs/ONBOARDING.md) for the full walkthrough, and copy the example
matching your layout from [`datasets/examples/`](datasets/examples/). The authoritative
field list is `src/tiled_catalog_broker/tools/_models.py`.

> Using Claude Code? Run **`/onboarding`** to have an agent read the contract surface and
> walk you through authoring the YAML and running the pipeline.

### Step 2: Stamp the catalog key

Derive the YAML's `key:` field from `label` (this is the dataset's
container key in Tiled). Run once per YAML; idempotent on re-run.

```bash
tcb stamp-key datasets/mydata.yml
```

`tcb generate` and `tcb register` are read-only over the YAML and will
error with a hint if `key` is missing.

### Step 3: Generate Manifests

Generate Parquet manifests from the YAML (this also validates it against the contract):

```bash
tcb generate datasets/mydata.yml
```

### Step 4: Start the Tiled Server

```bash
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
```

### Step 5: Register into Tiled

In a new terminal, register manifests into the running server via HTTP:

```bash
tcb register datasets/mydata.yml
```

### Step 6: Retrieve Data

Open a new terminal (keep the server running) and start Python:

```bash
uv run python
```

**Mode B -- Array access via Tiled (simplest):**

```python
from tiled.client import from_uri
from tiled.queries import Key

client = from_uri("http://localhost:8005", api_key="secret")

# Browse datasets
print(list(client))
# ['BROAD_SIGMA', 'LCLS_RIXS_STATIC', ...]

# Pick a dataset; narrow it with a metadata query (SQL-served)
ds = client["BROAD_SIGMA"]
hits = ds.search(Key("sigma") >= 0.04)

# Pick an entity, list its artifacts
ent = hits.values().first()
print(list(ent))
# ['rixs_spectrum']

# Read an array -- only the requested bytes cross the wire
spectrum = ent["rixs_spectrum"][:]
print(spectrum.shape)  # (151, 40)
```

**Mode A -- Expert path-based access (fast, for ML pipelines):**

```python
import os
import h5py

# Entity metadata carries HDF5 locators, relative to the YAML's data.directory
md = dict(ent.metadata)
rel_path = md["path_rixs_spectrum"]
dataset  = md["dataset_rixs_spectrum"]

base_dir = "/path/to/hdf5/root"          # the dataset YAML's data.directory
with h5py.File(os.path.join(base_dir, rel_path)) as f:
    spectrum = f[dataset][:]
```

Both modes are covered in full in
[docs/using-the-catalog.md](docs/using-the-catalog.md).

### Step 7: Interactive Exploration (Optional)

```bash
uv run --with marimo --with matplotlib \
  marimo edit examples/demo_query.py
```

The notebook reads the catalog over HTTP and does not import this package,
so it also runs standalone in any Python ≥ 3.10 environment:

```bash
pip install 'tiled[client]' marimo pandas h5py numpy matplotlib
marimo edit examples/demo_query.py
```

Set `TCB_DEMO_DATASET` to walk a dataset other than `BROAD_SIGMA`.

See [docs/using-the-catalog.md](docs/using-the-catalog.md) for the full read-side reference
(both access modes, copy-pasteable).

---

## Workflow Overview

The `tcb` CLI subcommands form a pipeline:

```
dataset YAML  -->  tcb stamp-key  -->  tcb generate  -->  tcb register  -->  tiled serve
(the contract)     (manifests)        (key in YAML)       (HTTP)             (queries)
```

| Subcommand | Purpose | Server needed? |
|------------|---------|----------------|
| `tcb generate` | Generate Parquet manifests from a dataset YAML | No |
| `tcb stamp-key` | Write the derived catalog key into the YAML | No |
| `tcb register` | Register manifests into a running server (HTTP); `--upload` streams the arrays into server storage | Yes |
| `tcb delete` | Remove registered data from a running server (external HDF5 files untouched; uploaded arrays removed) | Yes |

---

## HTTP Registration (Incremental)

`tcb register` registers data into a **running** Tiled server. It is
incremental: entities that already exist (by key) are skipped. Server URL
and apikey are read from `TILED_URL` / `TILED_API_KEY`.

```bash
# Register a dataset into the already-running server
tcb register datasets/mydata.yml

# Limit to 5 entities
tcb register datasets/mydata.yml -n 5

# Register multiple datasets at once
tcb register datasets/vdp.yml datasets/edrixs.yml
```

### Registering data the server cannot see (`--upload`)

Pointer registration requires the server to read your HDF5 files from its
own filesystem. When it can't — you're at another institution, the data is
on your laptop — add `--upload`: the arrays are read from your local files
and written through the server into its writable storage, where they
persist. Same YAML, same manifests, different transport. The dataset is
stamped `storage: uploaded`, and a dataset cannot mix uploaded and pointer
entities.

```bash
tcb register --upload datasets/mydata.yml
```

Full walkthrough (including the server-side setup, `config.demo.yml`):
`docs/remote-onboarding.md`.

### Switching between test and prod servers

Keep one `.env` file per server and source the right one before running
`tcb register`. No CLI flag needed:

```bash
# .env.test
TILED_URL=https://tiled-test.internal/api
TILED_API_KEY=...

# .env.prod
TILED_URL=https://tiled.internal/api
TILED_API_KEY=...
```

```bash
set -a; source .env.test; set +a   # export every var in the file
tcb register datasets/mydata.yml
```

---

## Deleting Registered Data

`tcb delete` removes registered data from the server. External HDF5 files
on disk are never touched; for datasets registered with `--upload`, the
arrays the server stores are deleted along with the catalog entries (the
catalog is their only server-side home). Granularity is inferred from the
number of positional arguments:

```bash
tcb delete <DATASET>                       # dataset + everything under it
tcb delete <DATASET> <ENTITY>              # one entity and its artifacts
tcb delete <DATASET> <ENTITY> <ARTIFACT>   # one artifact array
tcb delete all                             # every top-level container
```

Granular forms prompt for `y`/`yes` (bypass with `--yes`). The `all` form
requires retyping `TILED_URL` to confirm; case and trailing-slash
differences are normalized so `https://Tiled.example.com/` matches
`https://tiled.example.com`. Bypass non-interactively with `--confirm <URL>`.

`--dry-run` previews without deleting.

---

## Adding Your Own Dataset

Two things are needed:

### 1. Dataset Contract (`datasets/mydata.yml`)

The YAML contract describes your dataset's structure. Key fields:

```yaml
label: MyData
data:
  directory: /path/to/hdf5/root
  layout: per_entity
```

- `label` -- Human-readable name. After authoring, run `tcb stamp-key` to
  derive the Tiled container key (e.g. `"Broad Sigma"` -> `BROAD_SIGMA`)
  into the YAML's `key:` field.
- `data.directory` -- Root directory. All HDF5 `file` paths in the manifest
  are relative to this.
- `data.layout` -- One of `per_entity`, `batched`, `grouped` (ADR-0001).

The authoritative field list is `src/tiled_catalog_broker/tools/_models.py`;
see [docs/ONBOARDING.md](docs/ONBOARDING.md) for the walkthrough.

### 2. Parquet Manifests

The manifest contains two DataFrames:

**Entity DataFrame** -- one row per entity:

| Column | Required | Description |
|--------|----------|-------------|
| `uid` | Yes | Content-addressed entity ID. Tiled key is `{dataset_key}_{uid[:13]}` (synthesized at register, not stored). |
| *(any others)* | No | Become container metadata automatically |

**Artifact DataFrame** -- one row per artifact:

| Column | Required | Description |
|--------|----------|-------------|
| `uid` | Yes | Links to parent entity |
| `type` | Yes | Artifact key (e.g. `rixs_spectrum`, `powder`) |
| `file` | Yes | Relative path to HDF5 file (from `data.directory`) |
| `dataset` | Yes | HDF5 internal dataset path (e.g. `/spectra`) |
| `index` | No | Row index for batched arrays |
| `shape` | Yes | JSON-encoded shape **as registered** (batched: leading axis already dropped) |
| `dtype` | Yes | numpy dtype string (e.g. `float32`) |
| *(any others)* | No | Become artifact metadata automatically |

`shape` and `dtype` are captured by `tcb generate` so that **registration never
opens HDF5** — it reads only the manifest. A manifest produced before these
columns existed is rejected with a message telling you to re-run `tcb generate`.

### 3. Server Config

Add your `data.directory` to `readable_storage` in `config.yml`:

```yaml
readable_storage:
  - "/existing/path"
  - "/path/to/hdf5/root"   # <-- add this
```

### Run It

```bash
# Register into a live server (the single registration route)
tcb register datasets/mydata.yml
```

---

## Running Tests

### Unit Tests (no server required)

```bash
uv run --with pytest pytest tests/test_config.py tests/test_utils.py tests/test_generic_registration.py tests/test_generate.py tests/test_schema.py tests/test_examples.py -v
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
| `test_config.py` | Unit | Server connection settings from the environment |
| `test_utils.py` | Unit | Artifact key generation, shared helpers |
| `test_generate.py` | Unit | Parquet manifest generation |
| `test_schema.py` | Unit | YAML contract validation |
| `test_examples.py` | Unit | Example dataset YAMLs validate against the contract |
| `test_generic_registration.py` | Unit | Registration is dataset-agnostic (per-entity + batched shapes) |
| `test_registration.py` | Integration | Manifest loading + the registered result on a live server |
| `test_data_retrieval.py` | Integration | Mode A/B data access |
| `test_tiled_cache.py` | Integration | Disk-backed cache hit/miss behavior |

---

## Directory Structure

```
tiled-catalog-broker/
├── pyproject.toml             # Package definition (tiled-catalog-broker)
├── config.yml                 # Tiled server configuration
├── src/
│   └── tiled_catalog_broker/  # Installable Python package
│       ├── cli.py             # CLI: tcb {generate,stamp-key,register,delete}
│       ├── config.py          # Environment/config loading
│       ├── http_register.py   # HTTP registration via Tiled client
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
└── docs/                      # Design docs, ADRs, onboarding + read-side guides
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

### Re-registering data
`tcb register` is **incremental** and safe to run multiple times: an entity
already on the server is skipped, so a re-run resumes rather than duplicates.
