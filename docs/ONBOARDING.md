# Onboarding a Dataset

**Audience:** anyone (human or agent) registering a new HDF5 dataset into the catalog.
**Prerequisite:** access to the data files and a working Python environment with `uv`.

Onboarding takes a producer's HDF5 dataset and registers it so it can be discovered and
loaded through the catalog. You do it by **authoring one dataset YAML** against the
*contract surface*, then running three commands:

```
author dataset YAML  -->  tcb generate  -->  tcb stamp-key  -->  tcb register
   (the contract)         (manifests)        (key in YAML)       (into a server)
```

You should never need to read broker *implementation* (`generate.py`, `http_register.py`)
to do this. Everything you need is the contract surface below. If you find yourself
reverse-engineering implementation to learn what a valid dataset looks like, that is a gap
in this guide — fix the guide.

---

## 1. The contract surface

These are the only files you read to author a dataset. Together they define what a valid
dataset is:

| Artifact | What it tells you |
|----------|-------------------|
| `src/tiled_catalog_broker/tools/_models.py` | **The field contract.** Every YAML field, its type, whether it's required, and a one-line description. This is the source of truth — read it. |
| `src/tiled_catalog_broker/tools/schema/catalog_model.yml` | **The semantic model.** Canonical vocabulary (and aliases) for `method`, `material`, `producer`, `project`, `facility`, `data_type`. |
| `datasets/examples/{per_entity,batched,grouped}.yml` | **A worked example per layout.** Copy the one that matches your data and adapt it. |
| This guide | The layout definitions and the narrative tying it together. |

The pydantic models in `_models.py` are self-documenting: every field carries a
`description`, and the models `forbid` unknown keys in the closed sections (so a typo'd key
is an error, not silently ignored). When in doubt about a field, read its `Field(...)` line.

---

## 2. The data contract

The broker is **dataset-agnostic but not structure-agnostic**: your data must already be in
one of **exactly three layouts**, and this set is frozen (ADR-0001). The cost of matching a
layout is the producer's, paid once and up front — the broker does not guess at arbitrary
structure.

### Layouts (`data.layout`)

**`per_entity`** — one HDF5 *file* per entity. Each file holds that entity's artifact arrays;
its parameters are scalars (or attributes) in the file.

```
data/
├── entity_0001.h5   →  /spectrum (600,)   + scalar params tenDq, F2_dd, ...
├── entity_0002.h5   →  /spectrum (600,)   + scalars
└── ...
```

**`batched`** — many entities stacked along **axis-0** of shared datasets within each file.
Entity *i* is row *i* of every artifact dataset. Parameters live in a parallel array (one
row per entity), usually in a `/params` group.

```
run.h5
├── /spectra   (10000, 151, 40)   →  entity i = /spectra[i]
└── /params/
    ├── tenDq  (10000,)
    └── F2_dd  (10000,)
```

**`grouped`** — one HDF5 *group* per entity inside a file, each group self-contained.

```
samples.h5
└── /samples/
    ├── sample_000/  →  spectrum (151, 40)  +  /params/ scalars
    ├── sample_001/  →  spectrum (151, 40)  +  /params/ scalars
    └── ...
```

### Parameter locations (`parameters.location`)

Where each entity's queryable physics parameters are read from. Four are supported (see
`ParamLocation` in `_models.py`):

| `location` | Where parameters live | Typical layout |
|------------|-----------------------|----------------|
| `root_scalars` | scalar (0-dim) datasets at the file root | `per_entity` |
| `root_attributes` | root-level HDF5 attributes (`f.attrs`) | `per_entity` |
| `group` | datasets inside a named group — set `parameters.group` (e.g. `/params`); each is one row per entity | `batched` |
| `group_scalars` | scalar datasets inside each entity group | `grouped` |

For `grouped`, `parameters.entity_group` names the group that holds one subgroup per entity
(defaults to `samples`, then falls back to the file's top-level groups).

---

## 3. Authoring the dataset YAML

A dataset YAML describes **exactly one** dataset. For the full field list and per-field
rules read `src/tiled_catalog_broker/tools/_models.py`; for a complete file to adapt, copy
the `datasets/examples/<layout>.yml` that matches your layout. Key things to know:
- **Required** (config errors if missing): `label`, `metadata.method`/`data_type`/`material`,
  `data.directory`/`layout`, at least one `artifacts` entry, `parameters.location`, and `key`
  (filled by `tcb stamp-key`).
- The closed sections (`data`, `artifacts`, `parameters`, `shared`) reject unknown keys — a
  typo is a hard error. `metadata` is open by design.
- `artifacts[].dataset` resolves per layout: used as-is for `per_entity`/`batched`, and
  relative to each entity group for `grouped`.

---

## 4. The vocabulary is soft

`metadata` carries the facets that make cross-dataset discovery work
(`Key("project") == "MAIQMag"`, `Key("material") == "NiPS3"`). The semantic model
(`catalog_model.yml`) defines canonical ids and aliases for `method`, `material`, `producer`,
`project`, `facility`, and `data_type`.

It is **soft normalization, not a gate** (ADR-0003):
- Known **aliases** are rewritten to their canonical id (e.g. `EDRIXS` → `RIXS`, `nips3` →
  `NiPS3`) automatically.
- An **unknown** value validates with a **warning**, not an error — your dataset still
  onboards.

Prefer the canonical ids in the semantic model so your data lands on the same facets as
everyone else's; that consistency is the whole point. Field **presence** of `method`,
`data_type`, and `material` is required; only the **value** is soft.

---

## 5. Run the pipeline

```bash
# 1. Generate the Parquet manifests (entities.parquet + artifacts.parquet).
#    Validates the YAML against the contract; prints vocab warnings (non-fatal).
tcb generate datasets/my_dataset.yml

# 2. Stamp the catalog key derived from the label (slug). One-time; re-running is a no-op.
tcb stamp-key datasets/my_dataset.yml

# 3. Register into a running Tiled server (HTTP). Incremental — existing entities skip.
#    Server URL/key come from TILED_URL / TILED_API_KEY.
tcb register datasets/my_dataset.yml
```

`tcb register` is the **single registration route** (ADR-0002). It reads only the manifests —
each artifact's shape and dtype were captured by `tcb generate`, so registration never opens
your HDF5 files.

Two consequences worth knowing:

- **Re-run `tcb generate` if the data changes shape or dtype.** The manifest is a snapshot;
  registration trusts it. The read adapter re-checks both against the file and raises on a
  mismatch, so stale values surface as HTTP 500 on read rather than as silently wrong data.
- **A manifest without `shape`/`dtype` is rejected** with a message telling you to
  regenerate. Regenerating is cheap and idempotent.

Start the server with:

```bash
uv run --with 'tiled[server]' tiled serve config config.yml --api-key secret
```

### Paths: where your view and the server's view differ

The most common registration failure is silent — registration reports success and every
read then returns **HTTP 500**, because the paths written into the catalog point at
somewhere the server can't reach. Three settings have to agree, and only two live in your
dataset YAML:

| Setting | Whose view of the filesystem | What it's for |
|---|---|---|
| `data.directory` **(required)** | **yours**, the authoring host | Opening the files: `tcb generate` globs it with `file_pattern` and reads each artifact's shape and dtype |
| `data.server_base_dir` *(optional)* | **the server's** mount | The `data_uri` stamped into the catalog — the pointer the server follows at read time |
| `readable_storage` in the server's `config.yml` | **the server's** mount | The server's permission to read that root at all |

**If the server runs on your filesystem, omit `server_base_dir`.** It resolves the same
absolute paths you do, so `data.directory` is enough.

**If the server mounts the data elsewhere — any remote deployment — you must set it.**
Registration uses `server_base_dir` for the `data_uri` and falls back to `data.directory`
when it's unset, so omitting it writes *your* absolute paths into the catalog: dead
pointers on the server. The swap is mechanical — same trailing structure, different root:

```yaml
data:
  directory:       /sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/Zhantao
  server_base_dir: /prjmaiqmag01/data-source/Zhantao
```

Note that `readable_storage` must list the **server-side** root (`/prjmaiqmag01/...`), not
your `data.directory` — copying the wrong one of the two is an easy mistake, and it fails
the same silent way.

> `server_base_dir` is set by hand. Nothing in the broker derives it, and no environment
> variable supplies it — if a `.env` file defines `TILED_HOST_DATA_ROOT` or
> `TILED_SERVER_DATA_ROOT`, they have no effect.

---

## 6. Verify

```python
from tiled.client import from_uri
client = from_uri("http://localhost:8005", api_key="secret")

ds = client["MY_DATASET"]          # the stamped key
print(len(ds))                     # entity count
entity = ds.values().first()
print(dict(entity.metadata))       # physics parameters as queryable metadata
print(entity["spectrum"][:].shape) # an artifact array loads

# Cross-dataset discovery via facets:
from tiled.queries import Key
hits = client.search(Key("material") == "NiPS3")
```

---

## 7. Examples

Copy the example that matches your layout and adapt the paths:

- `datasets/examples/per_entity.yml` — one file per entity, scalar parameters.
- `datasets/examples/batched.yml` — entities stacked on axis-0, parameters in `/params`.
- `datasets/examples/grouped.yml` — one group per entity, `group_scalars` parameters.

Each is validated against the contract in `tests/test_examples.py`.
