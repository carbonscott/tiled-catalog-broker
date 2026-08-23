# Onboarding a Dataset

**Audience:** anyone (human or agent) registering a new HDF5 dataset into the catalog.
**Prerequisite:** access to the data files and a working Python environment with `uv`.

Onboarding takes a producer's HDF5 dataset and registers it so it can be discovered and
loaded through the catalog. You do it by **authoring one dataset YAML** against the
*contract surface*, then running three commands:

```
author dataset YAML  -->  tcb stamp-key  -->  tcb generate  -->  tcb register
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
| `group` | datasets inside one named group (`parameters.group`, e.g. `/params`) or several (`parameters.groups`, below); in `batched` each is one row per entity | `batched`, `per_entity` |
| `group_scalars` | scalar datasets inside each entity group (`group`/`groups` resolve relative to the entity group) | `grouped` |

For `grouped`, `parameters.entity_group` names the group that holds one subgroup per entity
(defaults to `samples`, then falls back to the file's top-level groups).

### Several parameter groups (`parameters.groups`)

Scientific files often spread an entity's scalars over several groups — instrument settings
here, sample description there, a free-form `parameters/` collection elsewhere (NeXus files
are the common case). `parameters.groups` reads all of them, **nested under the name you
give each group**, so the entity's metadata mirrors the file:

```yaml
parameters:
  location: group
  groups:                                  # name → HDF5 group path
    instrument: /entry/instrument          #   → metadata.instrument.Ei, metadata.instrument.name, ...
    sample:     /entry/sample              #   → metadata.sample.chemical_formula, ...
    parameters: /entry/parameters          #   → metadata.parameters.temperature
  recursive: false                         # true: descend subgroups (metadata.instrument.detector.distance)
  exclude: [/entry/instrument/BL17:SEEMeta:JSON]   # datasets or subgroups to skip (a large blob, say)
```

Rules, the same in every layout:

- Only **scalar** (0-dim) datasets are parameters; arrays in a parameter group are skipped —
  they are artifacts, list them under `artifacts:`. (`batched`: a 1-D dataset of length *N* is
  one value per entity; a scalar applies to every entity in the file.)
- A field's own **HDF5 attributes ride along as labels**, whatever the producer called them:
  `Ei` with `units="meV"` gives `metadata.instrument.Ei = 60.0` *and*
  `metadata.instrument.Ei_units = "meV"`. Labels are stored, not hashed — the entity's
  content-addressed UID is built from the parameter *values* only.
- Group paths are absolute for `per_entity`/`batched` and entity-relative for `grouped`.
- Nested metadata is queryable with Tiled's dotted keys: `Key("instrument.Ei") > 50`,
  `Key("sample.chemical_formula") == "NiPS3"`.
- `group:` (a single group) keeps flat field names, as before. `group` and `groups` are
  mutually exclusive; to nest a single group, name it under `groups`.

Artifacts get the same treatment for their own attributes: an array dataset's scalar attrs
(`units`, `long_name`, ...) become metadata on its array node in Tiled.

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
# 1. Stamp the catalog key derived from the label (slug). One-time; re-running is a no-op.
tcb stamp-key datasets/my_dataset.yml

# 2. Generate the Parquet manifests (entities.parquet + artifacts.parquet; a shared axis
#    is an artifact row with no uid — an artifact of the dataset rather than of an entity).
#    Validates the YAML against the contract (the key must be stamped first);
#    prints vocab warnings (non-fatal).
tcb generate datasets/my_dataset.yml

# 3. Register into a running Tiled server (HTTP). Incremental — existing entities skip.
#    Server URL/key come from TILED_URL / TILED_API_KEY.
tcb register datasets/my_dataset.yml
```

`tcb register` is the **single registration route** (ADR-0002). It reads only the manifests —
each artifact's (and shared axis's) shape and dtype were captured by `tcb generate`, so
registration never opens your HDF5 files.

Each `shared:` axis is registered **once, as an array child of the dataset container**
(`ds["eloss"]`), through the same transport as the artifacts — a pointer for a server that
can read your files, the bytes themselves with `--upload`. The dataset metadata also keeps
a `shared_dataset_<type>: <hdf5 path>` locator for Mode A.

**Check the counts before you register.** `tcb generate` ends with `Entities: N` and
`Artifacts: M (S shared axes)`. Work out what you expect first — `per_entity`: the files
`file_pattern` matches; `batched`: the leading axis of `artifacts[0]` summed over files;
`grouped`: the entity groups; artifacts = entities × `artifacts` entries, plus one per
`shared:` axis — and compare. A mismatch is the cheapest bug you will ever find: a glob
that caught a stray file, a wrong axis, a group you didn't know was there. Fix the YAML and
regenerate rather than registering a manifest you cannot account for.

Three consequences worth knowing:

- **Re-registering never rewrites.** `tcb register` is incremental: an entity already on
  the server is skipped (if a crashed run left it without some artifacts, the missing ones
  are added with a `WARNING ... half-registered` line). An artifact that exists is never
  touched — so after changing any *path* in the YAML (`directory`, `server_base_dir`, an
  artifact `dataset`), regenerate, **`tcb delete` the dataset**, and register again.
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
absolute paths you do, so `data.directory` is enough. The repo's `config.yml` already lists
`data` in `readable_storage` — resolved relative to the directory you start the server
from, normally the repo root — so a dataset placed under `./data/` is readable with no
config change. A dataset anywhere else needs its root added to `readable_storage`.

**If the server mounts the data elsewhere — any remote deployment — you must set it.**
Registration uses `server_base_dir` for the `data_uri` and falls back to `data.directory`
when it's unset, so omitting it writes *your* absolute paths into the catalog: dead
pointers on the server. The swap is mechanical — same trailing structure, different root:

```yaml
data:
  directory:       /home/me/rixs/data/my_dataset       # where *you* read the files
  server_base_dir: /mnt/shared/rixs/data/my_dataset    # where the *server* reads them
```

(A typical case: a facility filesystem bind-mounted into the server's container under a
different prefix — e.g. `/sdf/data/.../prjmaiqmag01/results/X` on SLAC's SDF appearing as
`/prjmaiqmag01/X` inside the server.) Note that `readable_storage` must list the
**server-side** root (`/mnt/shared/rixs/data/...` above), not your `data.directory` —
copying the wrong one of the two is an easy mistake, and it fails the same silent way.

> `server_base_dir` is set by hand. Nothing in the broker derives it, and no environment
> variable supplies it — if a `.env` file defines `TILED_HOST_DATA_ROOT` or
> `TILED_SERVER_DATA_ROOT`, they have no effect.

**Write `data.directory` as a physical path** (`pwd -P`, `readlink -f`), not through a
symlink. The server's containment check compares the path *as written* against
`readable_storage` and never resolves links, so a logical path that merely symlinks into
an allowed root is refused — and nothing opens the pointer until a read, so `tcb generate`
and `tcb register` both pass on it. Getting this wrong is expensive: fixing `directory`
afterwards means regenerate, delete the dataset, re-register.

---

## 6. Verify — read it back

Registration never opens your files, so `tcb register` finishing with `Artifact errors: 0`
says nothing about whether the server can read a byte: a pointer it cannot follow
registers cleanly and fails at first read. The proof is a read-back through the server —
with the same URL and key you registered into:

```python
from tiled.client import from_uri
client = from_uri("http://localhost:8005", api_key="secret")

ds = client["MY_DATASET"]          # the stamped key
shared = {k.removeprefix("shared_dataset_") for k in ds.metadata if k.startswith("shared_dataset_")}
ents = [k for k in ds if k not in shared]
print(len(ents))                   # == the entity count `tcb generate` printed
entity = ds[ents[0]]
print(dict(entity.metadata))       # physics parameters as queryable metadata
print(entity["spectrum"][:].shape) # an artifact array comes back with the manifest's shape
print(entity["spectrum"].metadata) # {type, shape, dtype, + the dataset's own attrs (units, ...)}
print(ds["energy"][:])             # each shared axis, served from the dataset container

# Cross-dataset discovery via facets:
from tiled.queries import Key
hits = client.search(Key("material") == "NiPS3")
# Within a dataset, nested parameters (parameters.groups) use dotted keys:
hot = ds.search(Key("parameters.temperature") > 100)
```

---

## 7. Examples

Copy the example that matches your layout and adapt the paths:

- `datasets/examples/per_entity.yml` — one file per entity, scalar parameters.
- `datasets/examples/batched.yml` — entities stacked on axis-0, parameters in `/params`.
- `datasets/examples/grouped.yml` — one group per entity, `group_scalars` parameters.
- `datasets/examples/per_entity_nexus.yml` — one NeXus file per entity: parameters spread
  over several groups (`parameters.groups`), an `NXdata` group of arrays as artifacts.

Each is validated against the contract in `tests/test_examples.py`. For how a NeXus file
maps onto the contract (and what is deliberately *not* modelled), see `docs/nexus-support.md`.

---

## 8. When it fails — symptom, cause, fix

Match on what you can see. Generation errors name the file and the YAML key involved;
registration and read errors mostly come from the server and are terser.

| Symptom | Cause | Fix |
|---|---|---|
| `tcb generate`: `Validation failed: ... 'key' is required` | The YAML is not stamped | `tcb stamp-key` |
| `tcb generate`: `Validation failed: ... Extra inputs are not permitted` | A typo'd or unknown key in a closed section (`data`, `artifacts`, `parameters`, `shared`) | Fix the key; `_models.py` is the field list |
| `tcb generate`: `OSError: Unable to open file ... file signature not found` | `file_pattern` matched a non-HDF5 sibling (a `.nc` twin, a Parquet sidecar, `.gz`) | Tighten `file_pattern`; `ls <directory>` first |
| `tcb generate`: `<file>: artifact type=... dataset '...' not found` | Wrong HDF5 path — or, in `grouped`, written absolute instead of relative to the entity group | Dump one file with `h5py` (`visititems`) and fix `dataset` |
| `tcb generate`: `shared axis type=...: dataset ... not found in any of the N files` / `... differs between a.h5 and b.h5` | Wrong path / the axis varies per entity | Fix the path / it is an artifact, not a shared axis — move it to `artifacts:` |
| `tcb generate`: `WARNING: /path@type is not carried as array metadata` | A dataset attribute is named like a manifest column (`type`, `shape`, `dtype`, `file`, `dataset`, `index`, `uid`) | Harmless; rename the attribute in the file if you need it carried |
| `tcb generate`: `pyarrow.lib.ArrowInvalid` while writing | A parameter changes type across files (scalar here, string or array there) | Dump two files and diff their parameters; fix the inconsistent one |
| `tcb generate` counts ≠ what you predicted | Glob matched extra/fewer files; `batched` leading axis isn't what you assumed (it is read from `artifacts[0]`); `entity_group` wrong or a non-entity subgroup counted | `ls <directory>/<file_pattern> \| wc -l`; dump the file; fix and regenerate |
| `tcb register`: `ERROR: manifests not found for '<label>'` | `tcb generate` not run, or run on a YAML with a different `label` | Run `tcb generate` on this YAML |
| `tcb register`: `dataset '<KEY>' exists with storage='external' but this run would register storage='uploaded'` (or vice versa) | The key was registered with the other transport | `tcb delete <KEY>` and re-register, or pick a new `label` |
| `tcb register`: `415 ... mimetype application/x-hdf5-broker is not one that the Tiled server knows how to read` | The server's config has no `adapters_by_mimetype` entry for the broker adapter | Server side: add it (the repo `config.yml` has it); on a server you don't run, ask its operator |
| `tcb register`: `WARNING ent=... half-registered: k of n artifacts on the server; registering the rest` | A previous run died mid-entity | Nothing — the re-run attaches the missing artifacts; check `Artifact errors: 0` |
| Register clean, **every read returns HTTP 500** `Internal server error` | The pointer the server followed is wrong: `data.directory` not under `readable_storage`; `server_base_dir` missing or wrong; a symlinked (logical) `directory`; or the file unreadable on the server. Ambiguous from the client by construction | §5 "Paths". On a local server, the log line `Refusing to serve file://... because it is outside the readable storage area` names the path. Fix the YAML, regenerate, `tcb delete` the dataset, re-register |
| Read returns 500 only for *some* entities | Stale manifest: a file changed shape/dtype since `tcb generate` (the adapter re-checks both) | Regenerate, delete, re-register |
| `KeyError: '<KEY>'` right after a clean register | You are reading a different server than you registered into | Same `TILED_URL` / `TILED_API_KEY` for register and read |
| `httpx.ConnectError` / connection refused | Server not running, wrong URL, or not reachable from this host | Start it / check the URL (`TILED_URL`) / check reachability |
| `401` naming the scopes it wanted | The API key lacks a scope (write for register, delete for `tcb delete`) | Read the message — it names required and held scopes; use a key that has them |
