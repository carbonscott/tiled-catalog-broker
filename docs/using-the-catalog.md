# Using the catalog — reading a registered dataset

**Audience:** consumers of a catalog someone else has already registered. You have the
server's `<URL>` and an `<API_KEY>`. Getting a dataset to that point is
`docs/ONBOARDING.md`'s job; this file starts where that one stops.

Two ways in, for the same data:

| Mode | How | Use when |
|---|---|---|
| **Mode B (Visualizer)** | Arrays over HTTP through Tiled | You're anywhere the server is reachable, and you want a slice, not the whole file |
| **Mode A (Expert)** | Query Tiled for locators, then read HDF5 with h5py | You're on the same filesystem as the data and want bulk reads at full speed |

Mode B is the default. Reach for Mode A when you're feeding a training loop from a
filesystem you can already see.

---

## Mode B — through Tiled

Tiled's own client *is* the client. The broker adds nothing on the HTTP path, so there is
no broker import in this section.

```python
from tiled.client import from_uri
from tiled.queries import Key

c = from_uri("<URL>", api_key="<API_KEY>")

list(c)                                   # dataset keys: ['BROAD_SIGMA', 'LCLS_RIXS_STATIC', ...]
ds = c["BROAD_SIGMA"]                     # a dataset container
dict(ds.metadata)                         # provenance: method, data_type, material, producer
len(ds)                                   # entity count (+ one child per shared axis)
```

The catalog is **two levels**: dataset containers at the root, entity containers beneath
them. Entities are never at the root — always index the dataset first.

A dataset's **shared axes** (arrays identical for every entity — an energy grid, a field
axis) sit beside the entities as array children of the dataset container, keyed by their
`type` and listed in the dataset metadata as `shared_dataset_<type>`:

```python
shared = {k.removeprefix("shared_dataset_") for k in ds.metadata if k.startswith("shared_dataset_")}
eloss = ds["eloss"][:]                    # (151,) — one read, not one per entity
entities = [k for k in ds if k not in shared]
```

```python
# Metadata queries are served by SQL, so they stay fast as the dataset grows:
hits = ds.search(Key("sigma") >= 0.04).search(Key("sigma") <= 0.05)
hits = hits.search(Key("gamma") == 0.1)   # chain freely
len(hits)

ent = hits.values().first()
dict(ent.metadata)                        # physics parameters + Mode-A locators
list(ent)                                 # artifact keys under this entity
```

> Only `Key` comparisons (`==`, `>=`, `<=`, …) are SQL-served. **Never use `Regex`** — it is
> not SQL-backed, so it silently degrades to filtering every entity client-side.

Reading an artifact reads only the bytes you ask for — the broker registers arrays against
its own `LazyHDF5ArrayAdapter`, which slices in h5py rather than pulling the whole dataset
through dask first (see `docs/SLICING-EXPLAINER.md`):

```python
arr = ent["rixs_spectrum"]
arr.shape                                 # (151, 40)
arr[0:5, :]                               # numpy array — only these rows cross the wire
```

Whole entity in one round trip:

```python
import io
buf = io.BytesIO()
ent.export(buf, format="application/x-hdf5")
open("entity.h5", "wb").write(buf.getvalue())
```

---

## Mode A — direct h5py

Registration stamps three locator keys onto every entity's metadata — `path_<type>`,
`dataset_<type>`, and (batched layouts only) `index_<type>`. They tell you where the bytes
live so you can skip the server:

```python
md = dict(ent.metadata)
md["path_rixs_spectrum"]                  # 'sim_00042.h5'  — relative to the dataset's directory
md["dataset_rixs_spectrum"]               # '/spectra'      — HDF5 path inside that file
md.get("index_rixs_spectrum")             # 7 or absent     — row on axis 0, batched layouts only
```

`base_dir` is the `data.directory` from the dataset's YAML — the locator paths are relative
to it. Ask whoever onboarded the dataset, or read it from `datasets/<name>.yml`.

```python
import os
import h5py

def load(entity, artifact_type, base_dir):
    """Read one artifact directly. `index` must be applied BEFORE any user slice."""
    md = dict(entity.metadata)
    path = os.path.join(base_dir, md[f"path_{artifact_type}"])
    with h5py.File(path, "r", locking=False) as f:
        ds = f[md[f"dataset_{artifact_type}"]]
        index = md.get(f"index_{artifact_type}")
        return ds[int(index)] if index is not None else ds[...]
```

For the common case — query, then bulk-load every match — the broker ships this as
`clients/query_manifest.py`:

```python
from tiled_catalog_broker.clients.query_manifest import query_catalog, load_artifacts

manifest = query_catalog(hits, artifact_type="rixs_spectrum")   # DataFrame: params + locators
arrays = load_artifacts(manifest, artifact_type="rixs_spectrum", base_dir="/path/to/data")

X = np.stack(arrays)
Theta = manifest[["sigma", "gamma"]].to_numpy()                 # your choice of columns
```

`base_dir` is required — there is no inference from the catalog.

---

## Which dataset keys exist?

`list(c)` on a live server is authoritative. The YAMLs in `datasets/` are the configs this
repo has onboarded, and each one's `key:` field is the container key it registers into.

---

## See also

- `docs/ONBOARDING.md` — registering a *new* dataset (the authoring side of this contract)
- `docs/SLICING-EXPLAINER.md` — why sliced reads are cheap, and how batched arrays are served
- `examples/demo_query.py` — a marimo notebook of the Mode B flow, runnable against a server
