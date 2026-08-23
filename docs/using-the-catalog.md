# How to read a registered catalog

Someone has registered a dataset and given you the server's `<URL>` and an `<API_KEY>`.
Getting it to that point is [How to publish a dataset](ONBOARDING.md).

There are two ways to get at an array, and the catalog records enough for both:

| | How | Use when |
|---|---|---|
| [**Through the server**](#read-an-array) | Arrays over HTTP through the Tiled client | Anywhere the server is reachable, and you want a slice rather than the whole file |
| [**Straight from the files**](#read-the-files-directly) | Ask the catalog where the bytes live, then open the HDF5 with h5py | You are on the same filesystem as the data and want bulk reads at full speed |

Start with the server: it needs only the Tiled client (`pip install 'tiled[client]'`),
imports none of this package, and works the same either way a dataset was registered.
Everything up to [Read an array](#read-an-array) is that path.

## Connect and list

```python
from tiled.client import from_uri
from tiled.queries import Key

c = from_uri("<URL>", api_key="<API_KEY>")

list(c)                                   # dataset keys: ['BROAD_SIGMA', 'LCLS_RIXS_STATIC', ...]
ds = c["BROAD_SIGMA"]                     # a dataset container
dict(ds.metadata)                         # provenance: method, data_type, material, producer
len(ds)                                   # entity count (+ one child per shared axis)
```

`list(c)` on a live server is the authoritative list of keys. Dataset containers sit at the
root and everything else is beneath one, so always index the dataset first.

## Shared axes

**Shared axes** (arrays identical for every entity — an energy grid, a field axis) sit
beside the entities as array children of the dataset, keyed by their `type` and listed in
its metadata as `shared_dataset_<type>`. So iterating children is not iterating entities —
separate them first:

```python
shared = {k.removeprefix("shared_dataset_") for k in ds.metadata if k.startswith("shared_dataset_")}
eloss = ds["eloss"][:]                    # (151,) — one read, not one per entity
entities = [k for k in ds if k not in shared]
```

## Search

Search on any parameter the entities carry, and chain as many as you like:

```python
hits = ds.search(Key("sigma") >= 0.04).search(Key("sigma") <= 0.05)
hits = hits.search(Key("gamma") == 0.1)
len(hits)

ent = hits.values().first()
dict(ent.metadata)                        # physics parameters, plus provenance fields
list(ent)                                 # artifact keys under this entity
```

> Only `Key` comparisons (`==`, `>=`, `<=`, …) are SQL-served. **Never use `Regex`** — it is
> not SQL-backed, so it silently degrades to filtering every entity client-side.

Nested parameters — the ones a dataset declared with
[`parameters.groups`](reference/dataset-yaml.md#parameters) — are queried with a dotted key:

```python
hot = ds.search(Key("instrument.Ei") > 50)
nips3 = ds.search(Key("sample.chemical_formula") == "NiPS3")
```

Each entity also carries `path_<type>`, `dataset_<type>`, and (batched only)
`index_<type>` — which file and row it came from. So provenance is searchable
(`Key("path_rixs_spectrum") == "sim_00042.h5"`), and it is what
[reading the files directly](#read-the-files-directly) uses.

## Read an array

Slice an artifact and only those bytes cross the wire:

```python
arr = ent["rixs_spectrum"]
arr.shape                                 # (151, 40)
arr[0:5, :]                               # numpy array
```

The server does the slice, so five rows of a ten-thousand-entity batched file reads five
rows (see [sliced reads](explanation/sliced-reads.md)).

Whole entity in one round trip:

```python
import io
buf = io.BytesIO()
ent.export(buf, format="application/x-hdf5")
open("entity.h5", "wb").write(buf.getvalue())
```

---

## Read the files directly

HTTP is enough for interactive work, not for a training loop pulling thousands of arrays
where the per-request hop dominates. If you are already on the filesystem that holds the
data, the locator metadata tells you where the bytes are:

```python
md = dict(ent.metadata)
md["path_rixs_spectrum"]                  # 'sim_00042.h5'  — relative to the dataset's directory
md["dataset_rixs_spectrum"]               # '/spectra'      — HDF5 path inside that file
md.get("index_rixs_spectrum")             # 7 or absent     — row on axis 0, batched layouts only
```

Locator paths are relative to `base_dir`, the `data.directory` from the dataset's YAML.
Ask whoever onboarded it.

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

None of this works for a dataset registered with `tcb register --upload` — the server holds
the bytes and there is no external file to open. Read through the server instead; same
array.

---

Next: [explore a dataset in a notebook](exploring-your-data.md) — both paths, with plots.
