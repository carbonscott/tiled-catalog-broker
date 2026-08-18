# Serving Slices of Multi-Dimensional Data with Tiled

**Audience:** Researchers familiar with tabular data (DataFrames, SQL) who
want to understand how we serve image-like data from batched HDF5 files.

---

## 1. The Problem: Tables Where Each "Cell" Is an Image

In tabular data, one file holds many rows and each cell is a number:

```
id  | temperature | pressure
----|-------------|--------
0   | 300.5       | 1.01
1   | 301.2       | 1.03
```

Our physics simulation data is different. One HDF5 file holds 10,000
spectra, but each "row" is a 2D array of shape `(151, 40)`:

```
spectra: shape (10000, 151, 40)
         ─────  ───────────────
         rows   each row is a 151x40 image
```

We need to serve each spectrum individually over HTTP, as if each one
were its own record.

## 2. The Analogy

| Tabular world | Our HDF5 world |
|---|---|
| One CSV with 10K rows | One HDF5 file with 10K spectra |
| Each row is a flat record | Each "row" is a `(151, 40)` image |
| `SELECT * WHERE id = 42` | `file["spectra"][42]` -> `(151, 40)` |
| Row filtering in SQL | `slice="42"` in Tiled |

The key insight: **slicing a multi-dimensional array is the HDF5
equivalent of a `WHERE` clause on a table.**

## 3. How It Works

### What the data looks like on disk

All 10,000 spectra live in a single HDF5 file:

```
NiPS3_combined_2.h5
  /spectra          shape: (10000, 151, 40)    <- the "table"
  /params/F2_dd     shape: (10000,)            <- parameter column
  /params/F4_dd     shape: (10000,)            <- parameter column
  ...
```

### What happens at registration time

When we register entity #42, we tell Tiled:

```python
# Our manifest says: index = 42
# We translate to Tiled's language:
parameters = {
    "dataset": "spectra",   # which "table" in the file
    "slice": "42",          # which "row" to serve
}
```

This is stored in Tiled's catalog database. The user never sees it.

### What happens at query time

```
User request:     client["EDRIXS"]["EDRIXS_636ce3e41ea05"]["rixs"][:]
                         |
Tiled server:     Opens HDF5 -> loads spectra as dask array (10000, 151, 40)
                         |
                  Applies slice="42" -> array[42] -> (151, 40)
                         |
Returns:          numpy array, shape (151, 40) -- one spectrum
```

The user gets back exactly their spectrum. They don't know (or need to
know) it came from row 42 of a batched file.

### How the keys are named

Access is three levels — `client[DATASET][ENTITY][ARTIFACT]` — and every
key is human-readable, derived deterministically (no opaque UUIDs in the path):

- **Dataset key**: `slugify_key(label)` — the dataset's `label` upper-snake-cased
  (`"EDRIXS RIXS"` -> `EDRIXS_RIXS`), written into the YAML by `tcb stamp-key`.
- **Entity key**: `{dataset_key}_{uid[:13]}`, where `uid` is the content hash of the
  entity's parameters (e.g. `EDRIXS_636ce3e41ea05`).
- **Artifact key**: the artifact's `type` from the manifest (e.g. `rixs`).

See [ONBOARDING.md](ONBOARDING.md) for how `label`, parameters, and artifact `type`
are declared in the dataset YAML.

## 4. Two Ways to Access the Data

We provide two modes because different users have different needs:

### Mode A -- Direct access (for ML pipelines)

Read the locator from metadata, load with h5py yourself:

```python
h = client["EDRIXS"]["EDRIXS_636ce3e41ea05"]
meta = h.metadata

# The locator: (file, dataset, index)
path    = meta["path_rixs"]       # "NiPS3_combined_2.h5"
dataset = meta["dataset_rixs"]    # "spectra"
index   = meta["index_rixs"]      # 42

with h5py.File(f"{base_dir}/{path}") as f:
    spectrum = f[dataset][index]   # shape (151, 40), ~18ms
```

This is fast (~18ms) because you read directly from the file. Good for
loading thousands of spectra in a training loop.

### Mode B -- HTTP access (for visualization)

Just ask Tiled for the data:

```python
h = client["EDRIXS"]["EDRIXS_636ce3e41ea05"]
spectrum = h["rixs"][:]            # shape (151, 40), ~300ms
```

This is simpler but slower (~300ms) because data travels over HTTP.
Good for interactive exploration in notebooks.

### Comparison

| | Mode A (Direct) | Mode B (HTTP) |
|---|---|---|
| Code complexity | 5 lines | 2 lines |
| Latency | ~18ms | ~300ms |
| Bulk access | `f["spectra"][0:100]` in one read | One HTTP call per entity |
| Best for | ML training, batch processing | Notebooks, visualization |

## 5. What the Manifest Looks Like

The manifest is a Parquet file -- one row per entity-artifact pair:

```
uid               | type | file                 | dataset  | index
------------------|------|----------------------|----------|------
636ce3e41ea05f0f  | rixs | NiPS3_combined_2.h5  | spectra  | 0
a1b2c3d4e5f60718  | rixs | NiPS3_combined_2.h5  | spectra  | 1
0f1e2d3c4b5a6978  | rixs | NiPS3_combined_2.h5  | spectra  | 2
...               | ...  | ...                  | ...      | ...
9f8e7d6c5b4a3210  | rixs | NiPS3_combined_2.h5  | spectra  | 9999
```

This is the **interface boundary**. The data provider fills in the
manifest; the broker reads it generically. The broker doesn't need to
know what "rixs" means or what shape the data is -- it reads everything
dynamically. The `uid` is the content hash of the entity's parameters;
the human-readable entity key (`EDRIXS_636ce3e41ea05`) is derived from it
plus the dataset key at registration time.

## 6. Scale

All 10,000 entities register over HTTP via `tcb register` (the single
registration route). The catalog database is ~5 MB. All 10K artifacts
share one HDF5 file (3.6 GB) and one structure definition — registration
stores only the locator (file, dataset, index) per entity, never the array data.

## 7. Summary

- Multi-dimensional data needs **slicing**, not row filtering
- Tiled's `slice` parameter handles this natively -- no custom code needed
- The manifest uses `index` (our language); registration translates it
  to `slice` (Tiled's language)
- Users of either mode never need to know about the slicing -- they get
  back one clean array per entity
