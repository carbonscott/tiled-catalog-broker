# The Parquet manifest

`tcb generate` writes two Parquet files per dataset; `tcb register` reads them and nothing
else. They are the contract between the two halves of the pipeline.

```
<yaml_dir>/manifests/<label>/
├── entities.parquet
└── artifacts.parquet
```

## `entities.parquet`

One row per entity.

| Column | Required | Description |
|---|---|---|
| `uid` | **yes** | Entity id. It is content-addressed when parameters exist and uses a deterministic positional fallback otherwise. The Tiled key is `{dataset_key}_{uid[:13]}` |
| *(any others)* | no | Become the entity container's queryable metadata |

Every extra column is carried through verbatim, which is what makes the broker
dataset-agnostic: no parameter name is known to it.

## `artifacts.parquet`

One row per entity artifact, plus one row per shared axis.

| Column | Required | Description |
|---|---|---|
| `uid` | conditional | Parent entity id. Null for a shared axis, whose parent is the dataset container |
| `type` | **yes** | The artifact's key under the entity, e.g. `rixs_spectrum` |
| `file` | **yes** | Path to the HDF5 file, relative to `data.directory` |
| `dataset` | **yes** | HDF5 path to the array inside that file |
| `index` | no | Row on axis 0, for `batched` layouts |
| `file_size` | no | Source file size in bytes; written by the current generator |
| `file_mtime` | no | Source file modification time; written by the current generator |
| `shape` | **yes** | JSON-encoded shape **as registered** — for `batched`, the leading axis is already dropped |
| `dtype` | **yes** | numpy dtype string, e.g. `float32` |
| *(any others)* | no | Become the artifact's metadata |

Shared-axis rows are registered once under the dataset container. Their `index`
and `uid` values are null.

## Snapshot semantics

`shape` and `dtype` are recorded at generate time, so registration never opens an HDF5
file — it reads the manifest and nothing else (see [sliced
reads](../explanation/sliced-reads.md)). The manifest is therefore a snapshot, and
registration trusts it.

- A manifest that no longer matches the data is not detected at registration. The read
  adapter re-checks both against the file and raises on a mismatch, so the discrepancy
  surfaces as HTTP 500 on read rather than as silently wrong data. `tcb generate` rebuilds
  the snapshot; it is idempotent.
- A manifest with no `shape`/`dtype` columns is rejected at registration, with a message
  naming the command that regenerates it.

## Entity metadata written at registration

Registration stamps locator keys onto each entity's metadata, one set per artifact `type`:

| Key | Value |
|---|---|
| `path_<type>` | the manifest's `file` — relative to `data.directory` |
| `dataset_<type>` | the manifest's `dataset` |
| `index_<type>` | the manifest's `index`; absent for non-batched layouts |

These are relative-path provenance, kept so that entities can be *searched* by where they
came from. They are not the mechanism for opening a file — for that, ask the catalog for the
data source, as [How to read a registered catalog](../using-the-catalog.md) shows.
