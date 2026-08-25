# The dataset YAML contract

One YAML describes exactly one dataset. This page describes its fields; to author one, see
[How to publish a dataset](../ONBOARDING.md), and to copy a working file, see
`datasets/examples/{per_entity,batched,grouped}.yml`.

Source: `src/tiled_catalog_broker/tools/_models.py`. Every field there carries a
`description`, and the models are validated on `tcb generate`.

## Strictness

| Section | Unknown keys |
|---|---|
| `data`, `artifacts`, `parameters`, `shared` | **rejected** — a typo is a hard error |
| `metadata` | allowed, by design; datasets add their own facets |
| top level | ignored (`provenance`, `extra_metadata`, `artifact_datasets` carry no structural rules) |

Required strings use `min_length=1`, so an empty string counts as missing, not as a value.

---

## Top level

| Field | Type | Required | Description |
|---|---|---|---|
| `label` | `str` | **yes** | Human-readable dataset name; the basis for the derived UID namespace |
| `key` | `str` | **yes** | Dataset container key in Tiled, e.g. `BROAD_SIGMA`. Written by [`tcb stamp-key`](cli.md#tcb-stamp-key) as `slug(label)` — not authored by hand |
| `metadata` | mapping | **yes** | Provenance and discovery facets |
| `data` | mapping | **yes** | Where the data lives and how entities are packed |
| `artifacts` | list | **yes** | The arrays each entity exposes; at least one |
| `parameters` | mapping | **yes** | How to extract each entity's physics parameters |
| `shared` | list | no | Axis arrays shared across all entities (default: empty) |

`key` is declared optional in the model and then enforced by a validator, so the error you
get for omitting it names `tcb stamp-key` rather than reading as a missing-field error.

---

## `metadata`

Queryable facets on the dataset container. Values are soft-checked against the semantic
model: a known alias is normalized to its canonical id, an unknown value warns and still
onboards (see [the soft vocabulary](../explanation/vocabulary.md)).

| Field | Type | Required | Description |
|---|---|---|---|
| `method` | `list[str]` | **yes** | Scientific methods or observables, e.g. `['RIXS']`; at least one |
| `data_type` | `str` | **yes** | `simulation` or `experimental` |
| `material` | `str` | **yes** | Target material or system, e.g. `NiPS3` |
| `producer` | `str` | no | Code that produced the data, e.g. `edrixs`; typically for simulations |
| `project` | `str` | no | Scientific project or collaboration |
| `facility` | `str` | no | Facility where the data was collected; typically for experiments |
| *(any other key)* | any | no | Becomes dataset container metadata |

Presence of `method`, `data_type`, and `material` is required. Only their **values** are
soft.

---

## `data`

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `directory` | `str` | **yes** | | Path to the data root, as **you** see it. `tcb generate` globs this |
| `layout` | enum | **yes** | | `per_entity`, `batched`, or `grouped` (see [the three layouts](../explanation/layouts.md)) |
| `file_pattern` | `str` | no | `**/*.h5` | Glob for HDF5 files under `directory` |
| `server_base_dir` | `str` | no | `None` | Path to the same data root as **the server** sees it. Stamped into each `data_uri`; falls back to `directory` when unset |

`server_base_dir` is set by hand. Nothing derives it, and no environment variable supplies
it. Omit it when the server runs on your filesystem; set it for any deployment where the
server mounts the data elsewhere.

---

## `artifacts`

A list; each entry becomes one array child of every entity.

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | `str` | **yes** | The array's key under its entity, e.g. `rixs_spectrum` |
| `dataset` | `str` | **yes** | HDF5 path to the array |

`dataset` resolves per layout: used as-is for `per_entity` and `batched`, and relative to
each entity group for `grouped`.

---

## `parameters`

| Field | Type | Required | Description |
|---|---|---|---|
| `location` | enum | **yes** | Where per-entity parameters are read from (below) |
| `group` | `str` | conditional | One parameter group; its fields become flat entity metadata |
| `groups` | `mapping[str, str]` | conditional | Named parameter groups; each name becomes a nested metadata key |
| `recursive` | `bool` | no | Descend into subgroups (default: `false`) |
| `exclude` | `list[str]` | no | Dataset or subgroup paths to skip (default: empty) |
| `entity_group` | `str` | no | HDF5 group holding one subgroup per entity (`grouped` layout); falls back to `samples`, then to the file's top-level groups |

`group` and `groups` are mutually exclusive. With `location: group`, exactly one
is required; both are invalid with `root_scalars` and `root_attributes`. Group
paths are absolute for `per_entity` and `batched`, and relative to each entity
group for `grouped`.

`groups` preserves the file structure as nested metadata. For example,
`groups: {instrument: /entry/instrument}` maps `Ei` to `instrument.Ei`.
`recursive: true` adds subgroup names at deeper levels. `exclude` paths use the
same path frame as the configured groups.

### `location` values

| Value | Parameters live in | Typical layout |
|---|---|---|
| `root_scalars` | scalar (0-dim) datasets at the file root | `per_entity` |
| `root_attributes` | root-level HDF5 attributes (`f.attrs`) | `per_entity` |
| `group` | scalar datasets in `group` or `groups`; in `batched`, length-*N* arrays provide one value per entity | `per_entity`, `batched` |
| `group_scalars` | scalar datasets in each entity group or its configured relative groups | `grouped` |

Only scalar datasets are parameters in `per_entity` and `grouped`. In `batched`,
a scalar applies to every entity and a one-dimensional array of batch length
provides one value per entity. Other arrays are ignored as parameters.

Scalar attributes on a parameter dataset become sibling labels named
`<field>_<attribute>`, such as `instrument.Ei_units`. They do not contribute to
the entity UID.

---

## `shared`

A list of axis arrays registered once as direct children of the dataset container, rather
than per entity — energy grids and the like.

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | `str` | **yes** | The array's key under the dataset, e.g. `energy` |
| `dataset` | `str` | **yes** | Absolute HDF5 path to an array shared by all entities |

Each one also lands on the dataset container's metadata as `shared_dataset_<type>`, so a
client can find the axis without listing children.

The generator records the first file containing the path. If other files contain
it, their values must match. The array is registered once under the dataset
container; its scalar HDF5 attributes become array metadata.
