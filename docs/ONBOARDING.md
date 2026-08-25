# How to publish a dataset

Turn a directory of HDF5 files into a dataset in a Tiled catalog: searchable by its
physics, readable from anywhere.

**Before you start:** [install `tcb`](install.md), arrange the files in one of the
[three supported layouts](explanation/layouts.md), and set `TILED_URL` and `TILED_API_KEY`.

## Pick a transport first

This decision changes two steps below; everything else is the same either way.

| | The server reads your files | You upload the arrays |
|---|---|---|
| **Command** | `tcb register` | `tcb register --upload` |
| **Use when** | The data is on a filesystem the Tiled server can open — the normal case for facility storage | The server cannot see your disk — a laptop, a university cluster |
| **What is registered** | Pointers. No array bytes move | The arrays themselves, streamed into the server's storage |
| **Afterwards** | The files stay where they are, and stay load-bearing | The server owns the bytes; your local files are no longer involved |

A dataset is one transport or the other — `tcb register` refuses to mix them.

<figure class="tcb-diagram">
--8<-- "diagrams/pipeline.svg"
<figcaption>What each step reads and writes. Only the last one needs a running server.</figcaption>
</figure>

## 1. Author the dataset YAML

The YAML is the contract: enough about your files to register them without knowing
anything about your science. `tcb generate` *reads* it — nothing generates it. Leave `key`
for step 2.

### With an agent (recommended)

A clone ships an `/onboarding` skill at `.claude/skills/onboarding/`. From the repository
root in Claude Code:

```
/onboarding
```

Tell it where your data is and what you know about it — the instrument or code that
produced it, the material, a paper, a README. It opens your HDF5 with `h5py` and drafts the
YAML from what is there: layout, artifacts, parameters, shared axes. Uploading? Add one
line:

> This is remote registration — the server cannot see my filesystem, so we will use
> `tcb register --upload`.

**You are still the reviewer.** An agent reads shapes and dtypes off your files; `method`,
`material`, and `producer` are provenance claims only you can confirm.

### By hand

```bash
cp datasets/examples/per_entity.yml datasets/my_dataset.yml
```

Then edit:

- **`label`** — distinctive. On a shared server, include your surname: `Okafor NiPS3
  Powder`, not `My Dataset`.
- **`data`** — `directory`, `layout`, and `file_pattern` if the default `**/*.h5` misses.
- **`metadata`** — `method`, `data_type`, `material` required. Prefer ids from
  `src/tiled_catalog_broker/tools/schema/catalog_model.yml`; unknown ones warn, not fail.
- **`artifacts`** — one per array an entity exposes. Arrays common to *every* entity, like
  an energy axis, go under `shared`.
- **`parameters`** — `location`, plus the matching group fields.

NeXus files start from `datasets/examples/per_entity_nexus.yml`. Full field list:
[the dataset YAML reference](reference/dataset-yaml.md). Naming:
[the soft vocabulary](explanation/vocabulary.md).

### The two fields the transport changes

=== "Server reads the files"

    `data.directory` is the root as **you** see it; `data.server_base_dir` is the same
    root as **the server** sees it. Step 3 gets them to agree.

=== "Upload from my machine"

    `data.directory` is the path on **your** machine — where `--upload` reads from. Omit
    `data.server_base_dir`; the server never opens your files.

    ```yaml
    # datasets/mydata.yml (abridged)
    label: My Dataset
    metadata:
      method: [INS]
      data_type: experimental
      material: NiPS3
    data:
      directory: /home/me/experiments/run42   # local to YOU
      layout: per_entity
      file_pattern: "*.h5"
    parameters:
      location: root_scalars
    artifacts:
      - type: spectrum
        dataset: /spectrum
    ```

## 2. Stamp the key and generate the manifests

```bash
tcb stamp-key datasets/my_dataset.yml
tcb generate datasets/my_dataset.yml
```

`stamp-key` derives the catalog key from `label` and writes it in. `generate` validates the
contract — it refuses to run before the key is stamped — then records each artifact's path,
shape, and dtype in [two Parquet manifests](reference/manifest.md). Nothing has touched the
network yet.

**Check the reported entity and artifact counts against your files.** If they are wrong,
fix the YAML and regenerate — usually a broad `file_pattern`, the wrong batched axis, or
the wrong entity group.

## 3. Make local and server paths agree

<a id="paths-where-your-view-and-the-servers-view-differ"></a>

=== "Upload from my machine"

    Nothing to do — no path has to agree with anything, `data.server_base_dir` is ignored,
    and the stock Tiled adapter serves the uploaded arrays.

    The receiving server needs `writable_storage`,
    [Tiled's own setting](https://blueskyproject.io/tiled/) rather than a broker one.
    `config.demo.yml` is a working example.

=== "Server reads the files"

    Three settings have to describe the same bytes:

    <figure class="tcb-diagram">
    --8<-- "diagrams/paths.svg"
    <figcaption>Two of the three are in your dataset YAML; the third is the server's own config.</figcaption>
    </figure>

    | Setting | Filesystem | Purpose |
    |---|---|---|
    | `data.directory` | yours | Root scanned by `tcb generate` |
    | `data.server_base_dir` | server | Equivalent root stored in catalog pointers |
    | `readable_storage` in `config.yml` | server | Roots the server may read |

    Keep the path below each root identical:

    ```yaml
    data:
      directory: /home/me/rixs/data/my_dataset
      server_base_dir: /mnt/shared/rixs/data/my_dataset
    ```

    - Add the **server-side** root to `readable_storage`.
    - Use physical paths (`pwd -P`, `readlink -f`), not symlinks.
    - Sharing a filesystem? Omit `server_base_dir`; it defaults to `data.directory`.

    The server also needs the broker's array adapter, or every read returns HTTP 500 after
    a registration that looked fine:

    ```yaml
    adapters_by_mimetype:
      application/x-hdf5-broker: "tiled_catalog_broker.adapters.lazy_hdf5:LazyHDF5ArrayAdapter"
    ```

    It is an import path resolved server-side, so the broker must be importable in the
    *server's* environment. This repository's `config.yml` has it already; see
    [sliced reads](explanation/sliced-reads.md).

## 4. Register the dataset

To run a server locally:

```bash
tiled serve config config.yml --api-key secret
```

Register a small sample first, then the rest:

=== "Server reads the files"

    ```bash
    tcb register -n 5 datasets/my_dataset.yml
    tcb register datasets/my_dataset.yml
    ```

    No array bytes move: registration reads the manifest and never opens an HDF5 file,
    which is what makes ten thousand entities cheap.

=== "Upload from my machine"

    ```bash
    tcb register --upload -n 5 datasets/my_dataset.yml
    tcb register --upload datasets/my_dataset.yml
    ```

    This moves the real data, so wall-clock scales with size and uplink, not entity count.
    Parameters become queryable metadata, arrays stream to the server, shared axes stream
    once each, and the container is stamped `storage: uploaded`.

Either way registration is incremental — an existing entity is skipped, so the second
command continues from the first and an interrupted run resumes.

Regenerate first if a shape or dtype changed. After changing any path in the YAML:
regenerate, `tcb delete`, register again — existing artifacts are never rewritten.

## 5. Read it back

Registration never opens a pointer-registered array, so a read through the server is the
real check. From any machine that can reach it:

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8005", api_key="secret")
ds = client["MY_DATASET"]              # the stamped key

# A dataset's children are its entities *plus* one array per shared axis, so drop
# the axes before taking "the first entity".
shared = {k.removeprefix("shared_dataset_") for k in ds.metadata if k.startswith("shared_dataset_")}
entity = ds[next(k for k in ds if k not in shared)]

print(dict(ds.metadata))               # the provenance you wrote in `metadata`
print(dict(entity.metadata))           # this entity's physics parameters
print(entity["spectrum"][:].shape)     # the array itself
```

Metadata on the dataset, parameters on the entity, an array at the shape you registered:
the whole contract, proven. Collaborators need nothing beyond the Tiled client.

Next: [read a registered catalog](using-the-catalog.md), or
[explore it in a notebook](exploring-your-data.md).

## 6. Start over

```bash
tcb delete MY_DATASET
```

This removes the catalog entries — and, for an uploaded dataset, the arrays the server
stored, since the catalog is their only home. Your HDF5 files are never touched.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `missing 'key' field` | Run `tcb stamp-key` first |
| `stored key ... does not match slug(label)` | Restore the label, or remove `key` and stamp it again |
| `manifests not found` | Run `tcb generate` with this YAML |
| Entity or artifact counts are wrong | Check `file_pattern`, the batched leading axis, or `entity_group` |
| Parameters are missing | Correct `parameters.location`, `group`, or `groups` |
| Registration succeeds but every read returns HTTP 500 | Either the server is missing `adapters_by_mimetype` (step 3), or `server_base_dir` / `readable_storage` / a symlinked path is wrong. Fix, then regenerate, delete, and register again |
| Only *some* reads return HTTP 500 | Those files changed shape or dtype since `tcb generate`. Regenerate |
| Entity count lower than expected | An interrupted run. Re-run the same command; it skips what is already there |

Every other message with a specific cause is in
[errors and warnings](reference/errors.md); flags and exit codes are in
[the `tcb` reference](reference/cli.md).
