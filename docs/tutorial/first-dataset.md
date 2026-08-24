# Register your first dataset

In about twenty minutes, we will register a synthetic dataset in a local Tiled
catalog, search it by a physics parameter, and read an array over HTTP. Using
generated data keeps the path reliable even when your own files are not ready.

**Before you start:** a Python 3.12+ environment with this package installed
([install `tcb`](../install.md)).

## 1. Generate the sample files

```bash
python examples/make_demo_dataset.py
```

```
Wrote 12 files to /path/to/tiled-catalog-broker/demo-data
  e.g. entity_0001.h5: /spectrum (64, 16), /curve (64,), params sigma, gamma, tenDq
```

The script creates twelve HDF5 files. Each file represents one entity and contains
two arrays plus three scalar parameters. Copy the printed path for step 3.

## 2. Start a server

In a **second terminal**, from the same directory:

```bash
tiled serve config config.demo.yml --api-key tutorialsecret
```

Leave it running. Back in your first terminal, tell the tools where it is:

```bash
export TILED_URL=http://127.0.0.1:8005
export TILED_API_KEY=tutorialsecret
```

## 3. Describe the data

Create `datasets/tutorial_demo.yml`, replacing the `directory` value with the path from
step 1:

```yaml
label: Demo Spectra

metadata:
  method: [RIXS]
  data_type: simulation
  material: NiPS3
  producer: edrixs
  project: MAIQMag

data:
  directory: /path/to/tiled-catalog-broker/demo-data
  file_pattern: "*.h5"
  layout: per_entity

parameters:
  location: root_scalars

artifacts:
  - { type: spectrum, dataset: /spectrum }
  - { type: curve,    dataset: /curve }

shared:
  - { type: energies, dataset: /energies }
```

This YAML says that each file is one entity, its parameters are root-level scalars,
and each entity exposes `spectrum` and `curve` arrays. `/energies` is identical in
every file, so it is declared as a shared axis and registered once for the whole
dataset rather than copied onto each entity.

## 4. Stamp the catalog key

```bash
tcb stamp-key datasets/tutorial_demo.yml
```

```
datasets/tutorial_demo.yml: stamped key 'DEMO_SPECTRA' (slug of label 'Demo Spectra')
```

The command writes `DEMO_SPECTRA`, the dataset's catalog key, into the YAML.

## 5. Build the manifests

```bash
tcb generate datasets/tutorial_demo.yml
```

```
Found 12 HDF5 files
Entities: 12 rows -> datasets/manifests/Demo Spectra/entities.parquet
Artifacts: 25 rows (1 shared axes) -> datasets/manifests/Demo Spectra/artifacts.parquet
```

Check the counts: twelve entities, two artifacts each, plus the one shared axis
makes 25 artifact rows. The two Parquet files now describe what the catalog will
register.

## 6. Register it

```bash
tcb register --upload datasets/tutorial_demo.yml
```

The end of the output should look like this:

```
Dataset 'DEMO_SPECTRA':
  metadata keys: 7
  shared axes: 1/1 registered as array children ['energies']
  entity containers: 12

Entity 'DEMO_SPECTRA_a9875d5c09c5d':
  metadata keys: 8 (locators: 2)
  artifact children: 2
    sample: ['spectrum', 'curve']

Artifact 'spectrum':
  shape: (64, 16)  dtype: float32
```

Your entity key will differ because it is derived from the file contents. The
`--upload` flag copies the arrays into the server's storage.

## 7. Find something and read it

```bash
python
```

```python
from tiled.client import from_uri
from tiled.queries import Key

c = from_uri("http://127.0.0.1:8005", api_key="tutorialsecret")
list(c)
```

```
['DEMO_SPECTRA']
```

Open the dataset and count what is under it:

```python
ds = c["DEMO_SPECTRA"]
len(ds)
```

```
13
```

Thirteen children: your twelve entities plus the `energies` axis. Read the axis
straight off the dataset container — it belongs to all of them, so it is stored once:

```python
ds["energies"].shape, ds["energies"][:4]
```

```
((64,), array([0.        , 0.12698413, 0.25396825, 0.38095238]))
```

Now search the entities by a physics parameter:

```python
hits = ds.search(Key("sigma") >= 0.05)
len(hits)
```

```
6
```

Six entities match. Inspect the first one:

```python
entity = hits.values().first()
dict(entity.metadata)
```

```
{'uid': 'a9875d5c09c5d958', 'gamma': 0.2, 'sigma': 0.0747653346366633,
 'tenDq': 2.0942448414759975, 'path_spectrum': 'entity_0003.h5',
 'dataset_spectrum': '/spectrum', 'path_curve': 'entity_0003.h5',
 'dataset_curve': '/curve'}
```

The three parameters are queryable fields. Now read an array:

```python
arr = entity["spectrum"]
arr.shape, arr.dtype
```

```
((64, 16), dtype('float32'))
```

```python
arr[0:5, :].shape
```

```
(5, 16)
```

Only those five rows crossed the wire; the server performed the slice.

## 8. See it in a notebook

```bash
export TCB_DEMO_DATASET=DEMO_SPECTRA
uv run --with marimo --with matplotlib marimo edit examples/demo_query.py
```

The notebook follows the same dataset → search → entity → array path and plots the
spectrum. It reuses the server settings from step 2.

## 9. Clean up

```bash
tcb delete DEMO_SPECTRA --yes
```

Stop the server with `Ctrl-C` in the second terminal, and remove the generated files:

```bash
rm -rf demo-data demo_catalog.db demo_storage datasets/tutorial_demo.yml
rm -rf "datasets/manifests/Demo Spectra"
```

## What you just did

You described the data once and produced three catalog levels:

- a **dataset** container, `DEMO_SPECTRA`, carrying the provenance you wrote in `metadata`
- twelve **entity** containers, each carrying `sigma`, `gamma`, and `tenDq` as queryable
  fields
- two **artifact** arrays under each entity, `spectrum` and `curve`
- one **shared axis**, `energies`, stored once beside the entities rather than twelve times

The broker did not know about `sigma` in advance. It carried that field from the
files through the manifests into searchable catalog metadata.

## Next

- [How to publish a dataset](../ONBOARDING.md) — the same pipeline, on your own files,
  including the layouts other than `per_entity`
- [How to prepare for the workshop](../workshop-prep.md) — if you are coming to the MAIQMag
  all-hands, this is what to have ready
- [The data model](../explanation/data-model.md) — why the catalog is shaped this way
