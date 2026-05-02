# Data Ingestion Guide

**Audience:** Anyone registering a new dataset with the generic data broker.
**Prerequisite:** Access to the data files and a working Python environment with `uv`.

---

## Quick-Reference Checklist

For experienced users who have done this before:

- [ ] **Assess:** Talk to the data provider — what parameters, what artifacts, what file layout?
- [ ] **Explore:** Open the files yourself, inspect shapes/dtypes/structure.
- [ ] **Pattern:** Identify which data pattern applies (A: one file per artifact, B: batched, C: multi-artifact per file).
- [ ] **Generator:** Write a manifest generator script (in your application directory) that produces the two Parquet files.
- [ ] **Entities Parquet:** One row per entity. Required columns: `uid`, `key`. All other columns become metadata.
- [ ] **Artifacts Parquet:** One row per artifact. Required columns: `uid`, `type`, `file`, `dataset`. Optional: `index`.
- [ ] **Key rule:** `type` must be unique per `uid`.
- [ ] **Generate:** Run your generator, inspect the Parquet output.
- [ ] **Config:** Write a dataset YAML config with `key`, `base_dir`, and `metadata`. Filename stem must match Parquet prefix.
- [ ] **Server config:** Add `base_dir` to `readable_storage` in `config.yml`.
- [ ] **Ingest:** `broker-ingest datasets/<name>.yaml`
- [ ] **Verify:** Spot-check with a Tiled client — container exists, metadata correct, arrays load.

If anything in the checklist is unclear, read the full guide below.

---

## Step 0: Assess the Dataset

When someone says "I have new simulated data," start with a conversation. Ask
them these questions:

### Questions for the data provider

1. **What are the physics parameters per entity?**
   Examples: `Ja_meV, Jb_meV, Jc_meV` (VDP), `F2_dd, F4_dd, tenDq` (EDRIXS).
   These become searchable metadata in Tiled.

2. **What artifact types does each entity produce?**
   Examples: magnetization curves, INS spectra, ground states, RIXS spectra.
   Each becomes a named child in the Tiled catalog.

3. **How are the HDF5 files organized?**
   - One small file per artifact? (like VDP: 110K files)
   - One big file with many entities batched along axis 0? (like EDRIXS: 10K spectra in one file)
   - One file per entity with multiple datasets inside? (like NiPS3 Multimodal)

4. **How many entities and artifacts total?**
   This determines whether you need bulk ingestion (~1,800 nodes/sec) or
   incremental HTTP registration (~5 nodes/sec).

5. **Where do the files live on disk?**
   Get the full path. The broker needs read access.

6. **Is there already a manifest or parameter table?**
   Sometimes the simulator produces a CSV, Parquet, or HDF5 attribute table.
   This can save significant work.

### Record the answers

Jot down the answers — they directly inform every subsequent step. A simple
text note is fine:

```
Dataset: SpinWave simulations (from Alice)
Parameters: J1, J2, K, D (4 scalars per entity)
Artifacts: dispersion (512x512 image), dos (1D array, 200 points)
Layout: one HDF5 file per entity, two datasets inside (/dispersion, /dos)
Count: ~5,000 entities
Location: /sdf/data/.../spinwave/results/
Existing manifest: CSV with J1,J2,K,D columns and filenames
```

---

## Step 1: Explore the Dataset

Before writing any code, open the actual files and look around. The data
provider's description is a starting point, but the files themselves are the
ground truth.

**The goal is to build your own understanding of:**
- How many files there are and how they are named
- What is inside each HDF5 file (groups, datasets, attributes)
- The shapes and dtypes of the arrays
- Whether the data is clean (no NaNs, consistent shapes, etc.)

### Use whatever tools you are comfortable with

There is no single right way to explore a dataset. Here are some options:

**Python with h5py** (one example to get you started):

```python
import h5py, os, glob

files = sorted(glob.glob("/path/to/data/*.h5"))
print(f"Found {len(files)} files")
print(f"First file: {files[0]} ({os.path.getsize(files[0]) / 1e6:.1f} MB)")

with h5py.File(files[0], "r") as f:
    def show(name, obj):
        if isinstance(obj, h5py.Dataset):
            print(f"  {name}: shape={obj.shape}, dtype={obj.dtype}")
        elif isinstance(obj, h5py.Group):
            print(f"  {name}/ (group)")
    f.visititems(show)
```

**Other approaches that work well:**
- `h5dump -H file.h5` — command-line, shows full HDF5 structure
- HDFView — GUI tool for browsing HDF5 files
- `pandas.read_parquet()` — if there is an existing manifest/table
- Jupyter notebook — good for interactive plotting and inspection
- Any tool or workflow you prefer

### What to look for

- **Consistency:** Do all files have the same internal structure? Pick a few at
  random and compare.
- **Shapes:** Are array dimensions what the provider described? Are they
  consistent across files?
- **Parameters:** Are they stored as HDF5 attributes, separate datasets, or in
  an external table?
- **Surprises:** NaN values, empty datasets, unexpected groups, files that
  don't match the naming pattern.

### Document your findings

Write down what you found. This becomes the basis for your manifest generator.
For example:

```
Explored 3 of 5,000 files.
Each file: /params group with scalar datasets (J1, J2, K, D), /dispersion (512, 512), /dos (200,).
All consistent. No NaNs. Parameters are float64.
Filenames: sw_00001.h5, sw_00002.h5, ... (zero-padded 5-digit IDs).
```

If you find problems at this stage, see [When the Data Doesn't Fit](#when-the-data-doesnt-fit) before proceeding.

---

## Step 2: Understand the Manifest Contract

The broker is **fully generic** — it never hardcodes parameter names or
artifact types. The only interface between you and the broker is two Parquet
files with a small set of standard columns.

### Entity manifest

One row per entity. Two required columns; everything else is free-form.

| Column | Required | Description |
|--------|----------|-------------|
| `uid` | **Yes** | Unique entity ID (string) |
| `key` | **Yes** | Tiled catalog key (must be unique across all datasets) |
| *(all other columns)* | Dynamic | Become Tiled metadata as-is |

Example:

| uid | key | J1 | J2 | K | D |
|------|-----|----|----|---|---|
| sw_00001 | H_sw_00001 | 1.5 | 0.3 | 0.01 | 0.05 |
| sw_00002 | H_sw_00002 | 2.0 | 0.1 | 0.02 | 0.10 |

### Artifact manifest

One row per artifact (one logical data array). Five standard columns.

| Column | Required | Description |
|--------|----------|-------------|
| `uid` | **Yes** | Foreign key to parent entity |
| `type` | **Yes** | Artifact name — must be **unique per uid** |
| `file` | **Yes** | Path to HDF5 file (relative to data directory) |
| `dataset` | **Yes** | HDF5 internal dataset path (e.g., `/dispersion`) |
| `index` | No | Row index for batched files; null for single-entity files |
| *(all other columns)* | Dynamic | Become artifact metadata as-is |

Example:

| uid | type | file | dataset | index |
|------|------|------|---------|-------|
| sw_00001 | dispersion | sw_00001.h5 | /dispersion | |
| sw_00001 | dos | sw_00001.h5 | /dos | |
| sw_00002 | dispersion | sw_00002.h5 | /dispersion | |
| sw_00002 | dos | sw_00002.h5 | /dos | |

### The three key rules

1. **`type` must be unique per `uid`.** It becomes the Tiled child key. If
   the same entity has two magnetization curves along different axes,
   disambiguate: `mh_powder_30T` and `mh_x_7T`, not both `mh_curve`.

2. **`file` + `dataset` + `index` form a self-contained locator.** Given these
   three values, anyone can load exactly one artifact without knowing anything
   about the dataset's conventions.

3. **All non-standard columns become metadata automatically.** The broker reads
   every column it doesn't recognize and stores it as Tiled metadata. You don't
   need to configure this anywhere.

For the full specification with examples from all three existing datasets, see
[`LOCATOR-AND-MANIFEST-CONTRACT.md`](LOCATOR-AND-MANIFEST-CONTRACT.md).

---

## Step 3: Identify Your Data Pattern

Most datasets fall into one of three patterns. Identify which one applies —
it determines how you fill in the `file`, `dataset`, and `index` columns.

### Pattern A: One file per artifact

Each artifact is a separate small HDF5 file. The `index` column is always null.

```
data/
  artifacts/H001_mh.h5      → one array inside
  artifacts/H001_gs.h5      → one array inside
  artifacts/H002_mh.h5
  ...
```

| uid | type | file | dataset | index |
|------|------|------|---------|-------|
| H001 | mh | artifacts/H001_mh.h5 | /curve | |
| H001 | gs | artifacts/H001_gs.h5 | /gs | |

**Reference implementation:** See your application directory's generators.

### Pattern B: Batched file (many entities in one file)

One large HDF5 file stores many entities along axis 0. The `index` column
selects one row.

```
data/
  all_spectra.h5             → dataset shape (10000, 151, 40)
```

| uid | type | file | dataset | index |
|------|------|------|---------|-------|
| H0000 | rixs | all_spectra.h5 | /spectra | 0 |
| H0001 | rixs | all_spectra.h5 | /spectra | 1 |
| H0002 | rixs | all_spectra.h5 | /spectra | 2 |

**Reference implementation:** See your application directory's generators.

### Pattern C: One file per entity, multiple datasets inside

Each entity has its own file containing multiple artifact datasets.
The `index` column is null; the `dataset` column varies.

```
data/
  401.h5    → /dispersion (512, 512), /dos (200,), /params/{J1,J2,...}
  402.h5
  ...
```

| uid | type | file | dataset | index |
|------|------|------|---------|-------|
| mm_401 | dispersion | 401.h5 | /dispersion | |
| mm_401 | dos | 401.h5 | /dos | |

**Reference implementation:** See your application directory's generators.

### Mixed patterns

Some datasets combine patterns (e.g., batched files with multiple dataset
paths). The manifest handles this naturally — each row specifies its own
`file`, `dataset`, and `index` independently.

---

## Step 4: Write the Manifest Generator

Manifest generators are dataset-specific scripts that live in your application
directory (not in this broker repo). Write a script that produces two Parquet
files conforming to the manifest contract. A common interface is:

```python
def generate(output_dir, n_entities=None):
    """Generate manifests in the generic broker standard.

    Args:
        output_dir: Directory to write Parquet files.
        n_entities: Limit the number of entities (None = all).

    Returns:
        (ent_df, art_df): Entity and artifact DataFrames.
    """
```

### Template

Here is a minimal starting point. Adapt it to your dataset.

```python
"""
Generate {Name} manifests in the generic broker standard.

Source data: /path/to/data/
"""

from pathlib import Path

import h5py
import pandas as pd


DATA_DIR = "/path/to/data"


def generate(output_dir, n_entities=None):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    ent_rows = []
    art_rows = []

    # --- Iterate over your data and populate ent_rows / art_rows ---
    # Each ent_row must include "uid" and "key" at minimum.
    # (This part is dataset-specific. See examples below.)

    ent_df = pd.DataFrame(ent_rows)
    art_df = pd.DataFrame(art_rows)

    # Write Parquet
    ent_df.to_parquet(output_dir / "{name}_entities.parquet", index=False)
    art_df.to_parquet(output_dir / "{name}_artifacts.parquet", index=False)

    print(f"Wrote {len(ent_df)} entities, {len(art_df)} artifacts")
    return ent_df, art_df
```

### Worked example: hypothetical "SpinWave" dataset

Suppose Alice gives you 5,000 HDF5 files, each containing:
- Scalar parameters `J1, J2, K, D` as datasets under `/params/`
- A dispersion image at `/dispersion` (shape 512x512)
- A density of states at `/dos` (shape 200)

This is **Pattern C** (one file per entity, multiple datasets inside).

```python
"""
Generate SpinWave manifests in the generic broker standard.

Source data: /sdf/data/.../spinwave/results/*.h5
"""

import glob
from pathlib import Path

import h5py
import pandas as pd


DATA_DIR = "/sdf/data/.../spinwave/results"
PARAM_NAMES = ["J1", "J2", "K", "D"]
ARTIFACT_MAP = {
    "dispersion": "/dispersion",
    "dos":        "/dos",
}


def generate(output_dir, n_entities=None):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(glob.glob(f"{DATA_DIR}/*.h5"))
    if n_entities is not None:
        files = files[:n_entities]

    ent_rows = []
    art_rows = []

    for filepath in files:
        # Derive uid from filename: sw_00001.h5 → sw_00001
        uid = Path(filepath).stem
        rel_path = Path(filepath).name  # relative to DATA_DIR

        with h5py.File(filepath, "r") as f:
            # Read parameters
            params = {p: float(f[f"params/{p}"][()]) for p in PARAM_NAMES}

        ent_rows.append({"uid": uid, "key": f"H_{uid[:8]}", **params})

        # One artifact row per type
        for art_type, dataset_path in ARTIFACT_MAP.items():
            art_rows.append({
                "uid":     uid,
                "type":    art_type,
                "file":    rel_path,
                "dataset": dataset_path,
            })

    ent_df = pd.DataFrame(ent_rows)
    art_df = pd.DataFrame(art_rows)

    ent_df.to_parquet(output_dir / "spinwave_entities.parquet", index=False)
    art_df.to_parquet(output_dir / "spinwave_artifacts.parquet", index=False)

    print(f"Wrote {len(ent_df)} entities, {len(art_df)} artifacts")
    return ent_df, art_df
```

Note: the output filenames (`spinwave_entities.parquet`, `spinwave_artifacts.parquet`)
must match the dataset config filename stem (`datasets/spinwave.yaml`). See
[Step 6](#step-6-write-a-dataset-config) for why.

### Tips

- **Start small.** Test with `n_entities=5` before processing the full
  dataset.
- **Inspect your output.** After generating, load the Parquet files with pandas
  and check that columns, types, and values look correct.
- **Copy the closest example.** If your data looks like EDRIXS, start from
  `gen_edrixs_manifest.py` and modify it.

---

## Step 5: Generate Manifests

Run your generator script from your application directory to produce the
Parquet files. Place the output in the `manifests/` directory.

Test with a small sample first, then inspect the output:

```bash
uv run --with pandas --with pyarrow \
  python -c "
import pandas as pd
ent = pd.read_parquet('manifests/spinwave_entities.parquet')
art = pd.read_parquet('manifests/spinwave_artifacts.parquet')
print(f'Entities:  {len(ent)} rows, columns: {list(ent.columns)}')
print(f'Artifacts: {len(art)} rows, columns: {list(art.columns)}')
print(ent.head())
print(art.head())
"
```

**Check before proceeding:**
- Entity manifest has `uid`, `key`, + your parameter columns
- Artifact manifest has `uid`, `type`, `file`, `dataset` (and `index` if batched)
- `type` values are unique within each `uid` group
- `file` paths are relative and point to real files

---

## Step 6: Write a Dataset Config

Your application directory should have this layout:

```
my-data-broker/
├── config.yml          # Tiled server config (readable_storage, port, etc.)
├── catalog.db          # SQLite catalog (created on first ingest)
├── datasets/           # Dataset YAML configs (one per dataset)
│   └── spinwave.yaml
├── generators/         # Manifest generator scripts
│   └── gen_spinwave_manifest.py
├── manifests/          # Generated Parquet files (output of generators)
│   ├── spinwave_entities.parquet
│   └── spinwave_artifacts.parquet
└── storage/            # Tiled writable storage (created automatically)
```

Create a YAML config file in the `datasets/` folder. This tells the broker
where the data lives and what metadata to attach to the dataset container.

```yaml
key: SpinWave
base_dir: /sdf/data/.../spinwave/results
metadata:
  organization: MAIQMag
  data_type: simulation
  material: FeGe
  producer: SpinWaveSim
  measurement: dispersion
```

### Field reference

| Field | Required | Description |
|-------|----------|-------------|
| `key` | **Yes** | Top-level container name in Tiled (e.g., `SpinWave`, `GenericSpin_Sunny_MH`). This is what users type in `client["SpinWave"]`. |
| `base_dir` | **Yes** | Absolute path to the data directory. Artifact `file` paths in the manifest are relative to this. Must also be listed in `config.yml` under `readable_storage`. |
| `metadata` | **Yes** | Dict of dataset-level metadata. Powers cross-dataset discovery queries like `client.search(Key("material") == "FeGe")`. |
| `label` | No | Human-readable display name. Defaults to `key`. |
| `generator` | No | Module name of the manifest generator (for documentation; not used by the ingest CLI). |

### The naming rule

**The YAML filename stem must match the Parquet filename prefix.** The
`broker-ingest` CLI resolves manifests by config filename:

```
datasets/spinwave.yaml
    ↓ stem = "spinwave"
manifests/spinwave_entities.parquet   ← CLI looks for this
manifests/spinwave_artifacts.parquet  ← and this
```

If these don't match, ingestion fails with "Parquet files not found." This is
the most common setup mistake.

### Dataset-level metadata for discovery

The `metadata` block is what enables cross-dataset search without knowing
container keys:

```python
# Find all simulation datasets
client.search(Key("data_type") == "simulation")

# Find datasets about a specific material
client.search(Key("material") == "FeGe")

# Combined
client.search(Key("material") == "FeGe").search(Key("data_type") == "simulation")
```

Choose metadata fields that help users discover your dataset alongside others.
Common fields: `organization`, `data_type` (simulation/experimental), `material`,
`producer`, `facility`, `instrument`, `measurement`.

#### Inherited fields (`amsc_public`)

A small set of metadata keys is **propagated** from the dataset YAML down to
every entity and artifact node during registration, so per-node consumers can
read the flag without walking the hierarchy. Currently the only such key is:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `amsc_public` | bool | `false` | Whether the dataset is public-readable. Propagated to all entity and artifact nodes. |

`tcb inspect` writes this as an active line in the draft YAML (defaulting to
`false`). Flip to `true` for datasets you want to mark as public. The
mechanism is implemented in `http_register.py` via the `INHERITED_KEYS`
tuple — add new keys there to extend propagation.

### Server configuration

Before ingesting, ensure your dataset's `base_dir` is listed in `config.yml`
under `readable_storage` so Tiled can serve the HDF5 files:

```yaml
trees:
  - path: /
    tree: catalog
    args:
      uri: "sqlite:///catalog.db"
      writable_storage: "storage"
      readable_storage:
        - /sdf/data/.../spinwave/results    # ← add your base_dir here
        - /sdf/data/.../other_dataset
```

---

## Step 7: Ingest into the Catalog

Two options depending on your situation.

### Option A: Bulk ingest via CLI (recommended)

This writes directly to the SQLite catalog using `broker-ingest`. No running
server needed. Fast (~1,800 nodes/sec).

Run from your application directory (where `manifests/` and `datasets/` live):

```bash
export BROKER=/path/to/tiled-catalog-broker/tiled_poc

uv run --with $BROKER broker-ingest datasets/spinwave.yaml
```

The CLI will:
1. Read `datasets/spinwave.yaml` for `key`, `base_dir`, and `metadata`
2. Load `manifests/spinwave_entities.parquet` and `manifests/spinwave_artifacts.parquet`
3. Create a dataset container named `SpinWave` in `catalog.db`
4. Bulk-insert all entities and artifacts

You can ingest multiple datasets at once:

```bash
uv run --with $BROKER broker-ingest \
    datasets/spinwave.yaml \
    datasets/another.yaml
```

### Option B: HTTP registration (incremental, for updates)

This registers through a running Tiled server. Slower (~5 nodes/sec) but
safe to run multiple times (skips existing entries).

```bash
# Terminal 1: start the server
uv run --with $BROKER tiled serve config config.yml --api-key secret

# Terminal 2: register
uv run --with $BROKER broker-register datasets/spinwave.yaml
```

---

## Step 8: Verify

After ingestion, start the server and spot-check that the data is accessible:

```bash
uv run --with $BROKER tiled serve config config.yml --api-key secret
```

```python
from tiled.client import from_uri

client = from_uri("http://localhost:8005", api_key="secret")

# Check the dataset container exists
ds = client["SpinWave"]
print(f"SpinWave: {len(ds)} entities")

# List some entities
for key in list(ds.keys())[:3]:
    h = ds[key]
    print(f"{key}: metadata keys = {list(h.metadata.keys())}")
    print(f"  children: {list(h)}")

# Check a specific artifact loads correctly
h = ds[list(ds.keys())[:1][0]]
arr = h[list(h)[0]].read()
print(f"  shape={arr.shape}, dtype={arr.dtype}")

# Verify dataset-level discovery works
from tiled.queries import Key
hits = client.search(Key("material") == "FeGe")
print(f"Discovery query: {list(hits.keys())}")
```

**Verify these things:**
- The dataset container exists with the expected number of entities
- Entities have all expected metadata keys (your physics parameters)
- Each entity has the expected child artifacts
- Arrays have the correct shapes
- Dataset-level discovery queries return your dataset
- A few random values match what you see when loading directly with h5py

---

## When the Data Doesn't Fit

Not every dataset is a good fit for the broker. If you identify issues during
assessment or exploration, provide clear feedback to the data provider about
what needs to change. This saves everyone time.

### Known constraints of the broker

| Constraint | Why it matters | What to ask the provider |
|-----------|----------------|--------------------------|
| **Data must be in HDF5** | The broker uses h5py to read arrays. Other formats (NPZ, TIFF, CSV, raw binary) are not supported. | "Could you save the output as HDF5 files? Here is a 5-line h5py example." |
| **Each entity needs a unique ID** | The `uid` column links entities to their artifacts. Without a consistent ID, there is no way to join the two manifests. | "Do your files have a run ID, parameter hash, or index we can use as a unique identifier?" |
| **Arrays should be rectangular** | Tiled serves arrays with fixed shapes. Ragged arrays (different lengths per entity) require extra handling. | "Are all your spectra the same length? If not, could they be zero-padded to a common size?" |
| **Artifact types must be unique per entity** | The `type` column becomes the Tiled child key. Two artifacts with the same type under one entity would collide. | "You have multiple magnetization curves per entity — could we name them `mag_x`, `mag_y`, `mag_z` instead of all `mag`?" |
| **There should be a parameter sweep structure** | The broker's value comes from querying across many entities by their physics parameters. A single simulation with no parameter variation does not benefit from the catalog. | "How many parameter sets did you simulate? Is there a systematic sweep?" |
| **Files must be accessible from the server** | The Tiled server needs read access to the HDF5 files at the paths listed in the manifest. | "Can we place the data under `/sdf/data/...` or mount it so the server can read it?" |

### How to give feedback

When pushing back, be specific about what needs to change and why. A good
message includes:

1. What you found during exploration
2. Which constraint is not met
3. A concrete suggestion for how to fix it

Example:

> Hi Alice,
>
> I explored the SpinWave data and found a few things we need to address
> before I can register it with the catalog:
>
> 1. The DOS arrays have different lengths (180–220 points depending on the
>    run). Our system needs rectangular arrays. Could you zero-pad them to
>    a common size (e.g., 220) during the simulation, or provide a
>    post-processing script that does this?
>
> 2. The files are in `.npz` format. We need HDF5. Converting is
>    straightforward — I can share a script if that helps.
>
> Once these are resolved, registration should take about an hour.

If the issues are fundamental (e.g., the data is not a parameter sweep at all,
or the format is too far from HDF5-based arrays), it may be worth discussing
whether the catalog is the right tool for this particular dataset.

---

## Glossary

| Term | Definition |
|------|-----------|
| **Entity** | One set of physics parameters and its associated simulation outputs. The fundamental unit in the catalog. |
| **Artifact** | One data array produced by an entity (e.g., a magnetization curve, a spectrum, a ground state). |
| **Locator** | The triple `(file, dataset, index)` that tells you exactly where to find one artifact in storage. |
| **uid** | Unique ID. A string that uniquely identifies one entity across all manifests. |
| **Dataset config** | A YAML file (`datasets/*.yaml`) that tells the broker the dataset's `key`, `base_dir`, and discovery `metadata`. The filename stem must match the Parquet manifest prefix. |
| **Manifest** | A Parquet file listing entities or artifacts with their metadata and locators. The interface between data provider and broker. |
| **Tiled** | The HTTP data catalog server. Stores metadata in SQLite/PostgreSQL and serves arrays over HTTP. |
| **Mode A** | Expert access: query Tiled for metadata/locators, then load data directly via h5py. Fast for bulk ML workloads. |
| **Mode B** | Visualizer access: read arrays through Tiled's HTTP API. Convenient for interactive exploration and remote access. |
| **Broker** | This software — the generic registration and query layer between raw HDF5 data and the Tiled catalog. |

---

## References

| Resource | Path |
|----------|------|
| Manifest contract (full spec) | [`docs/LOCATOR-AND-MANIFEST-CONTRACT.md`](LOCATOR-AND-MANIFEST-CONTRACT.md) |
| Broker design document | [`docs/DESIGN-GENERIC-BROKER.md`](DESIGN-GENERIC-BROKER.md) |
| Ingest CLI | `tiled_poc/broker/cli.py` (`ingest_main`) |
| Bulk registration engine | `tiled_poc/broker/bulk_register.py` |
| HTTP registration engine | `tiled_poc/broker/http_register.py` |
| Bulk ingest results | [`docs/BULK-INGEST-RESULTS.md`](BULK-INGEST-RESULTS.md) |
