# The three layouts

Your HDF5 files have to match one of three layouts. What you call your parameters and your
arrays, and how many entities you have, are all up to you.

<figure class="tcb-diagram">
--8<-- "diagrams/layouts.svg"
<figcaption>Where the entity boundary falls in each layout, and where each one keeps its parameters.</figcaption>
</figure>

## What each one looks like

**`per_entity`** — one HDF5 *file* per entity. Each file holds that entity's artifact
arrays; its parameters are scalars or attributes in the file.

```
data/
├── entity_0001.h5   →  /spectrum (600,)  + scalar params tenDq, F2_dd, …
├── entity_0002.h5   →  /spectrum (600,)  + scalars
└── …
```

**`batched`** — many entities stacked along **axis-0** of shared datasets within each file.
Entity *i* is row *i* of every artifact dataset. Parameters live in a parallel array, one
row per entity, usually in a `/params` group.

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
    └── …
```

## Why only three

Every extra layout is another way of reading files, with its own failure modes — and
another way for two datasets holding the same kind of data to look different in the
catalog. Queries are only worth writing if the catalog is consistent, and somebody has to
pay for that consistency. Here the producer pays once, by arranging the files to match a
layout; data that matches none of them does not onboard.

You keep everything else — parameter names, units, artifact names, entity count, what the
arrays mean. None of it is negotiated, and none of it is hardcoded in the broker.

Three is what the existing data needs: simulation sweeps written one run per file, batch
jobs that concatenate their output, and experimental collections organized by sample. A
fourth would need real data none of these can describe.

## Where the layout matters later

The layout changes what ends up in the catalog, not just how the files are read:

- **`batched`** — the manifest records each artifact's per-entity shape, leading axis
  dropped, and stores the row number in `index`. An entity is a slice, read by slicing in
  h5py rather than pulling the whole stacked dataset. See [sliced reads](sliced-reads.md).
- **`grouped`** — an artifact's `dataset` path resolves relative to each entity group.
- **`per_entity`** — the file is the entity boundary, so there is no index.

You declare it once and it is never re-inferred: the manifest, the locator metadata, and
the read path all rely on it matching the files.

See also [the `data` section](../reference/dataset-yaml.md#data) for the field, and
[How to publish a dataset](../ONBOARDING.md) for the steps.
