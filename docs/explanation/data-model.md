# The data model

A registered dataset has three levels of nested Tiled containers:

<figure class="tcb-diagram">
--8<-- "diagrams/hierarchy.svg"
<figcaption>Three levels. Provenance on the dataset, physics parameters on the entity, arrays on the artifact, and a shared axis stored once beside the entities.</figcaption>
</figure>

Datasets hold entities; entities hold artifacts. Every parameter on an entity is queryable
metadata, served by SQL, so searches stay fast as a dataset grows.

## The entity is the middle level

An entity is one simulation run, one sample, or one measurement: the parameters you search
on, plus one or more arrays. Two things look like they could do that job instead:

- **A file.** In one dataset a file is a run; in the next, a file holds ten thousand runs.
  See [the three layouts](layouts.md).
- **An array.** An entity often has several. A spectrum and a magnetization curve share one
  set of parameters, and you want them back together.

With the entity in the middle, the file layout can change without changing what a query
returns.

The shape follows [ArrayLake](https://docs.earthmover.io/concepts/data-model)'s
Organization → Repo → Group → Array, except ArrayLake's groups are mostly storage
namespaces, whereas here the middle level holds the science.

## Parameters are a free-form dict

Each entity's parameters are stored as a free-form metadata dict, not typed database
columns. Typed columns would perform about as well; the problem is that every dataset
shares one table. Two producers with unrelated physics:

| Producer | Its parameters |
|---|---|
| a spin-wave code | `Ja_meV`, `spin_s`, `g_factor` |
| an atomic-multiplet code | `Udd`, `Delta`, `crystal_10Dq` |

They overlap in nothing. A typed table would have to hold every column any producer ever
needed, each dataset filling in a few and leaving the rest null — and a new producer would
mean a schema migration, run by someone with write access, before your data could be
registered at all. A free-form dict holds `{Ja_meV, spin_s}` beside
`{Udd, Delta, crystal_10Dq}` with no shared schema and no nulls.

What you give up: nothing checks that two datasets using `sigma` mean the same quantity, or
stops a third from calling it `sig`. The [soft vocabulary](vocabulary.md) covers only the
few facets used for cross-dataset discovery, such as `material` and `method` — not the
physics parameters.

## What the broker does

Tiled stores the containers, answers the metadata queries, and slices the arrays. The
broker decides what shape to register, and writes it the same way every time:
`http_register.py` builds each entity's metadata from all the manifest's columns, so no
parameter name appears anywhere in the broker's code. That is what makes it
dataset-agnostic.

See also [the Parquet manifest](../reference/manifest.md), where those columns come from,
and [sliced reads](sliced-reads.md) for how an artifact is served once registered.
