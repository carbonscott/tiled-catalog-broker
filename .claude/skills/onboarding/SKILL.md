---
name: onboarding
description: >-
  Onboard a new HDF5 dataset into the Tiled catalog. Reads the contract surface
  (docs/ONBOARDING.md, the pydantic dataset models, the semantic vocabulary, and the
  example YAMLs), confirms the data contract, then guides authoring a dataset YAML and
  running tcb generate → stamp-key → register. Use when the user wants to onboard,
  register, or add a new dataset, or asks to "get ready to onboard a dataset".
---

# Onboarding a dataset

Goal: take a producer's HDF5 dataset and register it into the catalog by authoring **one
dataset YAML** against the contract surface, then running `tcb generate → stamp-key →
register`. You onboard by reading the *contract*, never broker *implementation*.

## Step 0 — Read the contract surface (do this first, do not skip)

Read these, in order. They define what a valid dataset is; do not reverse-engineer it from
`generate.py`/`http_register.py`:

1. `docs/ONBOARDING.md` — the walkthrough (layouts, the flow, field reference). It links the rest.
2. `src/tiled_catalog_broker/tools/_models.py` — the authoritative field contract (every YAML
   field, its type, whether it's required).
3. `src/tiled_catalog_broker/tools/schema/catalog_model.yml` — the controlled vocabulary
   (canonical ids + aliases for method/material/producer/project/facility/data_type).
4. `datasets/examples/{per_entity,batched,grouped}.yml` — one worked example per layout.

## Step 1 — Report ready

After reading, tell the user in 2–3 sentences that you understand the contract — the **three
frozen layouts** (`per_entity`, `batched`, `grouped`), the **required** `method`/`data_type`/
`material` metadata, and that the **vocabulary is soft** (unknown values warn, they don't
block) — and that you're ready to onboard their dataset.

## Step 2 — Gather the inputs (ask; don't guess)

Do **not** write any YAML until the user answers:

- **Data location**: the directory the HDF5 files live in, and a glob (`file_pattern`).
- **Layout**: which of the three matches their files (show the diagrams from ONBOARDING.md if
  unsure; pick by how entities are packed — one file each, axis-0 batched, or one group each).
- **Artifacts**: each array's name (`type`) and its HDF5 path (`dataset`).
- **Parameters**: where per-entity physics parameters live (`parameters.location`; plus
  `group`/`entity_group` as the layout requires).
- **Metadata**: `method`, `data_type`, `material` (required), and optional `producer`/
  `project`/`facility`. Prefer canonical ids from `catalog_model.yml`; flag unknown values as
  warnings, never invent vocabulary.
- **Shared axes** (optional): 1-D arrays shared by all entities (e.g. an energy axis).

## Step 3 — Author the YAML

Copy the `datasets/examples/<layout>.yml` that matches and adapt it. Set `label`; leave `key`
for `tcb stamp-key`. Keep the closed sections (`data`, `artifacts`, `parameters`, `shared`)
free of unknown keys — typos there are hard errors.

## Step 4 — Run the pipeline

```bash
tcb generate datasets/<name>.yml     # validates + writes entities/artifacts manifests
tcb stamp-key datasets/<name>.yml    # writes key = slug(label)
tcb register datasets/<name>.yml     # registers over HTTP (the single route)
```

Before `register`: the server's `config.yml` must list `data.directory` under
`readable_storage`; if the server sees the data at a different mount, set
`data.server_base_dir`.

## Step 5 — Verify

Connect with a Tiled client, check the dataset key exists, the entity count looks right, an
entity's metadata carries the physics parameters, and one artifact array loads
(`client[KEY].values().first()[<artifact>][:]`). See ONBOARDING.md §6.

## Guardrails

- Three layouts only — if the data fits none, the producer reshapes it; do not invent a layout.
- Vocabulary is soft: unknown values warn, they do not block. Don't hard-reject.
- Never write YAML before the user has given the Step 2 inputs.
