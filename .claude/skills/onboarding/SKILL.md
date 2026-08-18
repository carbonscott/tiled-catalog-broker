---
name: onboarding
description: >-
  Onboard a new HDF5 dataset into the Tiled catalog. Reads the contract surface
  (docs/ONBOARDING.md, the pydantic dataset models, the semantic vocabulary, and the
  example YAMLs), then explores the user's data + codebase to draft a first-pass dataset
  YAML for them to review, and runs tcb stamp-key → generate → register. Use when the user
  wants to onboard, register, or add a new dataset, or asks to "get ready to onboard a dataset".
---

# Onboarding a dataset

Goal: take a producer's HDF5 dataset and register it into the catalog by authoring **one
dataset YAML** against the contract surface, then running `tcb stamp-key → generate →
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

## Step 1 — Report ready, ask for the starting context

After reading, tell the user in 2–3 sentences that you understand the contract — the **three
frozen layouts** (`per_entity`, `batched`, `grouped`), the **required** `method`/`data_type`/
`material` metadata, and that the **vocabulary is soft** (unknown values warn, they don't
block). Then ask them for the starting context you need to explore:

- the **data path** (directory of HDF5 files);
- the **codebase / producer scripts** that generated it, if available (for inferring method,
  producer, material, parameter meanings); and
- any other context they want to give (paper, README, prior YAML, etc.).

## Step 2 — Explore the data and codebase

Do not interrogate the user field-by-field — go find the answers yourself, then confirm.
Build the YAML **dynamically** from what you discover:

- **Inspect the HDF5** with `h5py`: open representative files; list groups/datasets, shapes,
  dtypes, and attributes. Decide the **layout** from how entities are packed (one file each →
  `per_entity`; stacked on axis-0 → `batched`; one group each → `grouped`), the **artifacts**
  (the array datasets and good `type` names), the **parameters** (`location` + any
  `group`/`entity_group`), and any **shared axes** (1-D arrays common to all entities).
- **Read the codebase / context** to infer metadata (`method`, `data_type`, `material`,
  `producer`, `project`, `facility`). Map values onto canonical ids in `catalog_model.yml`;
  never invent vocabulary.

## Step 3 — Draft a first-pass YAML (don't blind-guess)

Copy the `datasets/examples/<layout>.yml` that matches and fill it from what you found. Set
`label`; leave `key` for `tcb stamp-key`. Keep the closed sections (`data`, `artifacts`,
`parameters`, `shared`) free of unknown keys — typos there are hard errors.

**Mark uncertainty as comments — do not silently guess.** If you can infer a value but it was
never stated explicitly (e.g. you suspect `method: [RIXS]` from the code but no source says
so), put your best guess in and flag it inline, e.g.:

```yaml
metadata:
  method: [RIXS]          # TODO confirm — inferred from edrixs calls in sim.py, not stated
  material: NiPS3         # TODO confirm — guessed from output dir name
```

Leave fields you genuinely can't determine as `# TODO fill in` rather than fabricating them.

## Step 4 — Hand it to the user for review

Show the drafted YAML and ask the user to review it: confirm the `# TODO`/inferred fields,
fix anything wrong, and fill anything missing. Iterate with them until it's right. Only then
move on.

## Step 5 — Run the pipeline

```bash
tcb stamp-key datasets/<name>.yml    # writes key = slug(label)
tcb generate datasets/<name>.yml     # validates (requires the key) + writes entities/artifacts manifests
tcb register datasets/<name>.yml     # registers over HTTP (the single route)
```

Before `register`: the server's `config.yml` must list `data.directory` under
`readable_storage`; if the server sees the data at a different mount, set
`data.server_base_dir`.

## Step 6 — Verify

Connect with a Tiled client, check the dataset key exists, the entity count looks right, an
entity's metadata carries the physics parameters, and one artifact array loads
(`client[KEY].values().first()[<artifact>][:]`). See ONBOARDING.md §6.

## Guardrails

- Build the YAML **dynamically** from exploring the data + code, then have the user review it
  — don't interrogate them field-by-field, and don't wait for a full spec before drafting.
- **Do not blind-guess.** Inferred-but-unstated values go in as a best guess with a `# TODO
  confirm` comment saying what you inferred it from; undeterminable fields stay `# TODO fill in`.
- Three layouts only — if the data fits none, the producer reshapes it; do not invent a layout.
- Vocabulary is soft: unknown values warn, they do not block. Don't hard-reject, don't invent ids.
