---
name: onboarding
description: >-
  Onboard a new HDF5 dataset into the Tiled catalog. Reads the contract surface
  (docs/ONBOARDING.md, the pydantic dataset models, the semantic vocabulary, and the
  example YAMLs), then explores the user's data + codebase to draft a first-pass dataset
  YAML with predicted entity/artifact counts, pauses for the user's review, runs tcb
  stamp-key → generate and compares the counts, pauses again for the go, then registers
  and proves the result by reading it back through the server. Use when the user wants to
  onboard, register, or add a new dataset, or asks to "get ready to onboard a dataset".
---

# Onboarding a dataset

Goal: take a producer's HDF5 dataset and register it into the catalog by authoring **one
dataset YAML** against the contract surface, then running `tcb stamp-key → generate →
register`. You onboard by reading the *contract*, never broker *implementation*.

## Step 0 — Read the contract surface (do this first, do not skip)

Read these, in order. They define what a valid dataset is; do not reverse-engineer it from
`generate.py`/`http_register.py`:

1. `docs/ONBOARDING.md` — the walkthrough: pick a transport, author, stamp/generate, make
   paths agree, register, read back. It links the layout, field, and error references.
2. `src/tiled_catalog_broker/tools/_models.py` — the authoritative field contract (every YAML
   field, its type, whether it's required).
3. `src/tiled_catalog_broker/tools/schema/catalog_model.yml` — the controlled vocabulary
   (canonical ids + aliases for method/material/producer/project/facility/data_type).
4. `datasets/examples/{per_entity,batched,grouped}.yml` — one worked example per layout;
   `per_entity_nexus.yml` shows a NeXus tree mapped onto the contract (several parameter
   groups → nested metadata).
5. `docs/ONBOARDING.md` step 4, the upload tab — **only if** the server cannot see the user's files (they
   will say so, or you learn it in Step 1). It is the `tcb register --upload` route: same
   contract, different transport.

## Step 1 — Report ready, ask for the starting context (and wait)

After reading, tell the user in 2–3 sentences that you understand the contract — the **three
frozen layouts** (`per_entity`, `batched`, `grouped`), the **required** `method`/`data_type`/
`material` metadata, and that the **vocabulary is soft** (unknown values warn, they don't
block). Then ask them for the starting context you need to explore:

- the **data path** (directory of HDF5 files);
- the **codebase / producer scripts** that generated it, if available (for inferring method,
  producer, material, parameter meanings);
- **where the server runs** relative to the data (same filesystem → pointer registration;
  server can't see the files → `tcb register --upload`); and
- any other context they want to give (paper, README, prior YAML, etc.).

**This is a conversation — stop and wait for the answer**, even if the data path was
already given in the invocation. What the user tells you here is the *only* source of
metadata besides the data itself (see Step 2). Don't go looking for more on your own.

## Step 2 — Explore the data and codebase

Explore **only two things**: (a) the data path, and (b) the codebase/context the user named
in Step 1. Do **not** hunt for context elsewhere in the repo or its history — not other
`datasets/*.yml`, not deleted files showing in `git status`, not `git show`/`git log`, not
`config.yml`'s storage roots, not example keys in `README.md`/`CLAUDE.md`, not old
`manifests/`. Those describe *other* datasets and deployments; copying values from them
fabricates provenance for this one (and, when the user is rehearsing onboarding, defeats the
exercise). If you notice such a file, ignore it and — if it seems relevant — *mention* it
to the user and let them decide whether to hand it to you as context.

The same goes for a YAML that already describes **this very dataset** (a prior draft in
`datasets/`, or manifests for it): don't silently adopt, overwrite, or delete it. Tell the
user it exists and ask whether to start from it or from scratch; any deletion of it is
theirs to approve.

Explore first so your questions are informed; then ask the user for what the data can't
say — in **one batched round**, not field-by-field across many turns. Build the YAML
**dynamically** from what you discover:

- **Inspect the HDF5** with `h5py`: open representative files; list groups/datasets, shapes,
  dtypes, and attributes. Decide the **layout** from how entities are packed (one file each →
  `per_entity`; stacked on axis-0 → `batched`; one group each → `grouped`), the **artifacts**
  (the array datasets and good `type` names), the **parameters** (`location` + `group`, or
  `groups` when the scalars are spread over several groups — each group nests under the
  name you give it; add `exclude` for blobs, `recursive` for subgroups), and any **shared
  axes** (arrays *identical* in every file → `shared:`; an axis that varies per entity is an
  artifact). Field attributes (`units`, ...) are captured automatically — nothing to declare.
- **If groups carry an `NX_class` attribute, the file is NeXus — map it, don't guess:**
  one `NXentry` per file → `per_entity` (several per file → `grouped`, `entity_group: /`);
  follow `@default` to the `NXdata` group — its `@signal` field, the `@axes` fields and any
  `FIELDNAME_errors` are the artifacts (axes go to `shared:` only if identical across the
  files you sampled; say which in a comment); `NXsample` / `NXinstrument` / `NXparameters` /
  `NXcollection` → one `parameters.groups` entry each, named after the group, with large
  string fields (JSON blobs) listed in `exclude`; root-level `definition` / `title` /
  `start_time` → a group entry pointing at the entry itself; `NXprocess` (`program`,
  `version`) → the `provenance:` block. Fields constant across every sampled file
  (`sample/chemical_formula`, `instrument/name`, `definition`) are candidates for dataset
  `metadata` (`material`, `facility`) — propose them as `# TODO confirm — "NiPS3" in all
  12 files`, mapped onto `catalog_model.yml` ids. See `datasets/examples/per_entity_nexus.yml`
  and `docs/explanation/nexus.md`.
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
`material`, `producer`, `project`, `facility`, `prior_distribution`, `provenance` are almost
never stated inside the HDF5 — expect them to be blank until the user fills them in Step 4.

**Predict the counts now, from what you inspected**, and keep them: entities
(`per_entity`: files matched by `file_pattern`; `batched`: Σ over files of the leading axis
of `artifacts[0]`; `grouped`: Σ entity groups) and artifacts (entities × number of
`artifacts` entries, plus one per `shared:` axis). Say how you got each number ("5 files ×
`/spectra` leading axis 2000 = 10,000 entities; 1 artifact each + 2 shared axes = 10,002").
These are what `tcb generate` will be checked against in Step 5.

## Step 4 — Hand it to the user for review (a dialogue, not a hand-off)

Show the drafted YAML **and your predicted counts**, and ask the user, in one message, the
concrete questions it leaves open: each `# TODO fill in` (what is the material / producer /
project ...?), each `# TODO confirm` (here's my inference and why — right?), the `label` (it
becomes the key), and the artifact/shared `type` names you chose. **Wait for their
answers**, apply them, show the result, and iterate until they say it's right. Only then
move on. Never fill a blank from anything other than the data or what the user told you.

**Run no `tcb` command before this sign-off — `stamp-key` and `generate` included.** They
are local and re-runnable, but they bake the label into a key and snapshot the YAML into
manifests, which presents decisions the user hasn't made yet (label, inferred metadata) as
settled. The review is about the YAML's *content*, not only about what reaches the server.

## Step 5 — Stamp, generate, compare — then stop

Once the user has signed off:

```bash
tcb stamp-key datasets/<name>.yml    # writes key = slug(label)
tcb generate datasets/<name>.yml     # validates + writes the manifests; prints Entities/Artifacts
```

Compare `generate`'s `Entities: N` / `Artifacts: M (S shared axes)` with the prediction
you gave in Step 4, and **tell the user the result — then stop and wait for their go
before registering.**

- **Match:** say so, with both numbers side by side, and that you are good to register
  (pending the two checks below, which you can do now and report in the same message).
- **Mismatch:** say so and **do not register.** A mismatch means you misread the data — the
  glob matched a stray file, the leading axis isn't what you thought, an entity group you
  didn't see. Go back to Step 2, find the discrepancy, fix the YAML, show the user the
  change, regenerate, compare again. A manifest you cannot account for does not get
  registered.

Before reporting "good to register", two checks:

- **Where does the server run relative to the data?** Same filesystem → omit
  `data.server_base_dir`; mounts it elsewhere → set it; cannot see it at all → `tcb
  register --upload` (`docs/ONBOARDING.md` step 4; trial with `-n 5`, verify, re-run). The
  rules — `readable_storage`, the physical-path caveat, what "the server's view" means —
  are **`docs/ONBOARDING.md` step 3, "Make local and server paths agree"**; follow that
  section rather than reasoning it out here, and **do not assume a host or facility**: paths in other YAMLs, `config.yml`
  or `CLAUDE.md` describe *other* deployments; the path the user gave you is where the
  data is.
- **Is the stamped key already on the target server?**
  `list(from_uri(TILED_URL, api_key=TILED_API_KEY))`. If so, tell the user —
  re-registering is incremental into that container, and a dataset is one transport or
  the other (`--upload` into a pointer-registered key, or vice versa, is refused). New
  label / delete / continue is their decision; fold it back into the Step 4 review rather
  than choosing.

## Step 6 — Register (on the user's go)

```bash
tcb register datasets/<name>.yml     # registers over HTTP (the single route)
```

(or `tcb register --upload ...` — trial with `-n 5` first, verify, re-run without `-n`).
Read the summary block, not the exit code: `Artifact errors` must be 0, and `Entities` +
`Skipped` should equal the entity count from Step 5. Any `WARNING ... half-registered`
lines mean a previous run died mid-entity and this run attached the rest — fine.

## Step 7 — Verify: read it back (the gate that actually proves it)

Registration never opens the files: a pointer the server cannot follow registers with
`Artifact errors: 0` and fails only at first read. So the proof is a **read-back through
the server**, not the register summary. With the same `TILED_URL` / `TILED_API_KEY` you
registered with:

```python
from tiled.client import from_uri
c = from_uri(TILED_URL, api_key=TILED_API_KEY)
ds = c["<KEY>"]
shared = {k.removeprefix("shared_dataset_") for k in ds.metadata if k.startswith("shared_dataset_")}
ents = [k for k in ds if k not in shared]
len(ents)                              # == generate's entity count (or -n, if you limited)
ent = ds[ents[0]]; dict(ent.metadata)  # the physics parameters, as queryable metadata
ent["<artifact>"][:].shape             # one artifact array, with the shape the manifest recorded
[ds[k][:].shape for k in shared]       # every shared axis, if any
```

Pass = the entity count matches, one artifact array **and each shared axis** come back
with the shapes the manifest recorded. Report those numbers. A bare `500` on the read is
the path/adapter/allowlist problem — `docs/reference/errors.md` decodes it, and every
other failure you are likely to meet, by symptom; fix the YAML, regenerate, **delete the
dataset** (`docs/ONBOARDING.md` step 6), re-register — an existing artifact is never
rewritten.

## Guardrails

- **Two sources only: the data, and what the user tells you.** Never mine the repo or git
  history (other dataset YAMLs, deleted files, `git show`, `config.yml` roots, README
  example keys, old manifests) for a dataset's metadata. If in doubt, ask the user.
- **It's a conversation, with two stops.** Ask for starting context and wait (Step 1);
  explore; ask the open questions in one batched round and wait (Step 4) — run **no `tcb`
  command** until the user has signed off on the YAML. After `generate`, report the
  count comparison and wait again (Step 5) — run **no `tcb register`** until they say go.
- **Predict, then compare.** Give the entity/artifact counts you expect with the draft
  (Step 3/4); after `generate`, compare and say whether you're good to register. A mismatch
  means you misread the data — don't register, find out why.
- **The read-back is the proof.** `register` reporting zero errors proves nothing about
  whether the server can read a byte; Step 7's read of an artifact and each shared axis
  does. Report what came back.
- **Check the server before registering.** If the stamped key already exists there, that's a
  decision for the user (new label / delete / continue), not for you.
- Build the YAML **dynamically** from exploring the data + code, then have the user review it
  — don't interrogate them field-by-field, and don't wait for a full spec before drafting.
- **Do not blind-guess.** Inferred-but-unstated values go in as a best guess with a `# TODO
  confirm` comment saying what you inferred it from; undeterminable fields stay `# TODO fill in`.
- Three layouts only — if the data fits none, the producer reshapes it; do not invent a layout.
- Vocabulary is soft: unknown values warn, they do not block. Don't hard-reject, don't invent ids.
