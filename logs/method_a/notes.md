# Method A — inspect-led onboarding of Zhantao_A

Worktree: /sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tcb-zhantao-method-a
Source dir: /sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/Zhantao/
Target YAML: datasets/zhantao_method_a.yml
Tiled URL: https://lcls-data-portal.slac.stanford.edu/tiled-test (env-loaded)
Limit: -n 100

## Step 1: tcb inspect     (start time: 2026-05-02T00:53:26Z, end time: 2026-05-02T00:58:49Z, duration: 323s)

Ran:
    uv run tcb inspect /sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/Zhantao/ \
        --output datasets/zhantao_method_a.yml

Files scanned: 1 HDF5 (`nips3_fwhm4_9dof_20000_20260303_0537.h5`, 19 GB).
The sibling `nips-9d-irn/` subdirectory was correctly skipped (no `.h5` files
inside — it's training/visualization code).

Auto-detected fields in the draft:
- `data.directory`: /sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/Zhantao
- `data.file_pattern`: nips3_fwhm4_9dof_20000_20260303_0537.h5
- `data.layout`: **grouped**  ← new layout I haven't seen used in any existing
  datasets/*.yml example (sam_klein, sunny_10k → batched; nips3_multimodal →
  per_entity)
- `data.server_base_dir`: /prjmaiqmag01/data-source/Zhantao  ← auto-filled
  from TILED_HOST_DATA_ROOT / TILED_SERVER_DATA_ROOT, as expected. ✓
- `metadata.amsc_public: false` (default, propagated to entities/artifacts)

The HDF5 is one file with 20,000 sample sub-groups (`/sample_1` … `/sample_20000`).
Per-sample structure (16 leaves each, identical shape across samples):
    data            (2000, 100) float32     ← primary observable (single-crystal S(Q,E))
    powder_data     (300, 100)  float32     ← powder observable
    energies        (100,)      float32     ← energy axis
    powder_energies (100,)      float32     ← powder energy axis
    qs_lab          (3, 2000)   float32     ← Q-vectors, lab frame
    qs_rlu          (3, 2000)   float32     ← Q-vectors, rlu
    powder_qs_lab   (300,)      float32     ← powder |Q|
    params/{Ax,Az,J1a,J1b,J2a,J2b,J3a,J3b,J4}  9 scalars

Total: 20000 samples × 16 leaves = 320,000 entries dumped into the
`unclassified:` block (file is 1.1M lines, mostly mechanical repetition).

### What's left to decide / sharp edges

1. **No `parameters:` block in the draft.** For `grouped` layout, inspect
   doesn't auto-emit a parameters block — it dumped each `/sample_N/params/<key>`
   as an unclassified leaf instead. The two existing parameter conventions are:
     - `location: root_scalars`  (per_entity layout, used by nips3_multimodal)
     - `location: group` + `group: /params`  (batched layout, used by sam_klein)
   The grouped-layout convention is not documented in the draft — needs a
   call from the user.
2. **Identity / metadata TODOs** (label + metadata block):
     - `label`  → human-readable; e.g., "Zhantao A" (key will derive to ZHANTAO_A)
     - `method` → INS? (powder + S(Q,E) at 100 meV scale fits)
     - `data_type` → simulation? (filename "fwhm4_9dof" suggests a resolution-
        broadened simulation grid; 9 DOF = 9 Hamiltonian params Ax, Az, J1a…J4)
     - `material` → NiPS3? (filename starts "nips3_…")
     - `producer` → optional (sunny_jl is plausible — same 9-param family as
        nips3_multimodal, but unconfirmed from the file alone)
     - `project` → optional (MAIQMag?)
     - `amsc_public` → leave false unless told otherwise
3. **Classification of the 16 per-sample leaves** into artifacts / shared /
   parameters:
     - Likely **artifacts** (per-entity, vary per sample): `data`, `powder_data`,
       `qs_lab`, `qs_rlu`, `powder_qs_lab`. (qs_lab/qs_rlu values do vary per
       sample in the inspect output — different orientations? — so they read
       as artifacts, not shared.)
     - Likely **shared** (constant across samples): `energies`, `powder_energies`
       (both `range [0.0, 100.0]` in every sample I sampled).
     - **Parameters** (9 scalars): need user to confirm where these go in
       grouped-layout YAML.
4. **Provenance block is empty** — no `created_at`, `code_version`, or `code_commit`
   discovered from HDF5 attributes. Optional but worth filling if known.

### Pausing for user

Per the workflow contract, I'm stopping here without editing the YAML.

After you authorized reading src/, I now know the grouped-layout contract
(`tools/schema.py`, `tools/generate.py::_generate_grouped`):

- `parameters.location: group_scalars` is the grouped-layout option (alongside
  the four documented in other YAMLs: root_scalars, root_attributes, group,
  manifest). With `parameters.group: params`, generate reads
  `<entity_group>/params/<name>` for each entity.
- `parameters.entity_group` defaults to `"samples"`. When that group doesn't
  exist (our case), generate falls back to using all root-level groups as
  entities — so `sample_1`…`sample_20000` will become entities with no
  extra config. Leave `entity_group` unset.
- `artifacts[].dataset` paths are **relative to the entity group**: write
  `/data`, `/powder_data`, etc. — generate prepends `/sample_N` per row.
- `shared` is read by `cli.py register` only as a string locator stored in
  the dataset container metadata (`shared_dataset_<type>`). It does NOT
  register the array. Since `energies` / `powder_energies` are stored
  redundantly under each sample, the shared locator must point at one
  representative copy, e.g. `/sample_1/energies`. (Sharp edge — one of the
  recommendations in the inspect draft would have been to deduplicate
  shared arrays at the producer side.)

Decisions still needed from you:
- Confirm identity / metadata: label="Zhantao A" → key ZHANTAO_A, method=[INS],
  data_type=simulation, material=NiPS3, producer=sunny_jl, project=MAIQMag,
  amsc_public=false.
- Confirm classification:
    artifacts: data, powder_data, qs_lab, qs_rlu, powder_qs_lab
    shared:    energies, powder_energies (use /sample_1/<name>)
    params:    Ax, Az, J1a, J1b, J2a, J2b, J3a, J3b, J4 → group_scalars at
               group "params"
  (qs_lab / qs_rlu values do vary slightly per sample in the inspect output,
   so I'm reading them as per-entity artifacts, not shared.)

### Why `tcb inspect` produced an unusable draft for grouped layout

After reading `tools/inspect.py`, the failure isn't a data quirk — it's two
gaps in the grouped-layout path:

**1. `walk_h5_tree` traverses the entire file** with `h5py.File.visititems`
(inspect.py:108-120), which recurses into every sample group. For our file
that means `result.datasets` ends up holding **20,000 × 16 = 320,000 leaves**
(`sample_1/data`, `sample_1/params/Ax`, …, `sample_20000/qs_rlu`). The
grouped-layout branch in `inspect_directory` (lines 752-766) does walk
`entity_groups[0]` separately to build an `inner_datasets` template, but
then merges those keys back into the same dict that already contains every
sample's leaves — the merge is a no-op for grouped because the prefixed
names (`sample_1/data`) collide with what `walk_h5_tree` already produced.
Net effect: the draft enumerates every entity's every leaf instead of one
representative.

**2. `classify_datasets` is a no-op for grouped** — lines 165-166:

    elif layout == "grouped":
        ds.category = "ARTIFACT_OR_AXIS"

That's the entire grouped branch. There is no `ndim == 0 → PARAMETER`
clause (unlike the per_entity branch at 143-147), no detection of
`<entity>/params/` subgroups (unlike batched at 132-141), no shared-axis
detection. Every leaf — including the 9 scalar Hamiltonian params — gets
tagged `ARTIFACT_OR_AXIS` and lands in the YAML's `unclassified:` block via
`emit_draft_yaml` (lines 593-606). No `parameters:`, `artifacts:`, or
`shared:` sections are emitted because no leaves are tagged with those
categories.

**What would need to change in inspect.py to fix this:**

  a. After `detect_grouped_layout` returns, **replace** `result.datasets`
     with the single-entity-group template (sample_1's leaves, stored as
     bare names like `data`, `params/Ax`, not prefixed with `sample_1/`),
     instead of merging into the file-wide walk. Or: skip the file-wide
     `walk_h5_tree` for grouped layout entirely.
  b. Extend `classify_datasets` for `layout == "grouped"` to mirror the
     batched/per_entity logic on the template:
       - `ndim == 0` → `PARAMETER`
       - parent group is `params` (or all-scalars under a single subgroup)
         → set the `parameters: { location: group_scalars, group: <name> }`
         hint at YAML emit time
       - `ndim > 1` → `ARTIFACT`
       - 1-D → `ARTIFACT_OR_AXIS` (still ambiguous, but only ~3 of these
         to classify by hand instead of 320k)
  c. Run a per-group consistency check (sample_1 vs sample_2, sample_500,
     sample_19999) analogous to `check_consistency` but inside the file,
     to flag missing leaves or mismatched shapes between groups. Also
     compare 1-D arrays across a few groups to auto-classify shared vs
     per-entity (the shared-axis check at 410-424 already does this for
     per_entity layout — it just needs to be invoked across groups
     instead of files for the grouped case).
  d. `emit_draft_yaml` would then emit the same parameters/artifacts/
     shared sections it already emits for the other layouts. The branch
     for grouped doesn't need new code in the emitter, just the upstream
     classification fix.

### How I worked out the classification without docs

  - Read `tools/schema.py` to learn `VALID_PARAM_LOCATIONS` includes
    `group_scalars`, which is the only one that fits a per-entity-group
    params subgroup. (The four locations used in existing YAMLs —
    `root_scalars`, `root_attributes`, `group`, `manifest` — don't.)
  - Read `tools/generate.py::_generate_grouped` (lines 455-553) to learn
    the call shape: `entity_group` defaults to "samples" with a fallback
    to root-level groups; `params_cfg.group` is the subgroup name; and
    `art["dataset"]` is treated as a path relative to each entity group
    (line 536: `full_ds_path = f"/{full_group}/{ds_path}"`).
  - Read `cli.py` (lines 77-79) to confirm `shared` is a string locator
    only — no array registration — so any path that resolves to a copy
    works; chose `/sample_1/<name>` as the canonical reference.
  - Cross-checked with `nips3_multimodal.yml` (per_entity) and
    `sam_klein.yml` (batched), both of which have the same 9-DOF NiPS3
    parameter set (Ax, Az, J1a–J4) and similar artifact shapes — strong
    evidence the producer is `sunny_jl` and method=INS, data_type=
    simulation, material=NiPS3.

### v2 YAML written

`datasets/zhantao_method_a_v2.yml` — kept the auto-generated header,
inline shape/range comments (one representative copy from sample_1), and
provenance/recommendations/consistency footer; replaced the 320k-line
`unclassified:` block with classified `parameters:`, `artifacts:`, and
`shared:` sections; pre-filled label and metadata. The original draft
`zhantao_method_a.yml` is untouched as a baseline for diffing.

User reviewed v2, approved, flipped `amsc_public: false → true`. Continuing.

## Step 3: tcb stamp-key     (start time: 2026-05-02T01:21:44Z, end time: 2026-05-02T01:21:50Z, duration: 6s)

    uv run tcb stamp-key datasets/zhantao_method_a_v2.yml

Output: `datasets/zhantao_method_a_v2.yml: stamped key 'ZHANTAO_A' (slug of label 'Zhantao A')`.
After: `key: ZHANTAO_A` written into the YAML alongside `label: "Zhantao A"`.

## Step 4: tcb generate     (start time: 2026-05-02T01:25:19Z, end time: 2026-05-02T01:26:31Z, duration: 72s)

    uv run tcb generate datasets/zhantao_method_a_v2.yml

Output:
    Found 1 HDF5 files
    Processed nips3_fwhm4_9dof_20000_20260303_0537.h5: 20000 entity groups (total: 20000)
    Entities: 20000 rows -> datasets/manifests/Zhantao A/entities.parquet
    Artifacts: 100000 rows -> datasets/manifests/Zhantao A/artifacts.parquet

20,000 entities × 5 artifacts each = 100,000 artifact rows. Manifest
directory uses the literal `label` string with a space ("Zhantao A") —
worth noting (not blocking, but unusual on a filesystem).

## Step 5: tcb register -n 100     (start time: 2026-05-02T01:27:10Z, end time: 2026-05-02T01:27:22Z, duration: 12s, FAILED)

    uv run tcb register datasets/zhantao_method_a_v2.yml -n 100

Server connection succeeded; dataset container `ZHANTAO_A` was created
on the remote test server **before** the crash. Then crashed with:

    NameError: name 'n' is not defined. Did you mean: 'np'?
    File ".../src/tiled_catalog_broker/http_register.py", line 256

Root cause: in `register_dataset_http`, the variable `n` is used on
lines 256, 282, and 287 but is never assigned anywhere in the function.
It's clearly meant to be `n = len(ent_df)` (the entity count after the
caller applies `-n`). Confirmed by:
  - line 256: `f"--- Registering {label} ({n} entities via HTTP, "`
  - line 282: `if (i + 1) % 5 == 0 or (i + 1) == n:`
  - line 287: `f"  Progress: {i+1}/{n} entities …"`
All three uses are progress/print formatting against the entity-count
denominator.

Crash happened **before any entities were registered** (the print at
line 256 is the first reference to `n`, and the ThreadPoolExecutor
block that does real work is downstream of that line). So:
  - On server: dataset container `ZHANTAO_A` exists, no children.
  - Locally: nothing to clean up.

This is a bug in http_register.py, not a problem with the YAML.

Proposed fix (one line, before line 256):

    n = len(ent_df)

Pausing per the workflow contract — no blind retries. Awaiting user
go-ahead to either (a) patch http_register.py and rerun (the empty
`ZHANTAO_A` container left on the server can be reused since the
function has an "existing container" path at lines 242-244, or
deleted via `tcb delete ZHANTAO_A` first for a clean run), or (b)
stop and report.

User confirmed the bug arose recently and asked for a fix + retry.

git blame confirms: bug was introduced **2026-05-01** (yesterday) in
commit `987aefa` "perf: parallelize HTTP entity registration with
ThreadPoolExecutor" — the parallelization refactor dropped the
original `n = len(ent_df)` assignment when restructuring the work
into a `ThreadPoolExecutor.submit` loop.

## Step 5a-fix: patch http_register.py

Edit: in `register_dataset_http`, just before the print at line 256,
add `n = len(ent_df)`. Confirmed via `git blame` — that line is now
flagged as "Not Committed Yet" while the surrounding lines are from
the parallelization commit on 2026-05-01.

## Step 5a-cleanup: tcb delete ZHANTAO_A     (start: 2026-05-02T01:30:35Z, end: 2026-05-02T01:30:38Z, duration: 3s)

First attempt without `--yes` errored: `Non-interactive shell. Use
--yes to proceed.` (Sharp edge: the delete confirmation prompt isn't
a TTY-only thing — it explicitly checks for non-interactive shells
and refuses without the flag, so any scripted/agent context needs
`--yes`.) Retry with `--yes` succeeded:

    deleted: ZHANTAO_A
    Done.

## Step 5b: tcb register -n 100 (retry)     (start: 2026-05-02T01:30:44Z, end: 2026-05-02T01:32:09Z, duration: 85s)

    uv run tcb register datasets/zhantao_method_a_v2.yml -n 100

Output (abridged):
    Created dataset container 'ZHANTAO_A'
    --- Registering Zhantao A (100 entities via HTTP, pool=8) ---
    Progress: 5/100 entities (0.3/sec) … 100/100 (1.3/sec)
    Registration complete:
      Entities:        100
      Artifacts:       500
      Skipped:         0
      Artifact errors: 0
      Time:            79.8 seconds

Verification probe walked root → ZHANTAO_A would have been one of the
8 datasets (verifier samples the alphabetically-first dataset
BROAD_SIGMA, not the just-registered one — that's a sharp edge for
post-register self-test, but not a failure).

## Final summary

- Wall clock end-to-end (step 1 inspect through step 5b register):
  2026-05-02T00:53:26Z → 2026-05-02T01:32:09Z ≈ **38m 43s**
- Active CLI step time (excluding human review pauses): ~9m 41s
  (323 + 6 + 72 + 12 + 3 + 85 = 581s)
- Registration succeeded: **100 entities, 500 artifacts (5 per entity), 0 errors**
- Sharp edges encountered:
  1. `tcb inspect` grouped-layout heuristic is a no-op classifier —
     produced a 1.1M-line draft with all 320,000 leaves under
     `unclassified:` and no `parameters:`/`artifacts:`/`shared:`
     sections. Fix described above (replace dataset dict with
     single-group template + extend `classify_datasets` for grouped).
  2. `http_register.py` regression introduced 2026-05-01: missing
     `n = len(ent_df)` after the ThreadPoolExecutor refactor. Fixed
     in this run; needs to ship in a real commit.
  3. `tcb delete` requires `--yes` in non-interactive shells (worth
     mentioning in any onboarding doc / agent contract).
  4. Manifest dir uses literal label "Zhantao A" with a space —
     functional but unusual on disk.
  5. `verify_registration_http` samples the alphabetically-first
     dataset, not the one just registered, so the smoke test
     doesn't actually exercise the new container.

---

## Followup (2026-05-05): Mode B verification + ZHANTAO_C comparison

### Mode B test — first attempt (failed for both A and B)

After the user asked me to compare ZHANTAO_A's registered output to
ZHANTAO_B (a parallel onboarding by another agent), I tested whether
sliced reads through the Tiled HTTP adapter (Mode B) actually returned
bytes. Both ZHANTAO_A and ZHANTAO_B failed identically:

    HTTPStatusError: 500 Internal Server Error for url
      .../api/v1/array/full/<DS>/<entity>/<art>?slice=...

The structure handshake (shape, dtype) succeeded; the byte fetch did
not. Initial hypothesis was that the test server's pod doesn't have
`/prjmaiqmag01/` mounted → `LazyHDF5ArrayAdapter` 500s on
`h5py.File()`. That hypothesis was **wrong**.

### Real cause: dtype mismatch in `create_data_source`

User patched `http_register.py`. Diff:

    - data_shape, _, _, _ = get_artifact_info(...)
    - data_dtype = np.float64
    + data_shape, dtype_str, _, _ = get_artifact_info(...)
    + data_dtype = np.dtype(dtype_str)

The hardcoded `np.float64` was registering an `ArrayStructure` that
lied about the on-disk dtype (real dtype is `float32`). The adapter's
slice path tripped on the byte-stride mismatch and 500'd. Same root
cause for both A and B — symmetric failure, unrelated to anything I
chose during onboarding.

### Step 5c: delete + re-register ZHANTAO_A     (2026-05-05T19:16:25Z → 19:19:08Z, ~163s including env reload)

Sequence (env vars had to be reloaded — they don't survive across
sessions, so re-sourced `.env.test` from the parent project root):

    set -a && source .../tiled-catalog-broker/.env.test && set +a
    uv run tcb delete ZHANTAO_A --yes      # 4s, 100 child nodes deleted
    uv run tcb register datasets/zhantao_method_a_v2.yml -n 100   # 79s

Result: 100 entities, 500 artifacts, 0 skipped, 0 errors, 73.2s
server-side. Same numbers as before the dtype fix — registration
itself wasn't broken, only the byte-fetch step.

### Mode B verification — ZHANTAO_A, all 5 artifacts on entity[0]

Method: connected as a client, walked
`c['ZHANTAO_A'][list(ds.keys())[0]]`, then for each artifact did
`np.asarray(arr[0:2,0:3])` (2-D) or `np.asarray(arr[0:3])` (1-D).
The slice triggers a Mode-B round-trip
(`/api/v1/array/full/.../slice=...`), forcing the
`LazyHDF5ArrayAdapter` to open the file and return real bytes —
the exact path the dtype bug had broken.

Sampled entity: `ZHANTAO_A_d7ff603010b27` (`source_group=sample_10002`).

| artifact         | shape       | dtype   | slice ok |
|------------------|-------------|---------|----------|
| data             | (2000, 100) | float32 | yes      |
| powder_data      | (300, 100)  | float32 | yes      |
| qs_lab           | (3, 2000)   | float32 | yes      |
| qs_rlu           | (3, 2000)   | float32 | yes      |
| powder_qs_lab    | (300,)      | float32 | yes      |

All five returned float32 bytes matching the on-disk dtype.

**Coverage caveat (worth being honest about):** I tested 1 entity
out of 100, sliced reads only (corner reads `[0:2,0:3]` and `[0:3]`),
and didn't compare values to a direct h5py read. The test confirms
that the failing path now succeeds for one representative entity,
which is enough to call the dtype fix verified — but not enough to
claim all 100 entities are healthy.

### ZHANTAO_C comparison

User authorized reading the C worktree, its YAML, its parquets, and
testing Mode B on it. Source paths:
  - `/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tcb-zhantao-method-c/datasets/zhantao_method_c_v2.yml`
  - `/sdf/.../tcb-zhantao-method-c/scripts/gen_zhantao_method_c.py`
  - `/sdf/.../tcb-zhantao-method-c/datasets/manifests/Zhantao_C/{entities,artifacts}.parquet`

Method C **bypasses the YAML-driven `tcb generate` pipeline
entirely** — they hand-rolled `gen_zhantao_method_c.py` which writes
parquet manifests directly from the HDF5. As a result the YAML is
43 lines (vs my 100) and contains only metadata; no
`parameters`/`artifacts`/`shared`/`layout`/`file_pattern`.

#### 1. Dataset-level metadata (server-side)

| Field                        | ZHANTAO_A (mine)         | ZHANTAO_C                 |
|------------------------------|--------------------------|---------------------------|
| `method`                     | `['INS']` (canonical)    | — (not set)               |
| `measurement`                | —                        | `'ins'` (custom field)    |
| `producer`                   | `'sunny_jl'` (canonical) | `'Sunny.jl'` (free-form)  |
| `project`                    | `'MAIQMag'`              | —                         |
| `organization`               | —                        | `'MAIQMag'` (custom field)|
| `data_type`/`material`/`amsc_public` | match            | match                     |
| `shared_dataset_*`           | 2 entries (sample_1 paths)| none                     |

C invented metadata field names that aren't in the catalog vocab
(`measurement`, `organization`) and used a free-form producer string.
`schema.py::_validate_vocab` only checks `method`, `data_type`,
`material`, `producer`, `facility`, `project` — so cross-dataset
queries through the catalog model (e.g.,
`client.search(Key('method') == 'INS')`) would silently miss
ZHANTAO_C. A's metadata aligns with `nips3_multimodal.yml` and
`sam_klein.yml`.

#### 2. Entities

| | ZHANTAO_A | ZHANTAO_C |
|---|---|---|
| count                        | 100              | 100               |
| key form                     | `ZHANTAO_A_<sha[:13]>` | `ZHANTAO_C_sample_<N>` |
| UID form                     | content-addressed sha256 of params | positional `sample_<N>` string |
| metadata field count         | 22               | 26                |
| 9 Hamiltonian params present | yes              | yes               |

Reproducibility implication: A's UID is
`_make_uid(params, namespace="ZHANTAO_A")` so the same params produce
the same UID across regenerations regardless of file order. C's UID
is the source group name — if Zhantao re-emits with samples in a
different order, the same physical params get a different UID. The
content-addressed form is what `generate.py::_make_uid`'s docstring
calls out as "preferred ... preserves identity across regeneration".

#### 3. Artifacts

| | ZHANTAO_A | ZHANTAO_C |
|---|---|---|
| per-entity artifact count   | 5                                          | 7 |
| artifact set                | data, powder_data, qs_lab, qs_rlu, powder_qs_lab | + energies, powder_energies |

C registered `energies` and `powder_energies` as **per-entity array
children** (one copy per sample, 100 redundant copies in the 100-entity
slice). I registered them as `shared:` locator strings on the
dataset container only — no array children, exposed via Mode A only.

Tradeoff:
- **A**: 1 `shared_dataset_energies` entry on the dataset container.
  ~100× less catalog state; energy axis only reachable via Mode A.
- **C**: 100 `energies` array children. Catalog state cost; energy
  axis reachable via Mode B from any entity. Wasteful at 100, more
  wasteful at 20000.

Neither is wrong — they reflect different opinions about what
"shared" means in the catalog.

#### 4. Parquet manifests

| | A entities.parquet                  | C entities.parquet               |
|---|-------------------------------------|----------------------------------|
| rows | 20000                            | 20000                            |
| cols | uid, source_group, 9 params      | uid, **key**, 9 params           |
| uid form | content-addressed sha[:16]   | positional `sample_<N>`          |

| | A artifacts.parquet                                              | C artifacts.parquet              |
|---|------------------------------------------------------------------|----------------------------------|
| rows | 100000 (20000×5)                                              | 140000 (20000×7)                 |
| cols | uid, type, file, dataset, index, **file_size, file_mtime**    | uid, type, file, dataset, index  |

C's parquets are missing `file_size` and `file_mtime`.
`tcb generate` populates those via `_file_fingerprint`
(generate.py:66-72); C's hand-rolled script skipped them. Not
required by `register_dataset_http`, so registration succeeds — but
consumers lose freshness/size info on the catalog. C also writes an
entity-level `key` column that lands as per-entity metadata
(`key='H_sample_2'`) — harmless but non-standard.

#### 5. Mode B reads on ZHANTAO_C

Tested entity[0] (`ZHANTAO_C_sample_2` → uid `sample_2`), all 7 artifacts:

| artifact         | shape       | dtype   | slice ok |
|------------------|-------------|---------|----------|
| data             | (2000, 100) | float32 | yes      |
| energies         | (100,)      | float32 | yes      |
| qs_rlu           | (3, 2000)   | float32 | yes      |
| qs_lab           | (3, 2000)   | float32 | yes      |
| powder_data      | (300, 100)  | float32 | yes      |
| powder_energies  | (100,)      | float32 | yes      |
| powder_qs_lab    | (300,)      | float32 | yes      |

All 7 read cleanly. Same code path as A (LazyHDF5ArrayAdapter,
post-dtype-fix), so the dtype fix benefits both.

### Net read on A vs C

| Axis | Verdict |
|------|---------|
| Vocab alignment | **A wins.** Uses canonical IDs; C invented `measurement`/`organization` and used free-form `Sunny.jl` — silent miss for catalog-model queries. |
| Entity UID semantics | **A wins.** Content-addressed; reproducible across regenerations. C's positional UIDs are file-order-dependent. |
| Mode B coverage | **C wins.** Exposes `energies`/`powder_energies` as readable arrays. A only exposes them as Mode-A locator strings. |
| Catalog efficiency | **A wins.** 1 shared locator vs 100 redundant array children for the energy axes. Scales worse for C at 20k entities. |
| Provenance fields in parquet | **A wins.** `file_size`/`file_mtime` present; C dropped them. |
| Pipeline alignment | **A wins.** Goes through the documented `tcb inspect → stamp-key → generate → register` path. C bypasses `generate` with a custom script — faster to author once, but loses schema validation, file fingerprinting, and the layout-aware logic. |
| Both register and serve | **Tie.** Both produced 100 entities × correct artifact arrays. Mode B works on both after the dtype fix. |

The most consequential differences for downstream consumers are (a)
C's vocab deviations breaking catalog-model queries and (b) C's
positional UIDs breaking content-addressed dedup on regeneration.
C's only meaningful advantage is broader Mode B coverage of the
energy axes — but that comes at a 100× / 20k× catalog-state cost
versus the `shared:` convention.

### ZHANTAO_D comparison

Source paths:
  - `/sdf/.../tcb-zhantao-method-d/datasets/zhantao_method_d_v5.yml`
    (D iterated through v1→v5; latest is what they registered)
  - `/sdf/.../tcb-zhantao-method-d/datasets/manifests/Zhantao_D/{entities,artifacts}.parquet`
  - No `scripts/` directory — D went through `tcb generate`, not a
    hand-rolled script (unlike C).

D **did not run `tcb inspect`** — they hand-authored the YAML and
iterated it (v1→v5). The v5 YAML's leading comment refers to "the
introspection cheat ... read tools/generate.py via inspect.getsource",
which is the Python-stdlib `inspect.getsource()` (reading the source
of `_generate_grouped`), not `tcb inspect`. So D's pipeline was
**stamp-key → generate → register only** (3 steps, not 4). My
inspect-led path was the only one of A/C/D that ran `tcb inspect`.

#### 1. Dataset-level metadata (server-side)

| Field                        | ZHANTAO_A (mine)         | ZHANTAO_D                        |
|------------------------------|--------------------------|----------------------------------|
| `method`                     | `['INS']` (canonical)    | —                                |
| `measurement`                | —                        | `'spin-wave'` (custom field)     |
| `producer`                   | `'sunny_jl'` (canonical) | `'Sunny.jl'` (free-form)         |
| `project`                    | `'MAIQMag'` (canonical)  | —                                |
| `organization`               | —                        | `'MAIQMag'` (custom field)       |
| `amsc_public`                | `True`                   | — (absent — *not* set)           |
| `description`                | —                        | full sentence (9-DoF LHC sweep…) |
| `prior_distribution`         | —                        | `'latin-hypercube-9d'`           |
| `created_at`                 | —                        | `'TODO'` (literal string)        |
| `producer_version`           | —                        | `'TODO'`                         |
| `producer_commit`            | —                        | `'TODO'`                         |
| `data_type` / `material`     | match                    | match                            |
| `shared_dataset_energies` / `_powder_energies` | `'/sample_1/energies'`, `'/sample_1/powder_energies'` | **same** — `/sample_1/...` |

D's vocab choices echo C's (`measurement`, `organization`,
`Sunny.jl`) so D shares C's catalog-query-miss problem. **D uniquely
omits `amsc_public`** entirely — that flag is supposed to propagate
to every entity/artifact via `INHERITED_KEYS`, so D's entities and
artifacts also lack the public-readable flag. D added richer
provenance fields than A (`description`, `prior_distribution`) but
left three as the literal string `'TODO'`, which means they're
queryable as `'TODO'` strings rather than null.

D **matches A** on the `shared:` convention: same `/sample_1/...`
locator paths for `energies` / `powder_energies`. (C had no shared
block at all.)

#### 2. Entities

| | ZHANTAO_A | ZHANTAO_D |
|---|---|---|
| count                        | 100                       | 100                      |
| key form                     | `ZHANTAO_A_<sha[:13]>`    | `ZHANTAO_D_<sha[:13]>`   |
| UID form                     | content-addressed sha256  | content-addressed sha256 |
| metadata field count         | 22                        | 21                       |
| 9 Hamiltonian params present | yes                       | yes                      |
| `amsc_public` per-entity     | yes                       | **no** (not inherited)   |

D used `tcb generate`, so its UIDs are content-addressed via
`_make_uid(params, namespace="ZHANTAO_D")` — same reproducibility
property as A. Same content + different namespace produces different
hashes, so D's `5ee2974680d73592` and A's UID for the same `sample_1`
params won't match across datasets — by design.

#### 3. Artifacts

| | ZHANTAO_A | ZHANTAO_D |
|---|---|---|
| per-entity artifact count | 5                                          | 5 |
| artifact set              | data, powder_data, qs_lab, qs_rlu, powder_qs_lab | sqw, powder, qs_lab, qs_rlu, powder_qs_lab |

Same five physical arrays, two type names changed:
- `data` → **`sqw`** (`S(Q,ω)`) — physics-canonical
- `powder_data` → **`powder`**

D's renames are arguably better than mine: `sqw` is the standard
spectroscopy name for the (Q,ω) intensity grid, `powder` matches
the convention in `nips3_multimodal.yml`. I kept the leaf names
verbatim. (B used `hisym`/`powder`; D used `sqw`/`powder` —
different but both more semantic than my unmodified leaves.)

D matches A on the shared-axis split (energies / powder_energies in
the `shared:` block, not registered as array children). C was the
outlier here — C registered them as per-entity array children.

#### 4. Parquet manifests

| | A entities.parquet              | D entities.parquet            |
|---|---------------------------------|-------------------------------|
| rows | 20000                        | 20000                         |
| cols | uid, source_group, 9 params  | uid, source_group, 9 params   |
| uid form | content-addressed sha[:16] | content-addressed sha[:16]  |

| | A artifacts.parquet                                          | D artifacts.parquet                                          |
|---|--------------------------------------------------------------|--------------------------------------------------------------|
| rows | 100000 (20000×5)                                           | 100000 (20000×5)                                            |
| cols | uid, type, file, dataset, index, file_size, file_mtime    | uid, type, file, dataset, index, file_size, file_mtime    |

**D's parquets are byte-shape-identical to A's** (same column set,
same row counts, content-addressed UIDs, file_size + file_mtime
preserved) — because D used `tcb generate` (just not `tcb inspect`
beforehand). Only the type strings (`sqw`/`powder` instead of
`data`/`powder_data`) differ. C's parquets, by contrast, were
hand-rolled and missing several of these fields.

#### 5. Mode B reads on ZHANTAO_D

Tested entity[0] (`ZHANTAO_D_4d69f5321ff02` → `source_group=sample_10001`),
all 5 artifacts:

| artifact         | shape       | dtype   | slice ok |
|------------------|-------------|---------|----------|
| sqw              | (2000, 100) | float32 | yes      |
| powder           | (300, 100)  | float32 | yes      |
| qs_rlu           | (3, 2000)   | float32 | yes      |
| qs_lab           | (3, 2000)   | float32 | yes      |
| powder_qs_lab    | (300,)      | float32 | yes      |

All five read cleanly — same code path as A and C, post-dtype-fix.

### Net read on A vs D

| Axis | Verdict |
|------|---------|
| Vocab alignment      | **A wins.** D uses the same custom-field deviations as C (`measurement`, `organization`, `Sunny.jl`); same catalog-query-miss problem. |
| `amsc_public` flag   | **A wins.** D omitted the flag entirely, breaking the `INHERITED_KEYS` propagation to every entity/artifact. Likely an oversight given they otherwise iterated 5 YAML versions. |
| Provenance richness  | **D wins.** D has `description`, `prior_distribution`, and stub `producer_version`/`commit`/`created_at` fields — better discovery context than my empty provenance block. (User asked me to leave provenance empty; D didn't have that constraint.) |
| Artifact type names  | **D wins.** `sqw`/`powder` are physics-semantic; my `data`/`powder_data` are unmodified leaves. The inspect comment said "TODO: rename if desired" — D took it, I didn't. |
| Entity UID semantics | **Tie.** Both content-addressed (D went through `tcb generate`). |
| Shared-axis convention | **Tie.** Both use the `shared:` block with `/sample_1/<name>` locators — same Mode-A-only access. |
| Parquet schema       | **Tie.** Same columns, same file_size/file_mtime preservation. |
| Pipeline alignment   | **A: 4 steps** (`inspect → stamp-key → generate → register`). **D: 3 steps** (`stamp-key → generate → register`, hand-authored YAML). Same `generate`/`register` engine — D just skipped inspect and authored the contract from scratch (using `inspect.getsource()` of `_generate_grouped` to figure out the schema). |
| Mode B read          | **Tie.** All 5 artifacts read cleanly on both. |
| YAML iteration       | **D wins on process.** v1→v5 (5 hand-edits via the new-version-each-time pattern); my v1→v2 was less iterative. |

D is **structurally the closest method to mine** at the
generate/register stage — same content-addressed UIDs, same
shared-axis convention, same parquet schema — even though D skipped
the `tcb inspect` step entirely. The remaining deltas are
(1) D's better artifact type names and richer provenance,
(2) D's worse vocab choices (custom fields breaking catalog queries),
(3) D's missing `amsc_public`.

Most-merged-best YAML across A/B/C/D would take:
  - A's canonical vocab (`method: [INS]` *or* `[SWT]`, `producer: sunny_jl`, `project: MAIQMag`)
  - A's `amsc_public: true`
  - D's `description` + `prior_distribution` + provenance scaffolding
  - D/B's artifact type renames (`sqw`/`powder`)
  - A/D's `shared:` convention with `/sample_1/...` locators
  - A/D's `tcb generate` pipeline (content-addressed UIDs + parquet
    fingerprints), not C's hand-rolled script
  - B's parsed `created_at: '2026-03-03'` (from the filename), not D's `'TODO'`

