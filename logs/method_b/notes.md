# Method B — Onboarding Zhantao_B (write YAML from scratch)

## Step 0: Exploration  (start: 2026-05-01T17:57:53-07:00, end: 2026-05-01T18:02:00-07:00, ~4m)

Explored the dataset under restriction "no `tcb inspect`":

- One HDF5 file: `nips3_fwhm4_9dof_20000_20260303_0537.h5` (~19 GB)
  Sibling `nips-9d-irn/` is a code/training dir — no .h5 files in it, so safe.
- **Layout: grouped.** 20,000 root-level groups `sample_1 … sample_20000`.
- Per-sample structure:
  - `params/{Ax, Az, J1a, J1b, J2a, J2b, J3a, J3b, J4}` — 9 scalar float32 params
  - `data` (2000, 100) float32 — likely high-symmetry-path S(Q,E)
  - `powder_data` (300, 100) float32 — powder S(Q,E)
  - `energies` (100,) — **identical across samples** (verified s1==s2). Range [0, 100].
  - `powder_energies` (100,) — **identical across samples**.
  - `qs_lab` (3, 2000), `qs_rlu` (3, 2000), `powder_qs_lab` (300,) — **differ per sample**.
- Filename hints: `fwhm4` → broadening, `9dof` → 9 parameters, `20000` → entity count, `20260303` → generation date, `0537` → time stamp suffix.

Catalog model lookup (controlled vocabulary):
- methods include INS / SWT (both plausible)
- materials includes NiPS3
- producers includes sunny_jl, edrixs, lajer2025Hamiltonian
- Key convention: `{METHOD}_{DATA_TYPE_SHORT}_{DISTINGUISHING_FEATURE}` — but
  user mandated label `Zhantao_B`, so derived key will be `ZHANTAO_B`.

Server-mount info from `.env.test`:
- `TILED_HOST_DATA_ROOT=/sdf/data/lcls/ds/prj/prjmaiqmag01/results/`
- `TILED_SERVER_DATA_ROOT=/prjmaiqmag01/`
- `data.directory: /sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/Zhantao`
- → `data.server_base_dir: /prjmaiqmag01/data-source/Zhantao`

## Step 1: Wrote datasets/zhantao_method_b.yml  (start: 2026-05-01T18:02:00-07:00, end: 2026-05-01T18:08:00-07:00, ~6m)

Drafted by hand (no `tcb inspect`). Ran short h5py probe to verify:
- 20,000 root groups `sample_1 … sample_20000`
- 9 params per sample under `params/`
- `energies` / `powder_energies` byte-identical across samples → shared
- `qs_lab` / `qs_rlu` / `powder_qs_lab` differ per sample → per-entity

YAML uses `layout: grouped`, `parameters.location: group_scalars`, and the
default `entity_group: samples` (which is absent in the file, so the broker
falls back to "all root groups", which is correct here).

Six TODO decisions left for the user to fill in before stamp-key/generate:
1. method — `INS` vs `SWT` (or both)
2. producer — `sunny_jl` vs blank vs new vocab entry
3. amsc_public — true/false
4. artifact type names — `hisym/powder` vs `data/powder_data`
5. whether to register the per-entity Q arrays as artifacts (option b in YAML)
6. created_at date — filename says 20260303

PAUSING for user to edit `datasets/zhantao_method_b.yml`.

## Step 1.5: Read producer's repo, drafted v2  (start: 2026-05-01T18:18:00-07:00, end: 2026-05-01T18:25:00-07:00, ~7m)

User asked whether I'd referenced the code sitting next to Zhantao's data —
`/sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/Zhantao/nips-9d-irn/`.
I had not. Read it. It's a small repo (`nips-9d-irn`, "Siren-IRN with FiLM
modulation across Hamiltonians", wandb owner royliu2007) with the data-gen
script `data_generation/nips_9d.jl`.

Key things `nips_9d.jl` settled:
- Producer: **Sunny.jl** (`using Sunny`). → `producer: sunny_jl` confirmed.
- Method: the script calls `SpinWaveTheory(sys; measure, regularization=1e-6)`
  then `intensities(swt, path_rot; energies, kernel)` averaged over 3 in-plane
  rotations. The output is a *modeled* S(Q,E), not a measurement.
  → `method: [SWT]` is more accurate than `[INS]`. (Catalog vocab has both.)
- `kernel = gaussian(; fwhm=4)` matches the `fwhm4` filename token.
- Each sample's `qs_rlu` is **random points in the BZ** (`[rand(3) .- 0.5 for
  _ in 1:n_q_points]`). Per-sample, not a shared axis. → strengthens decision-5
  toward registering Q arrays as artifacts so callers know which Q's go with
  each entity's `hisym` array.

Mismatches between the script and our actual file (post-processing happened):
- Script writes `energies` at file root + only `qs_rlu`, `data`, `params/*`
  per sample. Our file additionally has `qs_lab`, `powder_data`,
  `powder_energies`, `powder_qs_lab` per sample, and `energies` lives inside
  each sample group, downsampled to (100,) over [0, 100] (script: (150,) over
  [0, 150]). Script also uses `n_q_points = 2500` (file: 2000). So a curated
  variant of the script (or `curate.py`) produced our file.
- File mtime is 2026-04-14; filename date is 2026-03-03. created_at refers
  to the original simulation, so 2026-03-03 stays.

User direction: write v2 with these updates, decide artifacts myself,
amsc_public=true, keep the date, otherwise leave provenance empty.
v2 written at `datasets/zhantao_method_b_v2.yml`. Decisions in v2:
- decision-1 method: `[SWT]`
- decision-2 producer: `sunny_jl` (unchanged)
- decision-3 amsc_public: `true`
- decision-4 artifact names: `hisym` / `powder` (kept semantic)
- decision-5 Q arrays: registered all three as artifacts (option b)
- decision-6 created_at: `"2026-03-03"`

PAUSING for user signoff on v2 before stamp-key/generate/register.

## Step 2: tcb stamp-key  (start: 2026-05-01T18:27:31-07:00, end: 2026-05-01T18:27:38-07:00, 7s)

Cmd: `uv run tcb stamp-key datasets/zhantao_method_b_v2.yml`
Result: stamped key 'ZHANTAO_B' (slug of label 'Zhantao_B').

Side note: stamp-key round-trips the YAML through ruamel and reformats list
indentation slightly under `artifacts:`/`shared:` (loses one indent level).
Parses identically; no functional impact.

## Step 3: tcb generate  (start: 2026-05-01T18:28:03-07:00, end: 2026-05-01T18:29:11-07:00, 68s)

Cmd: `uv run tcb generate datasets/zhantao_method_b_v2.yml`
Result:
- Found 1 HDF5 file
- Processed nips3_fwhm4_9dof_20000_20260303_0537.h5: 20000 entity groups
- Entities: 20000 rows → datasets/manifests/Zhantao_B/entities.parquet
- Artifacts: 100000 rows → datasets/manifests/Zhantao_B/artifacts.parquet
  (20k entities × 5 artifacts/entity: hisym, powder, qs_lab, qs_rlu, powder_qs_lab)

## Step 4a: tcb register (FAILED — broker bug)  (start: 2026-05-01T18:30:30-07:00, end: 2026-05-01T18:30:36-07:00, 6s)

Cmd: `uv run tcb register datasets/zhantao_method_b_v2.yml -n 100`
Result: created `ZHANTAO_B` container on the test server, then crashed:
```
File ".../http_register.py", line 256, in register_dataset_http
    print(f"\n--- Registering {label} ({n} entities via HTTP, "
NameError: name 'n' is not defined. Did you mean: 'np'?
```
`register_dataset_http` references `n` at lines 256 and 282 but never assigns
it. Per-task instructions, paused, reported to user.

## Step 4b: Fix + delete + re-register  (start: 2026-05-01T18:31:00-07:00, end: 2026-05-01T18:34:49-07:00, ~4m)

Per user direction:
1. Patched `src/tiled_catalog_broker/http_register.py`: added `n = len(ent_df)`
   right after `start_time = time.time()` in `register_dataset_http`.
2. `uv run tcb delete ZHANTAO_B --yes` → deleted the empty container (0 children).
3. `uv run tcb register datasets/zhantao_method_b_v2.yml -n 100` →

```
Created dataset container 'ZHANTAO_B'
--- Registering Zhantao_B (100 entities via HTTP, pool=8) ---
... progress 1.2/sec ...
Registration complete:
  Entities:        100
  Artifacts:       500
  Skipped:         0
  Artifact errors: 0
  Time:            81.5 seconds
```

Verification (sampled BROAD_SIGMA, not ZHANTAO_B — verify_registration_http
just inspects `client.keys()[0]` which sorts to BROAD_SIGMA; structure looked
intact). Total dataset containers on server now: 9 (was 8, ZHANTAO_B added).

## Summary

- **Total wall-clock (start of session → register success): ~37 minutes**
  (17:57:53 → 18:34:49, including pause for user sign-off on YAML).
- **Active assistant time: ~13 minutes** across exploration, two YAML drafts,
  bug-fix, and three CLI runs.
- **Sharp edges hit:**
  - The `nips-9d-irn` producer repo sitting next to the data was easy to miss
    on first pass. It's the source of truth for method/producer.
  - Producer's raw script and the actual file disagree on shapes & contents
    → file is post-processed. created_at refers to original sim, not file mtime.
  - Default `entity_group: samples` doesn't exist in this file; broker silently
    falls back to "all root groups" — which is what we want here, but worth
    knowing.
  - **Broker bug**: `register_dataset_http` missing `n = len(ent_df)` in
    src/tiled_catalog_broker/http_register.py. Patched locally; needs a real
    fix upstream.
  - `tcb delete` requires `--yes` in non-interactive shells.
- **Registration succeeded**: 100 entities, 500 artifacts, 0 errors.

## Step 5: Compare ZHANTAO_B vs ZHANTAO_A on the test server  (2026-05-01T18:38:00-07:00)

Queried the live server with the tiled python client and walked
dataset → first-entity → first-artifact for both ZHANTAO_A and ZHANTAO_B.

Same in both:
- 100 entities, same source HDF5, same 5 artifacts/entity, same shapes
  (`data` 2000×100, `powder_data` 300×100, `qs_lab`/`qs_rlu` 3×2000,
  `powder_qs_lab` 300), all 9 params, `material: NiPS3`,
  `data_type: simulation`, `producer: sunny_jl`, `project: MAIQMag`,
  `amsc_public: true`.

Differences:

| Field | ZHANTAO_A | ZHANTAO_B (v2 registered) | More accurate |
|---|---|---|---|
| `method` | `[INS]` | `[SWT]` | **B** — producer is Sunny SpinWaveTheory |
| Artifact names | `data`, `powder_data` (raw) | `hisym`, `powder` (semantic) | **even** |
| `created_at` | absent | `2026-03-03` | **B** |
| `description` | absent | `"... — Zhantao"` | **even** (cleanup, not a bug) |
| `shared_dataset_energies` | `/sample_1/energies` | `/energies` | **A** — dereferenceable path |
| `shared_dataset_powder_energies` | `/sample_1/powder_energies` | `/powder_energies` | **A** — same |

**One real bug in v2** (per user): `shared:` `dataset:` paths in `grouped`
layout are stored verbatim in the dataset metadata as `shared_dataset_<type>`.
v2 wrote `/energies`, which does NOT exist in the file — energies live at
`/sample_N/energies`. A Mode-A consumer reading the metadata path and calling
`f[that_path]` would KeyError. ZHANTAO_A correctly anchors to `/sample_1/...`.

Cleanup item (not a bug, per user): description with "— Zhantao" trailing
the prose is odd phrasing; the provider's name is already encoded in the
dataset key. Drop it for v3.

## Step 6: v3 written  (2026-05-01T18:42:00-07:00)

`datasets/zhantao_method_b_v3.yml` — same as v2 with two changes:
- Bug fix: `shared.energies.dataset = /sample_1/energies` (and
  `powder_energies` similarly).
- Cleanup: trailing "— Zhantao" removed from the description.

NOT yet stamp-keyed / generated / registered. Re-registering would require
`tcb delete ZHANTAO_B --yes` first, then stamp-key/generate/register.
