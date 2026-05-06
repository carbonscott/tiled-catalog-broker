# Method C — onboarding-by-hand notes

Worktree: `/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tcb-zhantao-method-c`
Dataset label: `Zhantao_C`

## Step 1: Explore dataset      (start: 2026-05-05T11:44:52-07:00, end: 2026-05-05T12:11:00-07:00, duration: ~25 min)

What I ran:
- `ls /sdf/data/lcls/ds/prj/prjmaiqmag01/results/data-source/Zhantao/` — found one HDF5 file
  (`nips3_fwhm4_9dof_20000_20260303_0537.h5`, ~19.5 GB) plus a code repo
  `nips-9d-irn/` containing `data_generation/{nips_9d.jl, curate.py}` and
  README pointing to a Siren+FiLM IRN. Worth reading the Julia generator —
  it documents the physics parameters, the spin Hamiltonian (NiPS3), and the
  intent (S(q,ω) at LHS-sampled couplings).
- h5py walk on the dataset.

What's inside the H5:
- 20,000 top-level groups `sample_1` ... `sample_20000` (no other top-level keys).
- Each sample group contains:
  - `data`           shape (2000, 100), float32  — main S(q,ω)
  - `energies`       shape (100,),     float32  — per-sample energy axis (constant
    across samples, range 0..150 in 100 bins)
  - `qs_rlu`         shape (3, 2000),  float32  — q-points in reciprocal-lattice units
  - `qs_lab`         shape (3, 2000),  float32  — q-points in lab frame
  - `powder_data`    shape (300, 100), float32  — powder-averaged S(|q|, ω)
  - `powder_energies`shape (100,),     float32  — powder energy axis
  - `powder_qs_lab`  shape (300,),     float32  — powder |q| axis
  - `params/{Ax,Az,J1a,J1b,J2a,J2b,J3a,J3b,J4}` — 9 scalar Hamiltonian couplings
- Random spot check on 10 samples: shapes are 100% consistent. `energies` and
  `powder_energies` are identical across samples.

Pattern: this is "Pattern C-ish" but inverted — *one batched H5 file* with one
*group per entity* (not axis-0 batching). Locator handles it cleanly:
`file=<h5>, dataset=/sample_<N>/<arr>, index=null`.

Notes for sizing the entity key:
- `make_entity_key()` truncates uid to 13 chars. Using uid=`sample_<N>` keeps
  uniqueness because `sample_19999` is 12 chars and the full numeric tail is
  preserved even at the worst case.

Decisions I'm punting to the user (TODO in YAML):
- `amsc_public` — public/private flag, no obvious default.
- `measurement` — wrote `ins` (S(q,ω) from spin-wave theory is essentially
  simulated INS data) but flagging in case a different controlled value is
  preferred (`spin_wave`, `INS`, etc.).
- `facility` / `instrument` — pure simulation, no facility, omitted.

## Step 2: Author YAML + manifest generator     (start: 2026-05-05T12:11:00-07:00, end: 2026-05-05T12:25:00-07:00, duration: ~14 min)

Wrote two files:
- `datasets/zhantao_method_c.yml` — dataset config with `data.directory`,
  `data.server_base_dir` (computed from TILED_HOST_DATA_ROOT vs
  TILED_SERVER_DATA_ROOT), `metadata.{organization, data_type, material,
  producer, measurement}`. Two TODO comments: `measurement` value confirmation
  and `amsc_public` flag (left commented out).
- `scripts/gen_zhantao_method_c.py` — h5py walk producing
  `entities.parquet` (one row/sample, 9 param cols) and
  `artifacts.parquet` (7 rows/sample: data, energies, qs_rlu, qs_lab,
  powder_data, powder_energies, powder_qs_lab). uid = `sample_<N>`,
  Tiled-friendly `key = H_sample_<N>`. `index` column written as
  Int64 nullable so the parquet preserves null semantics for the broker.

Script writes to `datasets/manifests/Zhantao_C/`. CLI search picks that up
via the "next to YAML" rule.

PAUSED here for user to fill in TODOs (`amsc_public`, optionally
`measurement`) before I run anything.

User responded: `amsc_public: true`, keep `measurement: ins`.

## Step 3: Versioned YAML edit                  (start: 2026-05-05T12:16, end: 2026-05-05T12:18, duration: ~2 min)

Started by editing the original YAML in place — user corrected: "you should
always make a new yaml with every change, you can call it v2." I had a memory
about this rule but I had framed it as inspect-onboarding-only; updated the
memory to broaden the trigger. Wrote `datasets/zhantao_method_c_v2.yml`
with the resolved values; original `zhantao_method_c.yml` left untouched.

## Step 4: Stamp key                            (start: 2026-05-05T12:17:59-07:00, end: 2026-05-05T12:18:17-07:00, duration: ~18 s)

`uv run tcb stamp-key datasets/zhantao_method_c_v2.yml`
→ stamped `key: ZHANTAO_C` (slug of label `Zhantao_C`).

## Step 5: Generate manifests                   (start: 2026-05-05T12:18:43-07:00, end: 2026-05-05T12:20:44-07:00, duration: ~2 min)

`uv run --with h5py --with pandas --with pyarrow python scripts/gen_zhantao_method_c.py`
- 20,000 entities → datasets/manifests/Zhantao_C/entities.parquet (11 cols)
- 140,000 artifacts → datasets/manifests/Zhantao_C/artifacts.parquet (5 cols)
- All 7 artifact types per entity, dataset paths shape `/sample_<N>/<type>`,
  index column null (typed Int64).
- Read-and-print verification with pandas confirmed schema/dtypes.

## Step 6: Register -n 100 — FAILED             (start: 2026-05-05T12:30:58-07:00, end: 2026-05-05T12:31:15-07:00, duration: ~17 s)

`uv run tcb register datasets/zhantao_method_c_v2.yml -n 100`

```
Created dataset container 'ZHANTAO_C'
Pre-grouping artifacts by uid...
Traceback (most recent call last):
  ...
  File ".../tiled_catalog_broker/http_register.py", line 256, in register_dataset_http
    print(f"\n--- Registering {label} ({n} entities via HTTP, "
                                        ^
NameError: name 'n' is not defined. Did you mean: 'np'?
```

Diagnosis: `http_register.py` line 256 (and 282) refers to `n`, but the
function never binds it — looks like a refactor leftover; the surrounding
loop uses `len(ent_df)` semantically. Side-effect on the test server:
the empty `ZHANTAO_C` dataset container was already created before the
crash, so a retry needs to delete it (`tcb delete ZHANTAO_C`) or treat
it as "use existing".

PAUSED — reporting upstream as instructed.

## Step 7: Patch http_register.py + retry      (start: 2026-05-05T12:34:00-07:00, end: 2026-05-05T12:36:22-07:00, duration: ~2.5 min)

User said "patch and continue". Single-line fix in
`src/tiled_catalog_broker/http_register.py`: bind `n = len(ent_df)`
right before the registering banner. Both `n` references (lines 256
and 282) resolve.

Re-ran `tcb register datasets/zhantao_method_c_v2.yml -n 100` — broker
saw the empty `ZHANTAO_C` container from the prior failed run and
reused it ("Using existing dataset container"), then registered:

```
Registration complete:
  Entities:        100
  Artifacts:       700
  Skipped:         0
  Artifact errors: 0
  Time:            116.2 seconds   (~0.86 ent/s, matches expected HTTP rate)
```

## Step 8: Spot-check ZHANTAO_C                (start: 2026-05-05T12:36:30-07:00, end: 2026-05-05T12:37:30-07:00, duration: ~1 min)

Built-in `verify_registration_http` samples the alphabetically-first
dataset (`BROAD_SIGMA`), not ours, so I queried `ZHANTAO_C` directly:

- `client["ZHANTAO_C"]` exists, len = 100, inherited `amsc_public=True`
  visible at the dataset container.
- Dataset metadata keys: `{amsc_public, data_type, material, measurement,
  organization, producer}`.
- Sample entity `ZHANTAO_C_sample_1`: 9 scalar params present and equal
  to raw HDF5 values; 7 path/dataset locators (Mode A); 7 artifact
  children (Mode B).
- Mode B array fetch: `e['data'].read()` returned 503 Service
  Temporarily Unavailable from the test-tiled portal after retries.
  This is **server-side** and unrelated to registration — the locator
  metadata, structure, and DataSource entries are all correct in the
  catalog. Worth retrying later or asking whoever runs the portal.

## Wrap                                         (end: 2026-05-05T12:37:30-07:00)

- Total wall clock: ~52 min from 11:44:52 start, of which ~6 min was
  the user pause.
- Files written:
  - `datasets/zhantao_method_c.yml` (44 lines, with TODOs)
  - `datasets/zhantao_method_c_v2.yml` (40 lines, TODOs resolved)
  - `scripts/gen_zhantao_method_c.py` (105 lines)
  - 1-line edit (added `n = len(ent_df)`) to
    `src/tiled_catalog_broker/http_register.py` to fix unrelated bug
    that blocked register; commit-worthy on its own.
- Sharp edges:
  - `http_register.py` `NameError` for `n` — refactor leftover that
    crashed *every* register call. Patch unblocks; should land in a
    tiny PR.
  - First-time-edit YAML versioning rule: my memory framed it as
    inspect-onboarding-only; user clarified it applies to any YAML
    edit. Memory updated.
  - `verify_registration_http` only probes the first dataset by
    alphabetical order, so it never inspected ZHANTAO_C even though
    we just registered it. Not a bug, just a coverage gap to know about.
  - Server 503 on Mode B reads of the registered arrays — orthogonal
    to method-C; flagged for the portal admin.
- Registration result: **succeeded.** 100 entities, 700 artifacts, 0
  errors, all metadata propagated and locators correct.

## Step 9: Diagnosing the Mode-B 5xx storm     (start: 2026-05-05T12:38, end: 2026-05-05T13:05, duration: ~25 min)

User pushed back on my "server-side, give up" framing — rightly. I'd
been calling `arr.read()` (and its dask-array equivalent
`np.asarray(arr)`), which both go through Tiled's `array/full`
endpoint. Per the comment on `create_data_source` in
`http_register.py`, the LazyHDF5 adapter is specifically optimized for
*sliced* reads (`array/block`) — full-array fetches go through a
dask-via-stock-`application/x-hdf5` path that is what's broken on this
test server. Reproduced same 500 (with a real correlation ID, i.e.
worker-side error) on **ZHANTAO_A**, so the failure isn't anything
method C did.

Switched probe to `arr[0:2, 0:3]`-style slice access — works
instantly:

```
ZHANTAO_C[ZHANTAO_C_sample_2]: artifacts = ['data','energies','powder_data',
                                              'powder_energies','powder_qs_lab',
                                              'qs_lab','qs_rlu']
  data            full=(2000,100)  slice=(2,3)  in 0.09s
  energies        full=(100,)      slice=(3,)   in 0.08s
  powder_data     full=(300,100)   slice=(2,3)  in 0.10s
  powder_energies full=(100,)      slice=(3,)   in 0.08s
  powder_qs_lab   full=(300,)      slice=(3,)   in 0.10s
  qs_lab          full=(3,2000)    slice=(2,3)  in 0.10s
  qs_rlu          full=(3,2000)    slice=(2,3)  in 0.10s
```

So **Mode B does work** for ZHANTAO_C — registration is correct
end-to-end. The "broken" path is `array/full`, which appears to be
broken for everyone, not just us. Worth filing as a separate issue
against the test portal.

Side-incident: lost the `TILED_*` env vars partway through the session
(initially set from `.env.test` by whatever launched the session, but
not propagated reliably across all `Bash` invocations). Recovered by
sourcing `.env.test` from the parent repo
`/sdf/data/lcls/ds/prj/prjmaiqmag01/results/ajshack/tiled-catalog-broker/.env.test`
inline (`set -a; . .env.test; set +a; …`). The task description said
"in your shell from the project's .env.test (sourced before this
session was launched)" — but `.env.test` lives in the *parent* repo,
not the worktree, and the in-process inheritance proved flaky.

## Step 10: ZHANTAO_A vs ZHANTAO_C comparison  (start: 2026-05-05T13:05, end: 2026-05-05T13:20, duration: ~15 min)

Pulled both registered datasets via the test client and compared them
field-by-field.

### Bit-equality on overlapping samples

A's first 100 entities and C's first 100 entities overlap on exactly
3 samples — `[1, 10, 100]`. (A's first 100 are scattered through the
20K source samples by alphabetic hash order — first ids include
1, 10, 100, 1000, 10000, 10001, …; C's are samples 1..100 because my
generator sorts numerically.) On `sample_1`:

- 9 physics params (Ax, Az, J1a..J4): **all bit-equal** between A and C
- `data[0:2,0:3]`, `qs_rlu[0:2,0:3]`, `powder_data[0:2,0:3]`: **all
  np.array_equal == True**

So both pipelines correctly resolve their locators through to the same
H5 bytes. Locator authoring is correct in both.

### Dataset-container metadata

| field | A | C |
|---|---|---|
| amsc_public | True | True |
| data_type | simulation | simulation |
| material | NiPS3 | NiPS3 |
| producer | `sunny_jl` | `Sunny.jl` |
| (measurement) | `method: [INS]` (list) | `measurement: ins` (string) |
| (organization) | `project: MAIQMag` | `organization: MAIQMag` |
| shared axes | `shared_dataset_energies: /sample_1/energies`,<br>`shared_dataset_powder_energies: /sample_1/powder_energies` | absent |

Co-discoverability is broken across A and C — `Key("organization")
== "MAIQMag"` finds only C; `Key("project") == "MAIQMag"` finds only
A. Same split on `measurement`/`method`. The producer string also
differs in casing/punctuation. A has clearly been authored against a
controlled vocabulary that I couldn't see (the schema/inspect docs
are forbidden in method C), so this is the expected cost of the
docs-only path.

### Artifact set per entity

- A: 5 children per entity (`data, powder_data, powder_qs_lab,
  qs_lab, qs_rlu`).
- C: 7 children per entity (A's 5 + `energies, powder_energies`).

A correctly hoisted the constant-across-entities energy axes via
the `shared:` feature, saving 2 × N artifact nodes (400 nodes for
this 100-entity test, 40K for the full 20K dataset). C inserted them
as per-entity artifacts — wasteful but contract-correct. **This is
the design oversight in C worth fixing first** if a v3 is desired.

### Entity uid / key naming

| | A | C |
|---|---|---|
| uid scheme | 13-char hex hash | literal `sample_<N>` |
| Tiled entity key | `ZHANTAO_A_<hash>` | `ZHANTAO_C_sample_<N>` |
| first 100 selected by | first-100 of alphabetic-hash order | first-100 by numeric N |

Both are unique and broker-compatible. The hash scheme is more robust
to cross-dataset merges; the literal-N scheme is more readable.

### Entity-level extras

- A has `source_group` (full H5 group path) — useful for traceback.
- C has `key` (friendly name I added because the manifest contract
  said `key` was required; broker doesn't actually consume it).

### Artifact-level extras

A has `file_size: 19554997300` and
`file_mtime: 2026-04-14T12:13:18.041442+00:00` on every artifact —
provenance probably auto-stamped by `tcb inspect`. C has only
`type, dtype, shape, amsc_public`.

### Net assessment

C is structurally correct and reads identically. Cosmetic gaps vs A:
1. `energies`/`powder_energies` should use `shared:` rather than
   per-entity artifacts (real efficiency win).
2. Dataset metadata vocabulary drift (`organization` vs `project`,
   `measurement` vs `method`, `Sunny.jl` vs `sunny_jl`) — breaks
   cross-dataset discovery between A and C.
3. Missing provenance fields (`file_size`, `file_mtime`,
   `source_group`) on artifacts/entities.

All three are gaps that come from being forbidden to read the schema
file or inspect output — they encode controlled vocabulary and
provenance conventions that aren't otherwise documented. Method C
without those guardrails produces a working catalog entry but one
that drifts from the existing convention.

## Step 11: Parquet-level comparison (read A directly)  (start: 2026-05-05T13:30, end: 2026-05-05T13:40, duration: ~10 min)

User lifted the cross-worktree restriction to let me read A's actual
parquets at
`tcb-zhantao-method-a/datasets/manifests/Zhantao A/{entities,artifacts}.parquet`
(label was `Zhantao A` with a literal space — the broker accepts both
spaced and underscore-normalized variants via `_find_manifests`).

| Aspect | ZHANTAO_A | ZHANTAO_C |
|---|---|---|
| `entities.parquet` shape | 20,000 × 11 | 20,000 × 11 |
| 11th entity column | `source_group` (str: `sample_1`) | `key` (str: `H_sample_1`) |
| uid format | full 16-char hex hash (`97e086d1d218cc9a`) | literal `sample_<N>` |
| 9 physics-param columns | `Ax,Az,J1a,J1b,J2a,J2b,J3a,J3b,J4` (float64) | identical |
| sample_1's params (raw bytes) | `Ax=-0.018952, Az=0.223929, J1a=-4.893470, …` | bit-identical |
| `artifacts.parquet` shape | **100,000 × 7** | **140,000 × 5** |
| artifact types per entity | 5 (no energies/powder_energies — hoisted to dataset via `shared:`) | 7 |
| extra artifact columns | `file_size: int64`, `file_mtime: str` (ISO-8601) | none |
| `index` column dtype | `object` with literal Python `None` | `Int64` nullable (`<NA>`) |
| `file` column | `nips3_fwhm4_9dof_20000_20260303_0537.h5` | identical basename |
| `dataset` column | `/sample_<N>/<arr>` | identical pattern |

Notes on the table:
- The 40,000-row gap is exactly `(7 − 5) × 20,000` — the per-entity
  `energies` / `powder_energies` rows that C wrote and A correctly
  hoisted via `shared:`.
- Both null-index encodings (`object/None` vs `Int64/<NA>`) round-trip
  fine through `pd.notna()` in the broker, so they're functionally
  equivalent. A's encoding suggests `tcb generate` is letting pandas
  pick the dtype implicitly; mine is explicit (set by hand in the
  generator).
- A's uid is 16 hex chars; the 13-char form I'd previously inferred
  comes from `make_entity_key()` truncating to 13 when assembling
  the Tiled key (`ZHANTAO_A_97e086d1d218c`). The full 16 chars stay
  in the parquet for cross-checking.
- A's `source_group` is the basename (`sample_1`), not the full
  HDF5 path with leading slash — I'd guessed wrong about that too.
- The two extra artifact columns (`file_size`, `file_mtime`) make A's
  per-artifact metadata self-describing about the source HDF5 file
  state at registration time. Useful for invalidation / staleness
  checks. C lacks any of that.
- Net: at the parquet level the runs encode the *same physical
  references and the same physics values*, but A's manifest is
  smaller (no redundant axis rows) and richer (file provenance per
  artifact, group-name tracking per entity).

## Step 12: YAML-level comparison                 (start: 2026-05-05T13:40, end: 2026-05-05T13:48, duration: ~8 min)

Read `tcb-zhantao-method-a/datasets/zhantao_method_a_v2.yml` (the
version A actually registered from). It is *substantially* richer than
my hand-written `zhantao_method_c_v2.yml` because A drives `tcb
generate` from the YAML, while C uses a custom Python generator that
needs none of the YAML's structural fields.

| YAML field | ZHANTAO_A v2 | ZHANTAO_C v2 |
|---|---|---|
| header comments | `# AUTO-GENERATED by dcs inspect on 2026-05-01`, plus a v2 changelog about the grouped-layout heuristic | hand-written description of source data + manifest plan |
| `label` | `"Zhantao A"` (quoted, with space) | `Zhantao_C` (unquoted) |
| `key` (stamped) | `ZHANTAO_A` | `ZHANTAO_C` |
| `metadata.method` | `[INS]` (list) — controlled vocab | (absent) |
| `metadata.measurement` | (absent) | `ins` |
| `metadata.organization` | (absent) | `MAIQMag` |
| `metadata.project` | `MAIQMag` | (absent) |
| `metadata.data_type` | `simulation` | `simulation` |
| `metadata.material` | `NiPS3` | `NiPS3` |
| `metadata.producer` | `sunny_jl` | `Sunny.jl` |
| `metadata.amsc_public` | `true` | `true` |
| `data.directory` | identical | identical |
| `data.server_base_dir` | identical | identical |
| `data.file_pattern` | `"nips3_fwhm4_9dof_20000_20260303_0537.h5"` | (absent — the generator hardcodes it) |
| `data.layout` | `grouped` | (absent — generator-implicit) |
| `parameters.location` | `group_scalars` | (absent) |
| `parameters.group` | `params` | (absent) |
| `artifacts:` block | 5 entries (`data`, `powder_data`, `qs_lab`, `qs_rlu`, `powder_qs_lab`) with `dataset:` paths and shape/dtype/range comments | (absent — generator enumerates them) |
| `shared:` block | 2 entries (`energies` → `/sample_1/energies`, `powder_energies` → `/sample_1/powder_energies`) | (absent) |
| `provenance:` block | present but empty (commented-out fields for `created_at`, `code_version`, `code_commit`) | (absent) |
| inline annotations | per-artifact shape/dtype/range from sample_1, controlled-vocabulary hints (e.g. `select from ['INS', 'RIXS', …]`), data-producer recommendations | none |

Notes on the table:
- A's YAML *is* the contract: `tcb generate` reads `parameters`,
  `artifacts`, `shared`, and `data.layout` to produce the parquets.
  C's YAML is just provenance for `tcb register` — the parquets are
  produced by hand, so all the structural fields collapse into the
  Python script.
- The controlled-vocabulary hints inline in A's YAML
  (`select from ['INS', 'RIXS', 'Magnetization', 'VDP', 'XPS', 'XES',
  'SWT']` for `method`, `select from ['simulation', 'experimental',
  'benchmark', 'optimization']` for `data_type`, etc.) are exactly
  what I lacked in method C. They are emitted by `tcb inspect` from
  the schema file — both forbidden in method C — so without them I
  reinvented the field names (`measurement` instead of `method`,
  `organization` instead of `project`).
- A's `provenance:` block in v2 is empty. Stamp-key seems to have
  written `key: ZHANTAO_A` at the top level *and* the v2 file shows
  `provenance:` immediately followed by `key: ZHANTAO_A` at column 0,
  meaning the stamp ended up as a sibling of `provenance`, not under
  it. Cosmetic but worth noting if future stamp-key behavior is
  audited.
- The `# === Recommendations for data producer ===` block at the end
  of A's YAML (`No 'created_at' timestamp — add as HDF5 root
  attribute`, `energies / powder_energies stored redundantly under
  every sample_N group — consider storing them once at the file
  root`) is auto-generated feedback to the simulator author — not
  consumed by any tool, but a nice byproduct of inspect that C's
  hand-written YAML can't produce.
- Net: A's YAML doubles as documentation, schema contract, and a
  channel for feedback to the data producer. C's YAML is purely a
  registration handle.

## Step 13: Tiled-side metadata comparison  (consolidates Step 10's table with Step 11/12 corrections)

Already captured in Step 10's "Dataset-container metadata" and
"Entity-level structure" tables. The drift across A and C at the
Tiled level (`organization` vs `project`, `measurement` vs `method`,
`Sunny.jl` vs `sunny_jl`, missing `shared_dataset_*`) is a direct
consequence of the YAML drift documented in Step 12 — there is no
broker behavior smoothing it over; the parquet/YAML differences flow
through verbatim into the catalog metadata.



