# Three-level container hierarchy with free-form metadata

The broker registers every dataset as **three levels of nested Tiled containers** — Dataset →
Entity → Artifact — with each entity's physics parameters carried as a **free-form metadata
dict**, not as declared typed columns. Tiled stores that shape as three related tables with
JSONB-backed metadata on the container rows.

We considered flattening the hierarchy (one denormalized row per artifact) and/or promoting
parameters to native typed columns. Both are rejected.

## Evidence

A 2×2 benchmark measured all four combinations — hierarchical vs flat × JSONB vs native
columns — against PostgreSQL 15 across a 7-query suite at three scales (100 / 1,000 / 10,000
entities, up to ~3M rows in the flat case).

**Hierarchical vs flat.** The join cost of the three-level hierarchy is negligible on the
filtered queries that dominate discovery — a flat table wins only on single-entity artifact
retrieval (~0.13 ms vs ~0.8 ms at large scale). Against that, flattening loses badly
elsewhere: `COUNT(DISTINCT entity)` for aggregation runs ~500 ms vs ~5 ms hierarchical, a
dataset-level metadata update touches N×M rows instead of 1, and dataset/entity metadata is
duplicated onto every artifact row.

**JSONB vs native columns.** JSONB's overhead is minimal once expression indexes exist —
hierarchical+JSONB tracked hierarchical+native closely across the suite. The flat+JSONB
control was worst at scale (10–50× slower than hierarchical+JSONB on the filter and range
queries), confirming the two costs compound rather than cancel.

## Decision

Hierarchical containers, free-form metadata. This is the combination the benchmark ranked
best overall, and it is the only one of the four compatible with the broker being
**dataset-agnostic**.

That last point is decisive independent of the timings. Going native means every dataset's
parameters must live in a shared column superset: the benchmarked flat+native schema needed
**24 typed float columns** to cover just two producers — Sunny's `Ja_mev`, `Jb_mev`,
`spin_s`, `g_factor` alongside EDRIXS's `Udd`, `Upd`, `Delta`, `crystal_10Dq`, `zeta_d`… —
with every dataset NULL-padded against every other. Onboarding a seventh dataset would be an
`ALTER TABLE`, and the broker's core promise (`CLAUDE.md`: "no parameter names, artifact
types, or file layouts are hardcoded") would be false. Free-form metadata is what lets VDP
entities carry `{Ja_meV, spin_s}` and EDRIXS entities carry `{Udd, Delta, crystal_10Dq}` in
the same catalog with no shared schema and no nulls.

## Consequences

- `http_register.py` builds entity metadata from *all* manifest columns dynamically; there is
  no place in the broker where a parameter name is declared. That is load-bearing, not
  incidental — it is the mechanism this ADR protects.
- Single-entity artifact retrieval is the one path where the hierarchy costs measurable time.
  It is sub-millisecond and not on the discovery path, so it is accepted.
- The broker does not implement these tables — Tiled does. This ADR records that the shape the
  broker *registers* was validated end to end, not that the broker controls the storage engine.
- Benchmark harnesses live in the `tiled-bench` repo. Re-measuring this decision against the
  real Tiled schema — rather than the hand-written stand-ins used here — is planned there in
  `DATA-MODEL-BENCHMARK-PLAN.md`, which also states which outcomes would reopen this ADR.
