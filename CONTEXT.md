# Tiled Catalog Broker

A config-driven system for registering multi-modal scientific HDF5 datasets into a
Tiled catalog so they can be consistently registered and discovered. The broker is
**dataset-agnostic** but **not structure-agnostic**: producers must shape their data to
one of a small, fixed set of supported layouts.

## Language

### Hierarchy

**Dataset**:
A top-level container (VDP, EDRIXS, SUNNY, ...) with provenance metadata. Holds entities.
_Avoid_: collection, repo.

**Entity**:
A container whose physics parameters are queryable metadata; the unit a query selects.
_Avoid_: sample, record, row.

**Artifact**:
An array child of an entity (a spectrum, a magnetization curve). The thing arrays load from.
_Avoid_: observable (that's the physics word; in the catalog it's an artifact), output.

These three levels are nested Tiled **containers**, and an entity's parameters are a
**free-form metadata dict** rather than declared columns — benchmarked against the flat and
typed-column alternatives and kept because it is the only shape compatible with being
dataset-agnostic (ADR-0004).

### Onboarding

**Onboarding**:
The end-to-end process of taking a producer's HDF5 dataset and registering it into the
catalog: author a dataset YAML (agent or human, against the contract surface) →
`tcb generate` (manifests) → `tcb stamp-key` (key into the YAML) → `tcb register`.
Registration has exactly one route, HTTP `tcb register` (ADR-0002); learning what a valid
dataset looks like is the contract surface's job (see below), not a command's.
_Avoid_: ingest, inspect.

**Layout**:
The physical on-disk arrangement of a dataset's HDF5 files. Exactly **three** are
supported and this set is frozen (see ADR-0001):
- `per_entity` — one HDF5 file per entity; scalars are parameters.
- `batched` — entities stacked along axis-0 of datasets within each file.
- `grouped` — one HDF5 group per entity inside a single file.
_Avoid_: format, structure (reserve "structure" for the broader data contract).

**Data contract**:
The full set of rules a producer's data + YAML must satisfy to be onboarded: one of the
three layouts, the YAML field contract, and the controlled vocabulary. "We tell users
their data must follow this — we do not support arbitrary configurations."

**Dataset YAML**:
The per-dataset config file that declares identity, layout, artifacts, parameters, and
metadata. The human-and-agent-authored input contract; the Parquet manifest is the output.

**Semantic model**:
The controlled vocabulary in `tools/schema/catalog_model.yml` — canonical ids (and
aliases) for `method`, `material`, `producer`, `project`, `facility`, `data_type`. It keeps
cross-dataset discovery (`Key("project") == "MAIQMag"`) honest. It is **soft**: unknown
values warn (not error), aliases normalize to canonical ids (ADR-0003). It is a
normalization + reference layer, not a rigid gate.
_Avoid_: schema (overloaded — the structural pydantic models are also a "schema").

**Contract surface**:
The referenceable artifacts that define what a valid dataset is — the pydantic YAML models
(`tools/_models.py`), the semantic model, the layout definitions, `docs/ONBOARDING.md`, and
the annotated `datasets/examples/*.yml`. The onboarding principle is **implementation vs.
contract**: an agent (or human) onboards by reading the *contract surface*, never by reading
broker *implementation* (`generate.py`, `http_register.py`). Needing to reverse-engineer
implementation to learn what a valid dataset looks like is a gap in the contract surface —
fix the surface.
_Avoid_: "source" (ambiguous — distinguish contract artifacts from implementation code).

## Relationships

- A **Dataset** contains one or more **Entities**; an **Entity** contains one or more **Artifacts**.
- A **Dataset YAML** describes exactly one **Dataset** and declares its **Layout**.
- The **Semantic model** constrains the metadata vocabulary used across all **Datasets**.

## Flagged ambiguities

- **Controlled-vocabulary drift (resolved).** The #72 study found agents without sight of
  the semantic model independently invented `measurement` (use **`method`**),
  `organization` (use **`project`**), and `Sunny.jl` (use the canonical id **`sunny_jl`**).
  These synonyms break cross-dataset queries; the semantic model's canonical ids are
  authoritative.
- **"schema"** is overloaded: it means both the structural contract (pydantic models in
  `tools/_models.py`) and, loosely, the semantic vocabulary. Prefer **semantic model** for
  the vocabulary and **YAML contract / pydantic models** for structure.
