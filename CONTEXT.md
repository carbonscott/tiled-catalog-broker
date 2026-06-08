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

### Onboarding

**Onboarding**:
The end-to-end process of taking a producer's HDF5 dataset and registering it into the
catalog. Target flow: author a dataset YAML (agent or human, against the contract surface)
→ `tcb generate` (manifests) → `tcb register`. There is a **single registration route**
(HTTP `tcb register`); the bulk SQL path (`tcb ingest`) is removed (ADR-0002). `tcb inspect`
is removed — its job is replaced by the contract surface (see below).
_Avoid_: ingest (removed), inspect (removed).

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
The referenceable artifacts that define what a valid dataset is — the pydantic YAML
models, the semantic model, the layout definitions, the YAML-contract doc, and the
example `datasets/*.yml`. The onboarding principle is **implementation vs. contract**: an
agent (or human) onboards by reading the *contract surface*, never by reading broker
*implementation* (`inspect.py`, `generate.py`, `http_register.py`). Today the contract is
trapped inside implementation behavior; this effort lifts it onto the contract surface.
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
