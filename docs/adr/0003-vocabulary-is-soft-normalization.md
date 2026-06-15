# Controlled vocabulary is soft normalization, not a rigid gate

The semantic model (`catalog_model.yml`) stays **soft**: unknown metadata values produce
**warnings**, not errors, and known aliases are rewritten to canonical ids
(`resolve_aliases`). We deliberately do **not** make the vocabulary a hard gate that
rejects novel values.

The vocabulary's job is to keep **structured/faceted queries reliable** — so `NiPS3`,
`NIPS`, and `nips3` collapse to one facet rather than fragmenting `Key("material") == ...`.
Canonical ids + aliases achieve that without blocking new values. This is also what keeps
the door open to **more flexible discovery** (semantic/embedding search) later, where the
controlled vocab becomes one facet among several rather than a checkpoint. #72 found the
vocab's value is as a consistency/teaching tool, not a gate; hard enforcement would
contradict that and foreclose the flexible-discovery direction.

Consequence: field *presence* and field *value* are enforced separately. `_models.py`
hard-requires the **presence** of `data_type`, `method`, and `material` (a config missing
one errors), and `catalog_model.yml`'s `dataset_fields: required` list now matches that set.
What stays **soft is the value** — an out-of-vocab `material` validates with only a warning,
per this ADR. The field→vocab mapping that drives those warnings still lives in `schema.py`;
a pydantic model of the vocab file's own shape is deferred (low value until it's edited widely).
