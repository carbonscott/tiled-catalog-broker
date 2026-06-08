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

Consequence / cleanup: the `dataset_fields` section of `catalog_model.yml` currently
*declares* `data_type`/`method` as "required" with `enum_ref`s, but the validator does not
enforce that (it warns) and the field→vocab mapping is hardcoded in `schema.py` instead.
That section is decorative and contradicts actual enforcement — it should be corrected to
describe what the code really does (advisory), not expanded into a real gate. A pydantic
model of the vocab file's own shape is deferred (low value until the file is edited widely).
