# The soft vocabulary

`Key("material") == "NiPS3"` is only useful if everyone spelled NiPS3 the same way. The
semantic model is how that agreement is kept.

It lives in `src/tiled_catalog_broker/tools/schema/catalog_model.yml` and defines canonical
ids, plus aliases for them, across six fields: `method`, `material`, `producer`, `project`,
`facility`, `data_type`.

## Warnings, not rejections

An unknown value warns and validates. Your dataset onboards either way.

- A known alias is rewritten to its canonical id: `EDRIXS` becomes `RIXS`, `nips3` becomes
  `NiPS3`. Some aliases imply a default too, so `EDRIXS` also implies
  `data_type: simulation`.
- An unknown value is left as you wrote it, and `tcb generate` prints a warning.

The stricter option would be to reject unknown values. That fails badly for the first
person to bring a new material: they cannot register anything until someone edits a
vocabulary file in a repository they may not have write access to. In practice they would
pick a term that does pass, and the catalog would record something untrue. A warning gets
the data in with the value the producer meant, and leaves a record for whoever curates the
model later.

The fields `method`, `data_type`, and `material` must still be present. Only their values
are unchecked.

## What drift costs

Presence can be enforced. Agreement cannot, and this is where it breaks down.

Authors who have not read the model invent reasonable synonyms: `measurement` instead of
`method`, `organization` instead of `project`, `Sunny.jl` instead of the canonical
`sunny_jl`.

None of those are wrong as English, and all of them are invisible to a query written
against the canonical id. The data gets registered and cannot be found, without anything
failing or anyone being told. Rejecting unknown values would have caught it, which is the
trade-off.

So: prefer the canonical ids, and add to the model rather than working around it. Nothing
enforces that, which is why the model is worth a look before you write a `metadata` block.

See also [the `metadata` section](../reference/dataset-yaml.md#metadata) for the fields
themselves.
