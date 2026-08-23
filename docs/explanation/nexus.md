# NeXus support

The broker does not implement NeXus classes. Its generic dataset contract can
describe NeXus-structured HDF5, and the onboarding skill knows how to map between
them. See `datasets/examples/per_entity_nexus.yml` for a complete example.

## Why the contract grew

A SEQUOIA S(Q,E) dataset exposed two gaps. Its entity parameters were spread
across `NXinstrument`, `NXcollection`, and `NXsample`, while the contract could
name only one parameter group. Its `NXdata` arrays also carried relationships and
units that disappeared when registered as independent artifacts.

Requiring users to restructure valid NeXus files would make onboarding harder.
Hard-coding NeXus classes would undermine the broker's dataset-agnostic design.
The contract therefore gained generic ways to preserve the useful structure.

## How NeXus concepts map

| NeXus need | Contract feature | Catalog result |
|---|---|---|
| Scalars in several groups | `parameters.groups` | Nested, queryable entity metadata |
| Nested groups such as `NXdetector` | `parameters.recursive: true` | Deeper nested metadata |
| Collections containing unwanted datasets | `parameters.exclude` | Selected paths are skipped |
| Attributes on scalar fields | Automatic capture | Labels such as `instrument.Ei_units` |
| Attributes on arrays | Automatic capture | Metadata on the artifact node |
| Several `NXentry` groups per file | `grouped` layout | One entity per entry |
| HDF5 soft or external links | h5py resolves the link | The broker records the configured path |

The onboarding skill maps a tree using these conventions:

```text
NXroot@default -> entry                 one NXentry per file -> per_entity
                                         several per file -> grouped
NXentry@default -> data
  NXdata signal, axes, and errors       -> artifacts
                                         identical axes -> shared
  NXinstrument, NXsample,
  NXparameters, NXcollection            -> parameters.groups
  definition, title, start_time         -> metadata.entry.*
  NXprocess program and version         -> provenance
  values constant across files          -> dataset metadata, after user review
```

For `parameters.groups`, names become metadata prefixes. A field `Ei` in the
group named `instrument` becomes `instrument.Ei`, which Tiled can query with
`Key("instrument.Ei") > 50`.

Nothing in this mapping names a NeXus class. A plain HDF5 file with scalars in
`/settings/` and `/sample/` receives the same treatment.

## Deliberate boundaries

- **`NXdata` is not a catalog container.** Its signal, axes, and error arrays are
  sibling artifacts. Preserving their formal linkage would require a
  NeXus-specific metadata convention or a different catalog tree; neither has a
  demonstrated consumer yet.
- **Application definitions are not validated.** `definition` is captured as
  metadata, but the broker does not validate against `NXsqom` or another
  definition.
- **Captured parameters determine identity.** Every parameter, including fields
  such as `start_time` and `title`, contributes to the content-addressed entity
  UID. There is no `not_in_uid` option.
- **Units remain labels.** The broker stores `Ei_units`; it does not convert or
  validate units.

For exact field behavior, see [the dataset YAML reference](../reference/dataset-yaml.md)
and [the manifest reference](../reference/manifest.md). To register a real dataset,
follow [How to publish a dataset](../ONBOARDING.md).
