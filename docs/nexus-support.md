# NeXus files and the dataset contract

**Audience:** anyone onboarding NeXus-structured HDF5 (and anyone asking "does the broker
understand NeXus?").
**Short answer:** the contract stays generic — nothing in it names a NeXus class — but a
NeXus file maps onto it cleanly, and the onboarding skill knows the mapping.

This note records how the mapping works, what motivated it, and what was deliberately
*not* modelled. It is the design companion to `datasets/examples/per_entity_nexus.yml`.

---

## 1. Why this came up

A tester onboarded a SEQUOIA S(Q,E) reduction (`Ei60_NiPS3_SQE.h5`, one NeXus file per
incident energy) and hit two things:

1. The scalars that describe an entity were spread over `NXinstrument`, `NXcollection`
   (`parameters/`) and `NXsample`, but `parameters.group` could name **one** group, so
   temperature and chemical formula were left out.
2. The NXdata group (`data`, `data_errors`, `E`, `Q`) was flattened into separate arrays
   with nothing recording their relationship or their units.

The NeXus committee will be in the room at NOBUGS, so "restructure your file to fit our
tool" is not an answer. Neither is hard-coding NeXus into a broker whose whole point is to
be dataset-agnostic. The resolution: make the **generic** contract able to express what a
NeXus file contains, and put the NeXus *knowledge* in onboarding.

## 2. What the contract gained (generic, not NeXus)

| Need in a NeXus file | Contract feature | Result in the catalog |
|---|---|---|
| Scalars in several groups | `parameters.groups: {name: /path, ...}` | nested entity metadata: `metadata.instrument.Ei`, `metadata.sample.mass` — queryable as `Key("instrument.Ei") > 50` |
| Nested groups (`NXdetector` inside `NXinstrument`) | `parameters.recursive: true` | one level deeper per subgroup: `metadata.instrument.detector.distance` |
| Free-form collections with junk in them (`NXcollection`, a JSON blob) | `parameters.exclude: [/path, ...]` | skipped |
| `@units`, `@long_name` on fields | automatic — every scalar attribute of a captured field rides along as `<field>_<attr>` | `metadata.instrument.Ei_units = "meV"` (labels; not part of the entity UID) |
| `@units`, `@long_name` on arrays | automatic — an artifact dataset's scalar attributes become its array node's metadata | `entity["E"].metadata["units"] == "meV"` |
| Several `NXentry` per file | the existing `grouped` layout | one entity per entry |
| Soft / external links (`NXdata/data` → `NXdetector/data`) | nothing: the HDF5 library resolves links; the broker records the path it was given | works if an external link's target file sits at the same relative location on the server |

None of this names a NeXus class. A plain HDF5 file with scalars in `/settings/` and
`/sample/` gets exactly the same treatment.

## 3. How a NeXus tree maps (what the onboarding skill does)

```
NXroot@default → entry                 one NXentry per file  → layout: per_entity
NXentry@default → data                 (several per file     → layout: grouped, entity_group: /)
  NXdata: @signal field, @axes fields, FIELDNAME_errors
                                        → artifacts (axes → shared: only if identical in every file)
  NXinstrument / NXsample / NXparameters / NXcollection
                                        → parameters.groups, one entry each, named after the group
  definition / title / start_time       → a groups entry for the entry itself (metadata.entry.*)
  NXprocess (program, version)          → provenance:
  fields constant across all files (sample/chemical_formula, instrument/name)
                                        → candidates for dataset metadata (material, facility) — user confirms
```

`datasets/examples/per_entity_nexus.yml` is this mapping written out for the motivating file.

## 4. Deliberately not modelled (yet)

- **NXdata as a unit.** `@signal` / `@axes` / `FIELDNAME_errors` linkage is not recorded; the
  four arrays are sibling artifacts under the entity. With `long_name`/`units` on each node
  and sensible artifact names, the roles are evident to a reader; a client that wants "the
  whole NXdata group" fetches the siblings. The alternatives — an `axes_<artifact>` metadata
  convention, or registering the NXdata group as a container / xarray-style node — were
  considered and parked: the first caters to one format, the second changes the tree shape
  and needs a benchmark before it's adopted. Revisit if a consumer actually needs it.
- **Application definitions.** `definition` is captured as metadata like any other scalar;
  nothing validates a file against `NXsqom` or any other definition.
- **Identity.** Every captured parameter (now including `start_time`, `title`, ...) enters the
  content-addressed entity UID. A "store but don't hash" switch (`not_in_uid`) was discussed
  and left out until a dataset needs it; for experimental data each file is its own
  measurement anyway.
- **Units arithmetic.** `Ei_units` is a label, nothing converts or checks it.

## 5. Manifest shape (for the curious)

Still one row per entity, one column per parameter. A group's field is the column
`<group>.<field>`; registration splits on `.` and nests. Artifact rows gain one column per
distinct attribute name found on any artifact dataset. A `shared:` axis is an artifact row
with no `uid` — an artifact of the dataset container rather than of an entity. There is no
separate shared-axis manifest.
