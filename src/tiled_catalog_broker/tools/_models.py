"""Pydantic models for the dataset YAML contract.

These models declare the *structural* contract of a dataset config: required fields,
allowed enum values, and conditional requirements. They are part of the **contract
surface** (see CONTEXT.md) — an agent or human can read this file to learn what a valid
dataset YAML looks like, without reading broker implementation. Every field carries a
``description`` so the contract is self-documenting (and surfaces in the JSON schema).

Controlled-vocabulary checks (``method``/``material``/... against ``catalog_model.yml``)
stay in ``schema.py`` layered on top of a successful parse — they are non-fatal *warnings*,
not errors (ADR-0003).

Strictness: the closed sub-sections (``data``, ``artifacts``, ``parameters``, ``shared``)
``forbid`` unknown keys to catch typos. ``metadata`` is extensible so it ``allow``s extras;
the top level ``ignore``s extras for keys that carry no structural rules (``provenance``,
``extra_metadata``, ``artifact_datasets``). Required strings use ``min_length=1`` so an
empty string is treated as missing.
"""

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Layout(StrEnum):
    """How entities are physically packed in the HDF5 data (``data.layout``).

    Frozen set of three (ADR-0001):

    - ``per_entity`` — one HDF5 *file* per entity; parameters are scalars in that file.
    - ``batched`` — many entities stacked along axis-0 of shared datasets within each
      file; entity *i* is row *i* (e.g. ``/spectra`` has shape ``(N, 600)``).
    - ``grouped`` — one HDF5 *group* per entity inside a file, each group self-contained
      (e.g. ``/samples/sample_000/spectrum``).
    """

    per_entity = "per_entity"
    batched = "batched"
    grouped = "grouped"


class ParamLocation(StrEnum):
    """Where a dataset's physics parameters are read from (``parameters.location``).

    - ``root_scalars`` — scalar (0-dim) datasets at the file root.
    - ``root_attributes`` — root-level HDF5 attributes (``f.attrs``).
    - ``group`` — datasets inside a named group: one group (``parameters.group``, flat
      metadata) or several (``parameters.groups``, nested under each group's name).
    - ``group_scalars`` — scalar datasets inside each entity group (grouped layout);
      ``parameters.group`` / ``parameters.groups`` are then resolved *relative to* each
      entity group.

    Wherever a parameter is read from a dataset, that dataset's scalar HDF5 attributes —
    whatever the producer attached: ``units``, ``long_name``, ``description``, ... — are
    carried along as ``<field>_<attr>`` siblings (``Ei_units``). These are labels, not
    parameters: they are stored on the entity but do not enter its content-addressed UID.
    (``root_attributes`` parameters are themselves attributes and carry no labels.)
    """

    root_scalars = "root_scalars"
    root_attributes = "root_attributes"
    group = "group"
    group_scalars = "group_scalars"


class DatasetMetadata(BaseModel):
    # Extensible — datasets add their own keys (e.g. prior_distribution), so allow
    # extras. Values are soft-checked as warnings in schema.py, not gated (ADR-0003).
    model_config = ConfigDict(extra="allow")

    method: list[str] = Field(
        min_length=1,
        description="Scientific methods/observables, e.g. ['RIXS'] — at least one "
        "required (values soft-checked against the vocab).",
    )
    data_type: str = Field(
        min_length=1,
        description="'simulation' or 'experimental' — required (value soft-checked).",
    )
    material: str = Field(
        min_length=1,
        description="Target material or system, e.g. 'NiPS3' — required (value soft-checked).",
    )
    producer: str | None = Field(
        default=None,
        description="Code that produced the data, e.g. 'edrixs' — typically for simulations.",
    )
    project: str | None = Field(
        default=None,
        description="Scientific project or collaboration (value soft-checked).",
    )
    facility: str | None = Field(
        default=None,
        description="Facility where data was collected — typically for experiments.",
    )


class DataSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    directory: str = Field(
        min_length=1,
        description="Filesystem path to the dataset's data root (where the HDF5 files live).",
    )
    layout: Layout = Field(
        description="How entities are physically packed in the HDF5 data; see Layout.",
    )
    file_pattern: str = Field(
        default="**/*.h5",
        description="Glob for HDF5 files under `directory`.",
    )
    server_base_dir: str | None = Field(
        default=None,
        description="Base directory the Tiled server resolves each file's data_uri "
        "against; set when the server sees the data at a different mount than the author.",
    )


class ArtifactSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(
        min_length=1,
        description="Human-readable artifact name; becomes the array's key under its entity.",
    )
    dataset: str = Field(
        min_length=1,
        description="HDF5 path to the array. Resolved per layout: as-is for per_entity/"
        "batched, relative to each entity group for grouped.",
    )


class SharedAxisSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(
        min_length=1,
        description="Name of the shared axis, e.g. 'energy'; becomes its key under the dataset.",
    )
    dataset: str = Field(
        min_length=1,
        description="Absolute HDF5 path (in every layout, incl. grouped) to an axis array "
        "identical for all entities; read from the first file that holds it and "
        "registered once as an array child of the dataset container.",
    )


class ParametersSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    location: ParamLocation = Field(
        description="Where to read per-entity parameters from; see ParamLocation.",
    )
    group: str | None = Field(
        default=None,
        description="A single HDF5 group holding the parameter datasets; its fields "
        "become flat entity metadata. For location 'group', exactly one of "
        "`group` / `groups` is required.",
    )
    groups: dict[str, str] | None = Field(
        default=None,
        description="Several HDF5 groups, keyed by the name each is nested under in the "
        "entity metadata: {instrument: /entry/instrument, sample: /entry/sample} -> "
        "metadata.instrument.Ei, metadata.sample.mass. Paths are absolute for "
        "per_entity/batched and relative to each entity group for grouped.",
    )
    recursive: bool = Field(
        default=False,
        description="Descend into subgroups of each parameter group, nesting their "
        "scalars one level deeper per subgroup (metadata.instrument.detector.distance). "
        "Default: direct children only.",
    )
    exclude: list[str] = Field(
        default_factory=list,
        description="HDF5 paths of datasets or subgroups to skip when reading "
        "parameters (same frame as the group paths) — e.g. a large JSON blob.",
    )
    entity_group: str | None = Field(
        default=None,
        description="HDF5 group holding one subgroup per entity (grouped layout); the "
        "generator falls back to 'samples', then to top-level groups.",
    )

    @model_validator(mode="after")
    def _location_requirements(self):
        if self.group and self.groups:
            raise ValueError(
                "'parameters.group' and 'parameters.groups' are mutually exclusive — "
                "to nest a single group, name it under `groups`"
            )
        if self.location == ParamLocation.group and not (self.group or self.groups):
            raise ValueError(
                "one of 'parameters.group' or 'parameters.groups' is required when "
                "location is 'group'"
            )
        if self.location in (ParamLocation.root_scalars, ParamLocation.root_attributes) \
                and (self.group or self.groups):
            raise ValueError(
                f"'parameters.group'/'groups' do not apply to location "
                f"'{self.location.value}' — use location 'group' to read from groups"
            )
        if self.groups is not None and not self.groups:
            raise ValueError("'parameters.groups' must name at least one group")
        return self


class DatasetConfig(BaseModel):
    """Top-level dataset YAML contract.

    Unknown top-level keys (``provenance``, ``extra_metadata``, ``artifact_datasets``) are
    ignored: they carry no structural rules today and are consumed downstream from the raw
    config dict, not from this model.
    """

    model_config = ConfigDict(extra="ignore")

    label: str = Field(
        min_length=1,
        description="Human-readable dataset name; also the basis for the derived UID namespace.",
    )
    key: str | None = Field(
        default=None,
        description="Dataset container key in Tiled (human-readable, e.g. 'BROAD_SIGMA'); "
        "required — written by `tcb stamp-key` as slug(label).",
    )
    metadata: DatasetMetadata = Field(
        description="Dataset-level provenance and discovery metadata (queryable facets); "
        "required (must carry method, data_type, material).",
    )
    data: DataSection = Field(
        description="Where the data lives on disk and how its entities are laid out.",
    )
    artifacts: list[ArtifactSpec] = Field(
        min_length=1,
        description="The array artifacts each entity exposes (at least one).",
    )
    parameters: ParametersSection = Field(
        description="How to extract each entity's physics parameters from the HDF5.",
    )
    shared: list[SharedAxisSpec] = Field(
        default_factory=list,
        description="Axis arrays shared across all entities, registered once under the dataset.",
    )

    @model_validator(mode="after")
    def _require_key(self):
        if not self.key:
            raise ValueError(
                "'key' is required (dataset container key in Tiled); "
                "run `tcb stamp-key` to derive it from the label"
            )
        return self
