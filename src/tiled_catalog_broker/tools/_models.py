"""Pydantic models for the dataset YAML contract.

These models declare the *structural* contract of a dataset config: required fields,
allowed enum values, and conditional requirements. They are part of the **contract
surface** (see CONTEXT.md) — an agent or human can read this file to learn what a valid
dataset YAML looks like, without reading broker implementation. Every field carries a
``description`` so the contract is self-documenting (and surfaces in the JSON schema).

Two things stay in ``schema.py`` layered on top of a successful parse, by design:

- Controlled-vocabulary checks (``method``/``material``/``producer``/... against
  ``catalog_model.yml``) — these are non-fatal *warnings*, not errors (ADR-0003).
- Filesystem checks (does ``data.directory`` exist) — environment-dependent, so keeping
  them out of the model lets a config be validated without its data present.

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
    - ``group`` — datasets inside a named group (which group: ``parameters.group``).
    - ``group_scalars`` — scalar datasets inside each entity group (grouped layout).
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
    file_pattern: str | None = Field(
        default=None,
        description="Glob for HDF5 files under `directory` (defaults to '**/*.h5').",
    )
    server_base_dir: str | None = Field(
        default=None,
        description="Base directory the Tiled server resolves each file's data_uri "
        "against (derived at inspect time).",
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
        description="HDF5 path to a 1-D axis array shared by all entities (registered once).",
    )


class ParametersSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    location: ParamLocation | None = Field(
        default=None,
        description="Where to read per-entity parameters from; see ParamLocation.",
    )
    group: str | None = Field(
        default=None,
        description="HDF5 group path holding the parameter datasets; required when "
        "location is 'group'.",
    )
    entity_group: str | None = Field(
        default=None,
        description="HDF5 group holding one subgroup per entity (grouped layout); the "
        "generator falls back to 'samples', then to top-level groups.",
    )

    @model_validator(mode="after")
    def _location_requirements(self):
        if self.location == ParamLocation.group and not self.group:
            raise ValueError("'parameters.group' is required when location is 'group'")
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
    parameters: ParametersSection | None = Field(
        default=None,
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
