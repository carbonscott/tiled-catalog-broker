"""Pydantic models for the dataset YAML contract.

These models declare the *structural* contract of a dataset config: required fields,
allowed enum values, and conditional requirements. They are part of the **contract
surface** (see CONTEXT.md) — an agent or human can read this file to learn what a valid
dataset YAML looks like, without reading broker implementation.

Two things stay in ``schema.py`` layered on top of a successful parse, by design:

- Controlled-vocabulary checks (``method``/``material``/``producer``/... against
  ``catalog_model.yml``) — these are non-fatal *warnings*, not errors (ADR-0003).
- Filesystem checks (does ``data.directory`` exist) — environment-dependent, so keeping
  them out of the model lets a config be validated without its data present.

Conventions follow ``amsc-connector/core/_models.py`` (pydantic v2, ``ConfigDict``,
``@model_validator``). The broker targets Python >=3.10, so enums use ``(str, Enum)``
rather than ``StrEnum`` (3.11+).

Strictness: the closed sub-sections (``data``, ``artifacts``, ``parameters``, ``shared``)
``forbid`` unknown keys to catch typos. ``metadata`` is extensible so it ``allow``s extras;
the top level ``ignore``s extras for keys that carry no structural rules (``provenance``,
``extra_metadata``, ``artifact_datasets``). Required strings use ``min_length=1`` so an
empty string is treated as missing, matching the prior validator.
"""

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Layout(str, Enum):
    """Allowed values for ``data.layout`` (ADR-0001 — frozen set of three)."""

    per_entity = "per_entity"
    batched = "batched"
    grouped = "grouped"


class ParamLocation(str, Enum):
    """Allowed values for ``parameters.location``."""

    root_scalars = "root_scalars"
    root_attributes = "root_attributes"
    group = "group"
    group_scalars = "group_scalars"
    manifest = "manifest"


class DatasetMetadata(BaseModel):
    # Dataset metadata is extensible — datasets add their own keys (e.g.
    # prior_distribution, round), so allow extras. Vocabulary is checked as
    # warnings in schema.py, not here; every field is optional.
    model_config = ConfigDict(extra="allow")

    method: list[str] | None = None
    data_type: str | None = None
    material: str | None = None
    producer: str | None = None
    project: str | None = None
    facility: str | None = None


class DataSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    directory: str = Field(min_length=1)
    layout: Layout
    file_pattern: str | None = None
    server_base_dir: str | None = None


class ArtifactSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1)
    dataset: str = Field(min_length=1)


class SharedAxisSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1)
    dataset: str = Field(min_length=1)


class ParametersSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    location: ParamLocation | None = None
    group: str | None = None
    manifest: str | None = None

    @model_validator(mode="after")
    def _location_requirements(self):
        if self.location == ParamLocation.group and not self.group:
            raise ValueError("'parameters.group' is required when location is 'group'")
        if self.location == ParamLocation.manifest and not self.manifest:
            raise ValueError(
                "'parameters.manifest' is required when location is 'manifest'"
            )
        return self


class DatasetConfig(BaseModel):
    """Top-level dataset YAML contract.

    Unknown top-level keys (``provenance``, ``extra_metadata``, ``artifact_datasets``) are
    ignored: they carry no structural rules today and are consumed downstream from the raw
    config dict, not from this model.
    """

    model_config = ConfigDict(extra="ignore")

    label: str = Field(min_length=1)
    key: str | None = None
    key_prefix: str | None = None
    metadata: DatasetMetadata = Field(default_factory=DatasetMetadata)
    data: DataSection
    artifacts: list[ArtifactSpec] = Field(min_length=1)
    parameters: ParametersSection | None = None
    shared: list[SharedAxisSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def _require_identity(self):
        if not self.key and not self.key_prefix:
            raise ValueError(
                "'key' is required (dataset container key in Tiled), or set 'key_prefix'"
            )
        return self
