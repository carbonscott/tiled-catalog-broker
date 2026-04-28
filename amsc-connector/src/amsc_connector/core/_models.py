"""
Private core models for the AmSC framework (not for external use).

TODO: THIS IS A TEMPORARY FIX UNTIL CLIENT WORKS
"""

from enum import StrEnum
from typing import Annotated, Any

from pydantic import (
    AliasChoices,
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    field_validator,
)


class EntityTypeEnum(StrEnum):
    """Enum for supported entity types."""

    SCIENTIFIC_WORK = "scientificWork"
    ARTIFACT = "artifact"
    ARTIFACT_COLLECTION = "artifactCollection"
    ML_MODEL = "mlModel"
    TABLE = "table"


class _OwnerRef(BaseModel):
    """Reference to an owner (user, team, or organization).

    Private model - use only internally.
    """

    type: str = Field(..., description="'user' | 'team' | 'organization'")
    id: str = Field(..., description="Unique identifier for the owner")
    name: str | None = Field(default=None, description="Display name")
    email: str | None = Field(default=None, description="Contact email")


class _DistributionItem(BaseModel):
    """File or data distribution information.

    Private model - use only internally.
    """

    format: str | None = Field(
        default=None, description="Format (e.g., 'parquet', 'csv')"
    )
    url: HttpUrl | None = Field(default=None, description="Download or access URL")
    size: int | None = Field(default=None, description="Size in bytes")
    encoding_format: str | None = Field(default=None, description="MIME type")
    extras: dict[str, Any] | None = Field(
        default_factory=dict, description="Additional distribution metadata"
    )


PROCESS_READINESS_CLASSIFICATION_FQN = "ProcessingReadiness"
PROCESS_READINESS_TAG_PREFIX = f"{PROCESS_READINESS_CLASSIFICATION_FQN}."
MUTUALLY_EXCLUSIVE_CLASSIFICATION_FQNS = frozenset(
    {PROCESS_READINESS_CLASSIFICATION_FQN}
)


def _validate_entity_name_without_commas(value: str) -> str:
    """Normalize entity names and reject commas."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("name cannot be empty or whitespace-only.")

    normalized = value.strip()
    if "," in normalized:
        raise ValueError("name cannot contain commas.")
    return normalized


def _validate_optional_parent_fqn(value: str | None, entity_name: str) -> str | None:
    """Validate an optional parent FQN and normalize surrounding whitespace."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"parent_fqn must be a string for {entity_name} entities.")

    normalized = value.strip()
    if not normalized:
        raise ValueError(
            "parent_fqn cannot be empty or whitespace-only. Omit it to default to the "
            "same-catalog catalog container, or provide a ScientificWork, ArtifactCollection, "
            "or same-catalog catalog container."
        )
    return normalized


class Classification(BaseModel):
    """Curated classification family defined in the catalog taxonomy."""

    fqn: str = Field(
        ...,
        description="Fully qualified classification name defined in the catalog taxonomy.",
    )
    name: str = Field(
        ..., description="Human-readable display label for the classification."
    )
    mutually_exclusive: bool = Field(
        ...,
        description="Whether tags in this classification family are mutually exclusive.",
        validation_alias=AliasChoices("mutually_exclusive", "mutuallyExclusive"),
        serialization_alias="mutuallyExclusive",
    )
    description: str | None = Field(
        default=None,
        description="Description of the classification meaning and intended use.",
    )

    @field_validator("fqn", "name")
    @classmethod
    def _validate_required_non_empty_string(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only.")
        return v.strip()

    model_config = ConfigDict(populate_by_name=True)


class Tag(BaseModel):
    """Curated tag defined in the catalog taxonomy."""

    fqn: str = Field(
        ..., description="Fully qualified tag name defined in the catalog taxonomy."
    )
    name: str = Field(..., description="Human-readable display label for the tag.")
    classification: Classification = Field(
        ...,
        description="Classification family this tag belongs to.",
    )
    description: str = Field(
        ...,
        description="Description of the tag meaning and intended use.",
    )

    @field_validator("fqn", "name", "description")
    @classmethod
    def _validate_non_empty_string(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only.")
        return v.strip()


class TagRef(BaseModel):
    """Minimal tag assignment reference used on entity payloads."""

    fqn: str = Field(
        ..., description="Fully qualified name of the assigned catalog tag."
    )

    @field_validator("fqn")
    @classmethod
    def _validate_fqn(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("TagRef fqn cannot be empty or whitespace-only.")
        return v.strip()

    @property
    def classification_fqn(self) -> str:
        """Return the classification family portion of the tag FQN."""
        return self.fqn.split(".", 1)[0]


class _EntityCommonCreate(BaseModel):
    """
    Private base model encapsulating all shared fields across entity types for creation.

    This is the base for shared entity payload validation, including read-only
    entity models such as ``MLModel`` and ``Table``. External consumers should
    NOT instantiate this directly - use public models instead.

    OpenMetadata Field Mappings:

    - fqn → container id
    - name → container name
    - description → container description
    - location → container sourceURL
    - display_name → container displayName
    """

    # Mandatory fields
    name: str = Field(
        ...,
        description="Mutable identifier (e.g., DOI, persistent ID). "
        "Must be unique within its context.",
    )
    description: str = Field(
        ..., description="Short description providing context or additional details."
    )
    type: EntityTypeEnum = Field(
        ...,
        description="Immutable string indicating the supported entity type. Must be 'scientificWork', 'artifact', 'artifactCollection', 'mlModel', or 'table'.",
    )
    location: AnyUrl = Field(
        ...,
        description="Physical or logical location of the item (file path, cloud storage URI, etc.).",
    )

    # Optional fields
    display_name: str | None = Field(
        default=None,
        description="Human-readable version of the name for display purposes.",
        validation_alias=AliasChoices("display_name", "displayName"),
        serialization_alias="displayName",
    )
    tags: list[TagRef] | None = Field(
        default=None,
        description=(
            "Catalog-defined tag references assigned to the entity. On update, "
            "this field uses replace semantics: when provided, it is treated as "
            "the complete desired tag set for the entity rather than a partial "
            "merge. Clients that want to preserve existing tags should first "
            "read the current entity, modify the list, and then submit the full "
            "updated list. *Tags assigned to a parent entity are not inherited "
            "automatically by child entities; assign tags explicitly on each "
            "entity that should carry them.*"
        ),
    )

    @field_validator("type", mode="before")
    @classmethod
    def _coerce_type_from_string(cls, v) -> EntityTypeEnum:
        """Coerce string values into EntityTypeEnum so callers can pass strings."""
        # Allow the user to pass either an EntityTypeEnum or its string value
        if isinstance(v, EntityTypeEnum):
            return v
        if isinstance(v, str):
            try:
                return EntityTypeEnum(v)
            except ValueError as ve:
                raise ValueError(
                    f"Invalid type: {v}. Must be one of 'scientificWork', 'artifact', 'artifactCollection', 'mlModel', or 'table'."
                ) from ve
        raise TypeError(f"Type must be a string or EntityTypeEnum, got {type(v)!r}")

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str) -> str:
        return _validate_entity_name_without_commas(v)

    @field_validator("tags")
    @classmethod
    def _validate_tags(cls, v: list[TagRef] | None) -> list[TagRef] | None:
        if v is None:
            return None

        seen_fqns: set[str] = set()
        mutually_exclusive_by_classification: dict[str, list[str]] = {}

        for tag_ref in v:
            if not isinstance(tag_ref, TagRef):
                raise ValueError("All tags must be TagRef instances.")
            if tag_ref.fqn in seen_fqns:
                raise ValueError(
                    f"Duplicate tag assignment is not allowed: {tag_ref.fqn}"
                )
            seen_fqns.add(tag_ref.fqn)

            classification_fqn = tag_ref.classification_fqn
            if classification_fqn in MUTUALLY_EXCLUSIVE_CLASSIFICATION_FQNS:
                mutually_exclusive_by_classification.setdefault(
                    classification_fqn, []
                ).append(tag_ref.fqn)

        for (
            classification_fqn,
            assigned_tag_fqns,
        ) in mutually_exclusive_by_classification.items():
            if len(assigned_tag_fqns) > 1:
                raise ValueError(
                    f"Only one tag from mutually exclusive classification "
                    f"'{classification_fqn}' may be assigned to an entity."
                )

        return v

    model_config = ConfigDict(use_enum_values=True, populate_by_name=True)


class _EntityCommon(_EntityCommonCreate):
    """
    Private base model encapsulating all shared fields across entity types.

    This is the base for all public entity models. External consumers should
    NOT instantiate this directly - use public models instead.

    OpenMetadata Field Mappings:

    - fqn → container id
    - name → container name
    - description → container description
    - location → container sourceURL
    - display_name → container displayName
    """

    fqn: str | None = Field(
        default=None,
        description=(
            "Fully qualified name - immutable unique identifier (UUID). "
            "Assigned by catalog and computed from hierarchy."
        ),
    )


class ScientificWork(_EntityCommon):
    """
    Represents a scientific work entity.

    A scientific work can be a parent to Artifact entities and can exist
    standalone. Represents metadata entities such as datasets, studies,
    campaigns, biosamples, and projects.
    """

    parent_fqn: str | None = Field(
        default=None,
        description=(
            "Parent entity FQN generated by the system from catalog hierarchy. "
            "When present, it must reference an existing parent entity that already has an assigned FQN. "
            "Invalid parent references will fail entity resolution in downstream catalog operations."
        ),
        validation_alias=AliasChoices("parent_fqn", "parentFqn"),
        serialization_alias="parentFqn",
    )

    @field_validator("type")
    @classmethod
    def validate_type_is_scientific_work(
        cls, v: EntityTypeEnum | str
    ) -> EntityTypeEnum:
        """Ensure type is 'scientificWork' (not 'artifact')."""
        # First normalize to EntityTypeEnum (same logic as parent)
        if isinstance(v, EntityTypeEnum):
            enum_val = v
        elif isinstance(v, str):
            try:
                enum_val = EntityTypeEnum(v)
            except ValueError as ve:
                raise ValueError(
                    f"Invalid type: {v}. Must be 'scientificWork'."
                ) from ve
        else:
            raise TypeError("type must be an EntityTypeEnum or string")

        # Then check it's scientificWork
        if enum_val is not EntityTypeEnum.SCIENTIFIC_WORK:
            enum_label = f"{enum_val.__class__.__name__}.{enum_val.name}"
            raise ValueError(
                f"Invalid type '{enum_label}' for ScientificWork. Must be 'scientificWork'. "
                f"For artifacts, use the Artifact model with type='artifact'."
            )
        return enum_val

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "fqn": 'osti.catalog."10.15485/1873849"',
                    "name": "10.15485/1873849",
                    "description": "Soil carbon measurements from Arctic sites",
                    "type": "scientificWork",
                    "location": "https://doi.org/10.15485/1873849",
                    "parent_fqn": "osti.catalog",
                    "display_name": "Arctic Soil Carbon Dataset",
                },
                {
                    "fqn": "essdive.catalog.10.15485/1900011",
                    "name": "10.15485/1900011",
                    "description": "Permafrost core metadata package",
                    "type": "scientificWork",
                    "location": "https://doi.org/10.15485/1900011",
                    "parent_fqn": "essdive.catalog",
                    "display_name": "Permafrost Core Study",
                },
            ]
        },
    )


class Artifact(_EntityCommon):
    """
    Represents a concrete artifact (file, resource, or data entity).

    An artifact must have a parent ScientificWork, ArtifactCollection, or
    same-catalog catalog container. Artifact parents are not allowed.
    Represents concrete files, resources, or data entities such as data files,
    documents, and images.
    """

    parent_fqn: str | None = Field(
        default=None,
        description=(
            "Optional parent FQN used to establish parent-child relationships. When omitted, write "
            "operations default it to the catalog container for the supplied catalog_fqn. When "
            "provided, it must resolve to exactly one of: a ScientificWork, an ArtifactCollection, "
            "or the catalog container itself in the same data catalog. Artifact parents and all "
            "other entity types are invalid. Must reference an existing entity that already has an "
            "assigned FQN. Invalid parent FQN references or unsupported parent types will fail "
            "relationship resolution in downstream catalog operations."
        ),
        validation_alias=AliasChoices("parent_fqn", "parentFqn"),
        serialization_alias="parentFqn",
    )
    size: int | None = Field(default=None, description="Size specification in bytes. ")
    format: str | None = Field(
        default=None,
        description="MIME type representing the file format or schema (e.g., CSV, Parquet, JSON).",
    )

    @field_validator("parent_fqn")
    @classmethod
    def validate_parent_fqn(cls, v):
        """Validate that parent_fqn is either omitted or a non-empty string."""
        return _validate_optional_parent_fqn(v, cls.__name__)

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: EntityTypeEnum | str) -> EntityTypeEnum:
        """Validate that type is 'artifact' and return as EntityTypeEnum."""
        # Normalize input to EntityTypeEnum without returning raw strings
        if isinstance(v, EntityTypeEnum):
            enum_val = v
        elif isinstance(v, str):
            try:
                enum_val = EntityTypeEnum(v)
            except ValueError as ve:
                raise ValueError(
                    f"Invalid type: {v}. Must be 'artifact' for Artifact entities."
                ) from ve
        else:
            raise TypeError("type must be an EntityTypeEnum or string")

        if enum_val is not EntityTypeEnum.ARTIFACT:
            enum_label = f"{enum_val.__class__.__name__}.{enum_val.name}"
            raise ValueError(
                f"Invalid type: {enum_label}. Must be 'artifact' for Artifact entities."
            )
        return enum_val

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "fqn": "essdive.10.15485/1873849.soil_profiles.csv",
                    "name": "soil_profiles.csv",
                    "description": "CSV file containing soil profile measurements",
                    "type": "artifact",
                    "location": "https://data.ess-dive.lbl.gov/files/soil_profiles.csv",
                    "parent_fqn": "essdive.10.15485/1873849",
                    "display_name": "Soil Profile Data",
                    "format": "text/csv",
                    "size": 2048,
                },
                {
                    "fqn": "essdive.catalog.dataset.raw_files.instrument_a_001.csv",
                    "name": "instrument_a_001.csv",
                    "description": "Instrument A raw export file",
                    "type": "artifact",
                    "location": "s3://essdive-bucket/dataset/raw_files/instrument_a_001.csv",
                    "parent_fqn": "essdive.catalog.dataset.raw_files",
                    "display_name": "Instrument A Export",
                    "format": "text/csv",
                    "size": 84512,
                },
            ]
        },
    )


class ArtifactCollection(_EntityCommon):
    """
    Represents a directory-like structural node for organizing artifacts.

    ArtifactCollection entities can contain child ArtifactCollection entities
    and child Artifact entities.
    """

    name: Annotated[
        str,
        StringConstraints(
            strip_whitespace=True,
            min_length=1,
            pattern=r"^(?:[^./\x00]|(?:\.[^./\x00]|[^.\x00/][^/\x00]|[^/\x00][^.\x00/])|[^/\x00]{3,})$",
        ),
    ] = Field(
        ...,
        description="Collection name as a single POSIX path segment.",
    )
    description: str | None = Field(
        default=None, description="Short description of the collection."
    )  # type: ignore

    location: AnyUrl | None = Field(
        default=None,
        description="Optional location context for the collection.",
    )
    parent_fqn: str | None = Field(
        default=None,
        description=(
            "Optional parent FQN used to establish hierarchy. When omitted, write operations "
            "default it to the catalog container for the supplied catalog_fqn. When provided, it "
            "must resolve to exactly one of: a ScientificWork, an ArtifactCollection, or the "
            "catalog container itself in the same data catalog. Artifact parents and all other "
            "entity types are invalid. Must reference an existing entity that already has an "
            "assigned FQN. Invalid parent references or unsupported parent types will fail "
            "relationship resolution in downstream catalog operations."
        ),
        validation_alias=AliasChoices("parent_fqn", "parentFqn"),
        serialization_alias="parentFqn",
    )

    @field_validator("parent_fqn")
    @classmethod
    def validate_parent_fqn(cls, v):
        """Validate that parent_fqn is either omitted or a non-empty string."""
        return _validate_optional_parent_fqn(v, cls.__name__)

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: EntityTypeEnum | str) -> EntityTypeEnum:
        """Validate that type is 'artifactCollection'."""
        if isinstance(v, EntityTypeEnum):
            enum_val = v
        elif isinstance(v, str):
            try:
                enum_val = EntityTypeEnum(v)
            except ValueError as ve:
                raise ValueError(
                    f"Invalid type: {v}. Must be 'artifactCollection' for ArtifactCollection entities."
                ) from ve
        else:
            raise TypeError("type must be an EntityTypeEnum or string")

        if enum_val is not EntityTypeEnum.ARTIFACT_COLLECTION:
            enum_label = f"{enum_val.__class__.__name__}.{enum_val.name}"
            raise ValueError(
                f"Invalid type: {enum_label}. Must be 'artifactCollection' for ArtifactCollection entities."
            )
        return enum_val

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        return _validate_entity_name_without_commas(v)

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "fqn": "essdive.catalog.dataset.raw_files",
                    "name": "raw_files",
                    "description": "Raw instrument outputs",
                    "type": "artifactCollection",
                    "location": "s3://bucket/path/raw_files/",
                    "parent_fqn": "essdive.catalog.dataset",
                    "display_name": "Raw Files",
                }
            ]
        },
    )


class TableColumn(BaseModel):
    """Column definition for a Table entity."""

    name: str = Field(..., description="Column identifier")
    display_name: str = Field(
        ...,
        description="Human-readable column name",
        validation_alias=AliasChoices("display_name", "displayName"),
        serialization_alias="displayName",
    )
    data_type: str = Field(
        ...,
        description="Column data type (for example: STRING, DOUBLE, INT, BOOLEAN)",
        validation_alias=AliasChoices("data_type", "dataType"),
        serialization_alias="dataType",
    )
    description: str | None = Field(default=None, description="Column description")

    @field_validator("name", "display_name", "data_type")
    @classmethod
    def validate_required_strings(cls, v: str) -> str:
        """Ensure required string fields are non-empty."""
        if not isinstance(v, str) or not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only.")
        return v.strip()

    model_config = ConfigDict(populate_by_name=True)


class Table(_EntityCommon):
    """Represents a tabular data entity (read-only in v0.2)."""

    description: str | None = Field(default=None, description="Table description")  # type: ignore
    # make description optional for Table

    columns: list[TableColumn] = Field(
        ..., description="List of table column definitions."
    )
    table_type: str | None = Field(
        default=None,
        description="Storage format/type of table (for example: Iceberg, Parquet, CSV, Delta).",
        validation_alias=AliasChoices("table_type", "tableType"),
        serialization_alias="tableType",
    )

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: EntityTypeEnum | str) -> EntityTypeEnum:
        """Validate that type is 'table'."""
        if isinstance(v, EntityTypeEnum):
            enum_val = v
        elif isinstance(v, str):
            try:
                enum_val = EntityTypeEnum(v)
            except ValueError as ve:
                raise ValueError(
                    f"Invalid type: {v}. Must be 'table' for Table entities."
                ) from ve
        else:
            raise TypeError("type must be an EntityTypeEnum or string")

        if enum_val is not EntityTypeEnum.TABLE:
            raise ValueError(
                f"Invalid type: {enum_val}. Must be 'table' for Table entities."
            )
        return enum_val

    @field_validator("columns")
    @classmethod
    def validate_columns(cls, v: list[TableColumn]) -> list[TableColumn]:
        """Ensure tables always define at least one column."""
        if not isinstance(v, list) or len(v) == 0:
            raise ValueError("columns must contain at least one column definition.")
        return v

    @field_validator("table_type")
    @classmethod
    def validate_table_type(cls, v: str | None) -> str | None:
        """Normalize table_type when provided."""
        if v is None:
            return None
        if not isinstance(v, str):
            raise ValueError("table_type must be a string when provided.")
        trimmed = v.strip()
        return trimmed or None

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "fqn": "AmSC lakehouse.240966490975.edx_catalog_resources.2019_08_15_li_vent_csv",
                    "name": "2019_08_15_li_vent_csv",
                    "displayName": "2019_08_15_li_vent_csv",
                    "description": "The database for all tabular EDX Catalog Resources",
                    "type": "table",
                    "location": "https://us-east-1.console.aws.amazon.com/glue/home?region=us-east-1#/v2/data-catalog/tables/view/2019_08_15_li_vent_csv?database=edx_catalog_resources&catalogId=240966490975&versionId=latest",
                    "tableType": "Iceberg",
                    "columns": [
                        {
                            "name": "local_dt",
                            "displayName": "local_dt",
                            "dataType": "STRING",
                            "description": "Local datetime of measurement",
                        }
                    ],
                }
            ]
        },
    )


class MLHyperparameter(BaseModel):
    """Hyperparameter used for training an ML model."""

    name: str = Field(..., description="Hyperparameter name", examples=["batch_size"])
    value: str = Field(..., description="Hyperparameter value", examples=["64"])
    description: str | None = Field(
        default=None,
        description="Optional hyperparameter description",
        examples=["Mini-batch size"],
    )

    @field_validator("name", "value")
    @classmethod
    def validate_non_empty(cls, v: str) -> str:
        """Ensure required string fields are not empty."""
        if not isinstance(v, str) or not v.strip():
            raise ValueError("Field cannot be empty or whitespace-only.")
        return v.strip()


class MLModel(_EntityCommon):
    """
    Represents a machine learning model tracked in an ML model registry service (read-only in v0.2).
    """

    service_type: str = Field(
        ...,
        description="ML platform type/service identifier (maps to OpenMetadata serviceType).",
        examples=["Mlflow", "ClearML"],
        validation_alias=AliasChoices("service_type", "serviceType"),
        serialization_alias="serviceType",
    )
    algorithm: str | None = Field(
        default=None,
        description="Training algorithm used for the model (maps to OpenMetadata algorithm).",
        examples=["Random Forest", "LSTM", "XGBoost"],
    )
    hyperparameters: list[MLHyperparameter] | None = Field(
        default=None,
        description="Model training hyperparameters (maps to OpenMetadata mlHyperParameters).",
        validation_alias=AliasChoices(
            "hyperparameters", "hyperParameters", "mlHyperParameters"
        ),
        serialization_alias="hyperParameters",
    )

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: EntityTypeEnum | str) -> EntityTypeEnum:
        """Validate that type is 'mlModel'."""
        if isinstance(v, EntityTypeEnum):
            enum_val = v
        elif isinstance(v, str):
            try:
                enum_val = EntityTypeEnum(v)
            except ValueError as ve:
                raise ValueError(
                    f"Invalid type: {v}. Must be 'mlModel' for MLModel entities."
                ) from ve
        else:
            raise TypeError("type must be an EntityTypeEnum or string")

        if enum_val is not EntityTypeEnum.ML_MODEL:
            raise ValueError(
                f"Invalid type: {enum_val}. Must be 'mlModel' for MLModel entities."
            )
        return enum_val

    @field_validator("service_type")
    @classmethod
    def validate_service_type(cls, v: str) -> str:
        """Validate ML model service type."""
        if not isinstance(v, str) or not v.strip():
            raise ValueError("service_type is required and cannot be empty.")
        return v.strip()

    @field_validator("algorithm")
    @classmethod
    def validate_algorithm(cls, v: str | None) -> str | None:
        """Normalize algorithm when provided."""
        if v is None:
            return v
        if not isinstance(v, str):
            raise ValueError("algorithm must be a string when provided.")
        trimmed = v.strip()
        return trimmed or None

    @field_validator("hyperparameters")
    @classmethod
    def validate_hyperparameters(
        cls, v: list[MLHyperparameter] | None
    ) -> list[MLHyperparameter] | None:
        """Ensure hyperparameters, when provided, are MLHyperparameter instances."""
        if v is None:
            return None
        if not isinstance(v, list):
            raise ValueError(
                "hyperparameters must be a list of MLHyperparameter objects."
            )
        for item in v:
            if not isinstance(item, MLHyperparameter):
                raise ValueError(
                    "All hyperparameters must be MLHyperparameter instances."
                )
        return v

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "fqn": "mlflow_service.my_fraud_model",
                    "name": "my_fraud_model",
                    "description": "Fraud detection model version 3",
                    "type": "mlModel",
                    "location": "https://mlflow.example.org/#/models/my_fraud_model",
                    "display_name": "Fraud Model v3",
                    "service_type": "Mlflow",
                    "algorithm": "XGBoost",
                    "hyperparameters": [
                        {"name": "batch_size", "value": "64", "description": None},
                        {
                            "name": "learning_rate",
                            "value": "0.001",
                            "description": None,
                        },
                    ],
                }
            ]
        },
    )


class CatalogModel(BaseModel):
    """
    Catalog container - top-level container representing the AmSC catalog.

    This is the parent of all entities (datasets, studies, biosamples, etc.).
    """

    # Core descriptive fields
    name: str = Field(..., description="Unique name of the catalog container")
    domain: str = Field(
        ...,
        description="Domain which this catalog belongs to. "
        "Domains group related data assets, assign responsibilities, "
        "and support distributed data ownership",
    )
    description: str | None = Field(default=None, description="Container description")
    url: HttpUrl | None = Field(default=None, description="Landing page or access URL")

    # Service-level metadata
    display_name: str | None = Field(
        default=None,
        description="Human-friendly display name",
        validation_alias=AliasChoices("display_name", "displayName"),
        serialization_alias="displayName",
    )
    service_type: str = Field(
        default="CustomStorage",
        description="OpenMetadata service type",
        validation_alias=AliasChoices("service_type", "serviceType"),
        serialization_alias="serviceType",
    )
    parent_fqn: str | None = Field(
        default=None,
        description="Always None for catalog containers",
        validation_alias=AliasChoices("parent_fqn", "parentFqn"),
        serialization_alias="parentFqn",
    )

    @property
    def fqn(self) -> str | None:
        """
        Compute FQN dynamically based on hierarchy.

        FQN format: parent_fqn.name (if parent exists) or just name (for catalog).
        """
        if self.parent_fqn:
            return f"{self.parent_fqn}.{self.name}"
        return self.name

    model_config = ConfigDict(
        use_enum_values=True,
        populate_by_name=True,
        json_schema_extra={
            "example": {
                "name": "essdive",
                "domain": "BER",
                "display_name": "ESS-DIVE Data Catalog",
                "description": "Environmental System Science Data Infrastructure for Advancing Earth Science",
                "service_type": "CustomStorage",
            }
        },
    )
