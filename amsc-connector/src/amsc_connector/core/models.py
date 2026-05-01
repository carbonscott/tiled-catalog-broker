"""Current public schema module.

TODO: THIS IS A TEMPORARY FIX UNTIL CLIENT WORKS
"""

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ._models import (
    Artifact,
    ArtifactCollection,
    Classification,
    EntityTypeEnum,
    MLHyperparameter,
    MLModel,
    ScientificWork,
    Table,
    TableColumn,
    Tag,
    TagRef,
)


class SyncMessage(BaseModel):
    """Minimal message published to the sync stream.

    Contains only the path of the node that changed.  The worker fetches
    current state from Tiled on receipt so ordering and duplicate messages
    are both harmless.
    """

    path: list[str]


class DLQErrorType(StrEnum):
    REGISTRATION = "registration"
    TILED_FETCH = "tiled_fetch"


class BaseDLQHeaders[ET: DLQErrorType](BaseModel):
    """Base headers attached to all DLQ messages."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    x_event_id: str = Field(alias="x-tiled-event-id")
    x_error: str = Field(alias="x-error")
    x_error_type: ET = Field(alias="x-error-type")


class RegistrationDLQHeaders(BaseDLQHeaders[Literal[DLQErrorType.REGISTRATION]]):
    """Headers attached to DLQ messages from registration failures."""

    x_error_type: Literal[DLQErrorType.REGISTRATION] = Field(
        default=DLQErrorType.REGISTRATION, alias="x-error-type"
    )


class TiledFetchDLQHeaders(BaseDLQHeaders[Literal[DLQErrorType.TILED_FETCH]]):
    """Headers attached to DLQ messages from Tiled fetch failures."""

    x_error_type: Literal[DLQErrorType.TILED_FETCH] = Field(
        default=DLQErrorType.TILED_FETCH, alias="x-error-type"
    )


class RetryHeaders(BaseModel):
    """Headers attached to retry messages re-published to the sync stream."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    x_tiled_event_id: str = Field(alias="x-tiled-event-id")
    x_retry_count: str = Field(alias="x-retry-count")
    x_first_failed_at: str = Field(alias="x-first-failed-at")


class RetryErrorType(StrEnum):
    AUTH_FAILED = "auth_failed"
    UNKNOWN = "unknown"


class RetryPayload(BaseModel):
    """Payload serialized into the retry ZSET for delayed re-processing."""

    path: list[str]
    event_id: str
    retry_count: int = 0
    first_failed_at: float
    last_error_type: RetryErrorType
    entity_type: str | None = None


__all__ = [
    "Artifact",
    "ArtifactCollection",
    "BaseDLQHeaders",
    "Classification",
    "DLQErrorType",
    "EntityTypeEnum",
    "MLHyperparameter",
    "MLModel",
    "RegistrationDLQHeaders",
    "RetryErrorType",
    "RetryHeaders",
    "RetryPayload",
    "ScientificWork",
    "SyncMessage",
    "Table",
    "TableColumn",
    "Tag",
    "TagRef",
    "TiledFetchDLQHeaders",
]
