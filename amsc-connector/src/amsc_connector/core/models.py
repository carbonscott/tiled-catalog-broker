"""Current public schema module.

TODO: THIS IS A TEMPORARY FIX UNTIL CLIENT WORKS
"""

from enum import StrEnum
from typing import Generic, Literal, TypeVar

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


_ET = TypeVar("_ET", bound=DLQErrorType)


class BaseDLQHeaders(BaseModel, Generic[_ET]):
    """Base headers attached to all DLQ messages."""

    model_config = ConfigDict(validate_by_name=True, validate_by_alias=True)

    x_event_id: str = Field(alias="x-tiled-event-id")
    x_error: str = Field(alias="x-error")
    x_error_type: _ET = Field(alias="x-error-type")


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
    "ScientificWork",
    "SyncMessage",
    "Table",
    "TableColumn",
    "Tag",
    "TagRef",
    "TiledFetchDLQHeaders",
]
