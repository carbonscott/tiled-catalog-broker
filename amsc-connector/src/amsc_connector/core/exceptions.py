"""Custom exceptions for the AMSC connector."""


class EntityRegistrationError(Exception):
    """Raised when an entity cannot be registered with the catalog API.

    Attributes:
        status_code: HTTP status code from the API response.
        detail: Error detail string from the API or the connection error message.
        entity_type: The entity type that failed to register.
        catalog_name: The catalog the entity was being registered to.
    """

    STATUS_CODE: int | None = None

    def __init__(
        self,
        detail: str,
        *,
        status_code: int | None = None,
        entity_type: str | None = None,
        catalog_name: str | None = None,
        location: str | None = None,
    ) -> None:
        self.status_code = status_code or self.STATUS_CODE
        self.location = location
        self.detail = detail
        self.entity_type = entity_type
        self.catalog_name = catalog_name
        super().__init__(detail)


class RetryableEntityRegistrationError(EntityRegistrationError):
    """Raised when a registration failure should be retried later."""


class EntityRegistrationAuthError(RetryableEntityRegistrationError):
    """Raised when entity registration fails with 401 Unauthorized."""

    STATUS_CODE = 401


class EntityRegistrationParentMissingError(RetryableEntityRegistrationError):
    """Raised when a child entity is registered before its parent scientific work.

    TODO: remove when they remove this requirement
    """

    STATUS_CODE = 404


class TiledFetchError(Exception):
    """Raised when a Tiled client operation fails after retries.

    Attributes:
        detail: Error description.
        path: The Tiled node path that was being accessed.
        status_code: HTTP status code if the failure was an HTTP error.
    """

    def __init__(
        self,
        detail: str,
        *,
        path: list[str] | None = None,
        status_code: int | None = None,
    ) -> None:
        self.detail = detail
        self.path = path or []
        self.status_code = status_code
        super().__init__(detail)
