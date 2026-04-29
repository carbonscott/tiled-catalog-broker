"""FastStream worker that consumes tiled sync triggers from Redis Streams."""

import json
import logging
import os
import uuid

import httpx
import stamina
import tiled.client as tc
from faststream import Context, ExceptionMiddleware, FastStream, Header, Logger
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub
from pydantic import BaseModel, computed_field
from tiled.client.utils import ClientError
from tiled.structures.core import StructureFamily

from amsc_connector.core.config import get_settings
from amsc_connector.core.constants import (
    HEADER_EVENT_ID,
    STREAM_CLOSED,
    STREAM_DLQ,
    STREAM_SYNC,
)
from amsc_connector.core.exceptions import EntityRegistrationError, TiledFetchError
from amsc_connector.core.models import (
    Artifact,
    ArtifactCollection,
    RegistrationDLQHeaders,
    ScientificWork,
    SyncMessage,
    TiledFetchDLQHeaders,
)

Entity = ScientificWork | ArtifactCollection | Artifact

CONSUMER_GROUP = "amsc-connector"
CONSUMER_NAME = os.getenv("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")
EventIdDep = Header(HEADER_EVENT_ID)

settings = get_settings()

exc_middleware = ExceptionMiddleware()


@exc_middleware.add_handler(EntityRegistrationError)
async def handle_registration_error(
    exc: EntityRegistrationError, message: RedisStreamMessage, logger: Logger
) -> None:
    """Log and publish failed registration messages to the DLQ."""
    body = message.body
    event_id = message.headers.get(HEADER_EVENT_ID, "unknown")
    logger.exception(f"Failed to register location={exc.location} event_id={event_id}")
    headers = RegistrationDLQHeaders(x_event_id=event_id, x_error=exc.detail[:500])
    await broker.publish(
        body,
        stream=STREAM_DLQ,
        headers=headers.model_dump(by_alias=True),
    )
    logger.info(f"Published to DLQ: event_id={event_id} location={exc.location}")


@exc_middleware.add_handler(TiledFetchError)
async def handle_tiled_fetch_error(
    exc: TiledFetchError, message: RedisStreamMessage, logger: Logger
) -> None:
    """Log and publish failed Tiled fetch messages to the DLQ."""
    body = message.body
    event_id = message.headers.get(HEADER_EVENT_ID, "unknown")
    path_str = "/".join(exc.path)
    logger.exception(
        f"Tiled fetch failed path={path_str} event_id={event_id}: {exc.detail}"
    )
    headers = TiledFetchDLQHeaders(x_event_id=event_id, x_error=exc.detail[:500])
    await broker.publish(
        body,
        stream=STREAM_DLQ,
        headers=headers.model_dump(by_alias=True),
    )
    logger.info(f"Published to DLQ: event_id={event_id} path={path_str}")


broker = RedisBroker(settings.redis_dsn, middlewares=[exc_middleware])
tiled_external_url = settings.tiled_external_url.unicode_string().rstrip("/")

app = FastStream(broker)


def _should_retry_tiled(exc: Exception) -> bool:
    """Decide whether a Tiled client error is transient and worth retrying."""
    if isinstance(exc, ClientError):
        return exc.response.status_code >= 500
    return isinstance(exc, (httpx.ConnectError, httpx.TimeoutException))


@app.on_startup
async def _startup() -> None:
    base_url = settings.amsc_api_base_url.unicode_string().rstrip("/") + "/api/current/"
    app.context.set_global(
        "amsc_client",
        httpx.AsyncClient(
            base_url=base_url,
            headers={
                "Authorization": f"Bearer {settings.amsc_api_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        ),
    )

    tiled_url = settings.tiled_url.unicode_string()

    @stamina.retry(
        on=_should_retry_tiled,
        attempts=settings.tiled_retry_attempts,
        timeout=settings.tiled_retry_timeout,
        wait_initial=settings.tiled_retry_wait_initial,
        wait_max=settings.tiled_retry_wait_max,
    )
    def connect_tiled() -> tc.container.Container:
        return tc.from_uri(tiled_url, api_key=settings.tiled_api_key)

    tiled_root = connect_tiled()
    app.context.set_global("tiled_root", tiled_root)
    logging.info("Connected to Tiled at %s", tiled_url)


@app.on_shutdown
async def _shutdown(
    amsc_client: httpx.AsyncClient = Context(),  # noqa: B008
    tiled_root: tc.container.Container = Context(),  # noqa: B008
) -> None:
    await amsc_client.aclose()
    tiled_root.context.close()
    app.context.reset_global("amsc_client")
    app.context.reset_global("tiled_root")


def _expected_fqn(catalog_name: str, path: list[str]) -> str:
    """Compute the expected OMD FQN for a tiled path."""
    return ".".join([catalog_name, *path])


def _tiled_location(path: list[str]) -> str:
    """Build the external Tiled URL for a given path."""
    return f"{tiled_external_url}/{'/'.join(path)}"


# TODO: this will eventually be replaced by the AmSC client lib,
# but it doesn't work at the moment
async def _post_entity(
    catalog_name: str,
    entity_type: str,
    body: dict,
    client: httpx.AsyncClient,
) -> httpx.Response:
    """POST a new entity to the catalog. Returns the raw response."""
    try:
        return await client.post(
            f"catalog/{catalog_name}/{entity_type}",
            json=body,
        )
    except httpx.RequestError as exc:
        raise EntityRegistrationError(
            str(exc),
            entity_type=entity_type,
            catalog_name=catalog_name,
            location=body.get("location"),
        ) from exc


async def _put_entity(
    fqn: str,
    body: dict,
    client: httpx.AsyncClient,
    entity_type: str,
    catalog_name: str,
) -> httpx.Response:
    """PUT (update) an existing entity by FQN. Returns the raw response."""
    try:
        return await client.put(
            f"catalog/{fqn}",
            json=body,
        )
    except httpx.RequestError as exc:
        raise EntityRegistrationError(
            str(exc),
            entity_type=entity_type,
            catalog_name=catalog_name,
            location=body.get("location"),
        ) from exc


def _is_already_exists(resp: httpx.Response) -> bool:
    """Return True if the response indicates the entity already exists.

    TODO: remove once the API returns 409 Conflict for duplicate entities
    instead of 400 with a string-matched detail message.
    """
    try:
        return resp.status_code == 400 and "already exists" in resp.json().get(
            "detail", ""
        )
    except Exception:
        return False


async def _create_or_update(
    catalog_name: str,
    fqn: str,
    entity: Entity,
    client: httpx.AsyncClient,
) -> None:
    """Create an entity via POST; if it already exists, update it via PUT.

    Raises EntityRegistrationError on any HTTP or connection error.
    """
    entity_type = entity.type
    body = entity.model_dump(by_alias=True, exclude_none=True, mode="json")
    location = body.get("location", "unknown")

    resp = await _post_entity(catalog_name, entity_type, body, client)
    action = "Created"
    if _is_already_exists(resp):
        resp = await _put_entity(fqn, body, client, entity_type, catalog_name)
        action = "Updated"

    if resp.is_error:
        raise EntityRegistrationError(
            resp.text[:300],
            status_code=resp.status_code,
            entity_type=entity_type,
            catalog_name=catalog_name,
            location=location,
        )

    logging.info("%s %s: %s", action, entity_type, fqn)


class TiledNodeContext(BaseModel):
    path: list[str]
    structure_family: StructureFamily
    metadata: dict
    location: str
    parent_fqn: str | None
    mimetype: str | None

    @computed_field
    @property
    def key(self) -> str:
        return self.path[-1]


def _fetch_tiled_context(
    path: list[str],
    catalog_prefix: str,
    tiled_root: tc.container.Container,
) -> TiledNodeContext:
    """Fetch current node state from Tiled and return as a structured context.

    Raises KeyError if the node does not exist (e.g. was deleted).
    Raises TiledFetchError if a transient error persists after retries.
    """

    @stamina.retry(
        on=_should_retry_tiled,
        attempts=settings.tiled_retry_attempts,
        timeout=settings.tiled_retry_timeout,
        wait_initial=settings.tiled_retry_wait_initial,
        wait_max=settings.tiled_retry_wait_max,
    )
    def _fetch_node() -> tuple[tc.container.Container, StructureFamily, str | None]:
        node = tiled_root["/".join(path)]
        sf = node.structure_family
        mimetype = None
        if sf != StructureFamily.container:
            sources = node.data_sources()
            mimetype = sources[0].mimetype if sources else None
        return node, sf, mimetype

    try:
        tiled_node, structure_family, mimetype = _fetch_node()
    except KeyError:
        raise
    except (ClientError, httpx.HTTPError) as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        raise TiledFetchError(
            f"Tiled fetch failed after retries: {exc}",
            path=path,
            status_code=status_code,
        ) from exc

    return TiledNodeContext(
        path=path,
        structure_family=structure_family,
        metadata=dict(tiled_node.metadata),
        location=_tiled_location(path),
        parent_fqn=(
            None if len(path) == 1 else _expected_fqn(catalog_prefix, path[:-1])
        ),
        mimetype=mimetype,
    )


def _build_entity(
    ctx: TiledNodeContext,
) -> Entity:
    """Build the appropriate entity model from a TiledNodeContext."""
    common = {
        "name": ctx.key,
        "description": json.dumps(ctx.metadata),
        "display_name": ctx.key,
        "location": ctx.location,
        "parent_fqn": ctx.parent_fqn,
    }

    match (ctx.structure_family, len(ctx.path)):
        case (StructureFamily.container, 1):
            return ScientificWork(type="scientificWork", **common)
        case (StructureFamily.container, _):
            return ArtifactCollection(type="artifactCollection", **common)
        case _:
            return Artifact(type="artifact", format=ctx.mimetype, **common)


@broker.subscriber(
    stream=StreamSub(
        STREAM_SYNC,
        group=CONSUMER_GROUP,
        consumer=CONSUMER_NAME,
    ),
)
async def on_sync(
    msg: SyncMessage,
    logger: Logger,
    amsc_client: httpx.AsyncClient = Context(),  # noqa: B008
    tiled_root: tc.container.Container = Context(),  # noqa: B008
    event_id: str = EventIdDep,
) -> None:
    """Fetch current node state from Tiled and create-or-update in OpenMetadata."""
    node_path = msg.path

    logger.info("sync id=%s path=%s", event_id, "/".join(node_path))

    try:
        ctx = _fetch_tiled_context(
            node_path, settings.openmetadata_fqn_prefix, tiled_root
        )
    except KeyError:
        logger.error(
            "Node not found in Tiled (possibly deleted) path=%s event_id=%s — skipping",
            "/".join(node_path),
            event_id,
        )
        return

    if not ctx.metadata.get("amsc_public", False):
        logger.info(
            "Node is not marked public; skipping registration path=%s event_id=%s",
            "/".join(node_path),
            event_id,
        )
        return

    entity = _build_entity(ctx)
    fqn = _expected_fqn(settings.openmetadata_fqn_prefix, node_path)
    await _create_or_update(
        settings.openmetadata_fqn_prefix,
        fqn,
        entity,
        amsc_client,
    )


@broker.subscriber(
    stream=StreamSub(
        STREAM_CLOSED,
        group=CONSUMER_GROUP,
        consumer=CONSUMER_NAME,
    ),
)
async def on_stream_closed(
    msg: SyncMessage, logger: Logger, event_id: str = EventIdDep
) -> None:
    logger.info(
        "Received stream_closed event id=%s path=%s", event_id, "/".join(msg.path)
    )
