"""FastStream worker that consumes tiled sync triggers from Redis Streams."""

import asyncio
import contextlib
import json
import logging
import os
import time
import uuid

import httpx
import redis.asyncio as aioredis
import stamina
import tiled.client as tc
from faststream import Context, ExceptionMiddleware, FastStream, Header, Logger
from faststream.redis import RedisBroker, RedisStreamMessage, StreamSub
from pydantic import BaseModel, computed_field
from redis.commands.core import AsyncScript
from tiled.client.utils import ClientError
from tiled.structures.core import StructureFamily

from amsc_connector.core.config import get_settings
from amsc_connector.core.constants import (
    HEADER_EVENT_ID,
    HEADER_FIRST_FAILED_AT,
    HEADER_RETRY_COUNT,
    RETRY_ZSET,
    STREAM_CLOSED,
    STREAM_DLQ,
    STREAM_SYNC,
)
from amsc_connector.core.exceptions import (
    EntityRegistrationAuthError,
    EntityRegistrationError,
    TiledFetchError,
)
from amsc_connector.core.models import (
    Artifact,
    ArtifactCollection,
    RegistrationDLQHeaders,
    RetryErrorType,
    RetryHeaders,
    RetryPayload,
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


@exc_middleware.add_handler(EntityRegistrationAuthError)
async def handle_registration_auth_error(
    exc: EntityRegistrationAuthError,
    message: RedisStreamMessage,
    logger: Logger,
    redis_client: aioredis.Redis = Context(),  # noqa: B008
    event_id: str = EventIdDep,
    retry_count_raw: str = Header(HEADER_RETRY_COUNT, default="0"),
    first_failed_at_raw: str = Header(HEADER_FIRST_FAILED_AT, default=""),
) -> None:
    """Schedule a delayed retry for 401 auth failures via the retry ZSET."""
    logger.exception(
        f"Auth failure registering location={exc.location} event_id={event_id}"
    )
    sync_msg = SyncMessage.model_validate_json(message.body)
    retry_count = int(retry_count_raw) + 1
    payload = RetryPayload(
        path=sync_msg.path,
        event_id=event_id,
        retry_count=retry_count,
        first_failed_at=float(first_failed_at_raw or time.time()),
        last_error_type=RetryErrorType.AUTH_FAILED,
        entity_type=exc.entity_type,
    )
    score = time.time() + settings.dlq_retry_delay_seconds
    await redis_client.zadd(RETRY_ZSET, {payload.model_dump_json(): score})

    if retry_count >= settings.dlq_retry_alert_threshold:
        logger.error(
            "401_RETRY_THRESHOLD_EXCEEDED event_id=%s retry_count=%d "
            "entity_type=%s — possible permanent auth failure",
            event_id,
            retry_count,
            exc.entity_type,
        )

    logger.info(
        "Scheduled 401 retry in %ds event_id=%s retry_count=%d entity_type=%s",
        settings.dlq_retry_delay_seconds,
        event_id,
        retry_count,
        exc.entity_type,
    )


@exc_middleware.add_handler(EntityRegistrationError)
async def handle_registration_error(
    exc: EntityRegistrationError,
    message: RedisStreamMessage,
    logger: Logger,
    event_id: str = EventIdDep,
) -> None:
    """Publish non-auth registration failures to the DLQ stream."""
    logger.exception(f"Failed to register location={exc.location} event_id={event_id}")
    headers = RegistrationDLQHeaders(x_event_id=event_id, x_error=exc.detail[:500])
    await broker.publish(
        message.body,
        stream=STREAM_DLQ,
        headers=headers.model_dump(by_alias=True),
    )
    logger.info(f"Published to DLQ: event_id={event_id} location={exc.location}")


@exc_middleware.add_handler(TiledFetchError)
async def handle_tiled_fetch_error(
    exc: TiledFetchError,
    message: RedisStreamMessage,
    logger: Logger,
    event_id: str = Header(HEADER_EVENT_ID, default="unknown"),
) -> None:
    """Log and publish failed Tiled fetch messages to the DLQ."""
    body = message.body
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

    # Own redis.asyncio client for ZSET retry operations
    redis_client = aioredis.from_url(settings.redis_dsn)
    app.context.set_global("redis_client", redis_client)


# Lua script for atomic score-checked pop from the retry ZSET.
# Returns and removes only members with score <= the given threshold.
# Note "score" here refer to the time when the retry should be attempted
_RETRY_POP_LUA = """
local result = redis.call(
    'ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2]
)
if #result > 0 then redis.call('ZREM', KEYS[1], unpack(result)) end
return result
"""


@app.after_startup
async def _start_retry_scheduler(
    redis_client: aioredis.Redis = Context(),  # noqa: B008
) -> None:
    """Launch the background retry scheduler as an asyncio task."""
    retry_pop_script: AsyncScript = redis_client.register_script(_RETRY_POP_LUA)  # pyrefly: ignore[not-async]
    task = asyncio.create_task(_retry_scheduler_loop(retry_pop_script))
    app.context.set_global("_retry_scheduler_task", task)


async def _drain_ready_retries(
    retry_pop_script: AsyncScript, logger: logging.Logger
) -> None:
    """Pop all due items from the retry ZSET and re-publish them."""
    now = str(time.time())
    batch_size = str(settings.retry_scheduler_batch_size)
    while results := await retry_pop_script(keys=[RETRY_ZSET], args=[now, batch_size]):
        for raw in results:
            payload = RetryPayload.model_validate_json(raw)
            headers = RetryHeaders(
                x_tiled_event_id=payload.event_id,
                x_retry_count=str(payload.retry_count),
                x_first_failed_at=str(payload.first_failed_at),
            )
            await broker.publish(
                SyncMessage(path=payload.path).model_dump(),
                stream=STREAM_SYNC,
                headers=headers.model_dump(by_alias=True),
            )
            logger.info(
                "Dispatching retry event_id=%s retry_count=%d",
                payload.event_id,
                payload.retry_count,
            )


async def _retry_scheduler_loop(retry_pop_script: AsyncScript) -> None:
    """Poll the retry ZSET and re-dispatch ready items to the sync stream."""
    logger = logging.getLogger("amsc_connector.retry_scheduler")
    logger.info(
        "Retry scheduler started (poll_interval=%ds, batch_size=%d)",
        settings.retry_scheduler_poll_interval_seconds,
        settings.retry_scheduler_batch_size,
    )
    try:
        while True:
            try:
                await _drain_ready_retries(retry_pop_script, logger)
            except Exception:
                logger.exception("Error in retry scheduler loop")
            await asyncio.sleep(settings.retry_scheduler_poll_interval_seconds)
    except asyncio.CancelledError:
        logger.info("Retry scheduler stopped")


@app.on_shutdown
async def _shutdown(
    amsc_client: httpx.AsyncClient = Context(),  # noqa: B008
    tiled_root: tc.container.Container = Context(),  # noqa: B008
) -> None:
    # Cancel the retry scheduler
    task: asyncio.Task | None = app.context.get("_retry_scheduler_task")
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        app.context.reset_global("_retry_scheduler_task")

    # Close the dedicated Redis client
    redis_client: aioredis.Redis | None = app.context.get("redis_client")
    if redis_client is not None:
        await redis_client.aclose()
        app.context.reset_global("redis_client")

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

    if resp.status_code == 401:
        raise EntityRegistrationAuthError(
            resp.text[:300],
            entity_type=entity_type,
            catalog_name=catalog_name,
            location=location,
        )

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
