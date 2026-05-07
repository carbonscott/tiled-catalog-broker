"""FastStream worker that consumes tiled sync triggers from Redis Streams."""

import asyncio
import contextlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass

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
    EntityRegistrationParentMissingError,
    RetryableEntityRegistrationError,
    TiledFetchError,
)
from amsc_connector.core.models import (
    Artifact,
    ArtifactCollection,
    PendingEntry,
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
RECOVERY_CONSUMER_NAME = f"{CONSUMER_NAME}-r"
RECOVERY_MIN_IDLE_MS = 30_000
EventIdDep = Header(HEADER_EVENT_ID)

settings = get_settings()

exc_middleware = ExceptionMiddleware()


@dataclass(frozen=True)
class RegistrationRetryPolicy:
    error_type: RetryErrorType
    delay_seconds: int
    alert_threshold: int


REGISTRATION_RETRY_POLICIES: dict[
    type[RetryableEntityRegistrationError], RegistrationRetryPolicy
] = {
    EntityRegistrationAuthError: RegistrationRetryPolicy(
        error_type=RetryErrorType.AUTH_FAILED,
        delay_seconds=settings.auth_retry_delay_seconds,
        alert_threshold=settings.auth_retry_alert_threshold,
    ),
    EntityRegistrationParentMissingError: RegistrationRetryPolicy(
        error_type=RetryErrorType.MISSING_PARENT_ENTITY,
        delay_seconds=settings.missing_parent_retry_delay_seconds,
        alert_threshold=settings.missing_parent_retry_alert_threshold,
    ),
}

UNKNOWN_REGISTRATION_RETRY_POLICY = RegistrationRetryPolicy(
    error_type=RetryErrorType.UNKNOWN,
    delay_seconds=settings.auth_retry_delay_seconds,
    alert_threshold=settings.auth_retry_alert_threshold,
)


@exc_middleware.add_handler(RetryableEntityRegistrationError)
async def handle_retryable_registration_error(
    exc: RetryableEntityRegistrationError,
    message: RedisStreamMessage,
    logger: Logger,
    redis_client: aioredis.Redis = Context(),  # noqa: B008
    event_id: str = EventIdDep,
    retry_count_raw: str = Header(HEADER_RETRY_COUNT, default="0"),
    first_failed_at_raw: str = Header(HEADER_FIRST_FAILED_AT, default=""),
) -> None:
    """Schedule a delayed retry for retryable registration failures."""
    policy = REGISTRATION_RETRY_POLICIES.get(
        type(exc), UNKNOWN_REGISTRATION_RETRY_POLICY
    )
    error_type = policy.error_type
    logger.exception(
        "Retryable registration failure error_type=%s location=%s event_id=%s",
        error_type,
        exc.location,
        event_id,
    )

    sync_msg = SyncMessage.model_validate_json(message.body)
    retry_count = int(retry_count_raw) + 1
    payload = RetryPayload(
        path=sync_msg.path,
        event_id=event_id,
        retry_count=retry_count,
        first_failed_at=float(first_failed_at_raw or time.time()),
        last_error_type=error_type,
        entity_type=exc.entity_type,
    )
    await redis_client.zadd(
        RETRY_ZSET,
        {payload.model_dump_json(): time.time() + policy.delay_seconds},
    )

    if retry_count >= policy.alert_threshold:
        logger.error(
            "REGISTRATION_RETRY_THRESHOLD_EXCEEDED event_id=%s retry_count=%d "
            "entity_type=%s error_type=%s",
            event_id,
            retry_count,
            exc.entity_type,
            error_type,
        )

    logger.info(
        "Scheduled registration retry in %ds event_id=%s retry_count=%d "
        "entity_type=%s error_type=%s",
        policy.delay_seconds,
        event_id,
        retry_count,
        exc.entity_type,
        error_type,
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


async def _cleanup_consumer(
    redis_client: aioredis.Redis,
    stream: str,
    consumer: str,
) -> None:
    try:
        # Re-inject any pending (unacked) messages as new stream entries so
        # another worker picks them up immediately rather than waiting for
        # XAUTOCLAIM's idle window.
        while raw := await redis_client.xpending_range(
            stream,
            CONSUMER_GROUP,
            min="-",
            max="+",
            count=100,
            consumername=consumer,
        ):
            for entry in (PendingEntry.model_validate(p) for p in raw):
                msg_id = entry.message_id
                msgs = await redis_client.xrange(
                    stream, min=msg_id, max=msg_id, count=1
                )
                if msgs:
                    _, fields = msgs[0]
                    # not atomic with xack below — crash here leaves msg in both
                    # stream and PEL; autoclaim picks it up after 30s (idempotent)
                    await redis_client.xadd(stream, fields)
                # another pod's autoclaim can steal this msg between xpending_range
                # and here; we'd xadd duplicate + ack their copy (idempotent,
                # guarded by min_idle_time=30s)
                await redis_client.xack(stream, CONSUMER_GROUP, msg_id)
                logging.warning(
                    "Re-queued pending msg %s consumer=%s stream=%s at shutdown",
                    msg_id,
                    consumer,
                    stream,
                )
        await redis_client.xgroup_delconsumer(stream, CONSUMER_GROUP, consumer)
        logging.info("Deleted consumer %s from stream %s", consumer, stream)
    except Exception:
        logging.exception(
            "Failed to clean up consumer %s from stream %s", consumer, stream
        )


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

    # Delete this worker's consumers from the group on graceful shutdown.
    # Skip any consumer that still has pending entries — XAUTOCLAIM on another
    # pod will recover them.  On a crash (not graceful shutdown) this code never
    # runs, which is fine for the same reason.
    redis_client: aioredis.Redis | None = app.context.get("redis_client")
    if redis_client is not None:
        await asyncio.gather(
            *(
                _cleanup_consumer(redis_client, stream, consumer)
                for stream in (STREAM_SYNC, STREAM_CLOSED)
                for consumer in (CONSUMER_NAME, RECOVERY_CONSUMER_NAME)
            )
        )
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


def _get_error_detail(resp: httpx.Response) -> str:
    """Extract error detail from response JSON, falling back to raw text."""
    try:
        payload = resp.json()
        if isinstance(payload, dict) and "detail" in payload:
            return str(payload["detail"])
    except ValueError:
        pass
    return resp.text.strip() or resp.reason_phrase


def _raise_for_error(
    resp: httpx.Response,
    *,
    entity_type: str,
    catalog_name: str,
    location: str,
) -> None:
    """Raise the appropriate exception for an error response."""
    detail = _get_error_detail(resp)

    # 401 Unauthorized
    if resp.status_code == 401:
        raise EntityRegistrationAuthError(
            detail[:300],
            entity_type=entity_type,
            catalog_name=catalog_name,
            location=location,
        )

    # 404 Parent Missing
    if (
        resp.status_code == 404
        and "Parent ScientificWork not found" in detail
        and entity_type != "scientificWork"
    ):
        raise EntityRegistrationParentMissingError(
            detail[:300],
            entity_type=entity_type,
            catalog_name=catalog_name,
            location=location,
        )

    # Generic Fallback
    raise EntityRegistrationError(
        detail[:300],
        status_code=resp.status_code,
        entity_type=entity_type,
        catalog_name=catalog_name,
        location=location,
    )


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

    # Success on POST
    if not resp.is_error:
        logging.info("Created %s: %s", entity_type, fqn)
        return

    # Check for conflict
    detail = _get_error_detail(resp)
    if resp.status_code in (400, 409) and "already exists" in detail:
        resp = await _put_entity(fqn, body, client, entity_type, catalog_name)
        if not resp.is_error:
            logging.info("Updated %s: %s", entity_type, fqn)
            return

    # If PUT failed, or not "Already Exists" error, handle
    _raise_for_error(
        resp,
        entity_type=entity_type,
        catalog_name=catalog_name,
        location=location,
    )


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


async def _do_sync(
    msg: SyncMessage,
    logger: Logger,
    amsc_client: httpx.AsyncClient,
    tiled_root: tc.container.Container,
    event_id: str,
) -> None:
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

    if settings.amsc_dry_run:
        body = entity.model_dump(by_alias=True, exclude_none=True, mode="json")
        logger.info(
            "DRY_RUN would_register entity_type=%s fqn=%s location=%s body=%s",
            entity.type,
            fqn,
            ctx.location,
            body,
        )
        return

    await _create_or_update(
        settings.openmetadata_fqn_prefix,
        fqn,
        entity,
        amsc_client,
    )


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
    await _do_sync(msg, logger, amsc_client, tiled_root, event_id)


@broker.subscriber(
    stream=StreamSub(
        STREAM_SYNC,
        group=CONSUMER_GROUP,
        consumer=RECOVERY_CONSUMER_NAME,
        min_idle_time=RECOVERY_MIN_IDLE_MS,
    ),
)
async def on_sync_recovery(
    msg: SyncMessage,
    logger: Logger,
    amsc_client: httpx.AsyncClient = Context(),  # noqa: B008
    tiled_root: tc.container.Container = Context(),  # noqa: B008
    event_id: str = EventIdDep,
) -> None:
    """Reclaim and reprocess sync messages abandoned in the PEL by crashed consumers."""
    await _do_sync(msg, logger, amsc_client, tiled_root, event_id)


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


@broker.subscriber(
    stream=StreamSub(
        STREAM_CLOSED,
        group=CONSUMER_GROUP,
        consumer=RECOVERY_CONSUMER_NAME,
        min_idle_time=RECOVERY_MIN_IDLE_MS,
    ),
)
async def on_stream_closed_recovery(
    msg: SyncMessage, logger: Logger, event_id: str = EventIdDep
) -> None:
    logger.info(
        "Received stream_closed event id=%s path=%s", event_id, "/".join(msg.path)
    )
