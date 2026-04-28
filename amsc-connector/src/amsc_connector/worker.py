"""FastStream worker that consumes tiled webhook events from Redis Streams."""

import logging
import os
import uuid

from faststream import Header, FastStream, Logger
from faststream.redis import RedisBroker, StreamSub
from tiled.server.schemas import (
    ContainerChildCreatedEvent,
    ContainerChildMetadataUpdatedEvent,
    EventType,
    StreamClosedEvent,
)

from amsc_connector.core.config import get_settings
from amsc_connector.core.constants import EVENT_TYPE_TO_STREAM, HEADER_EVENT_ID

logging.basicConfig(level=logging.INFO)

CONSUMER_GROUP = "amsc-connector"
CONSUMER_NAME = os.getenv("WORKER_ID", f"worker-{uuid.uuid4().hex[:8]}")
EventIdDep = Header(HEADER_EVENT_ID)

broker = RedisBroker(get_settings().redis_dsn)
app = FastStream(broker)


@broker.subscriber(
    stream=StreamSub(
        EVENT_TYPE_TO_STREAM[EventType.container_child_created],
        group=CONSUMER_GROUP,
        consumer=CONSUMER_NAME,
    ),
)
async def on_child_created(
    msg: ContainerChildCreatedEvent, logger: Logger, event_id: str = EventIdDep
) -> None:
    logger.info("Received child_created event id=%s: %s", event_id, msg)


@broker.subscriber(
    stream=StreamSub(
        EVENT_TYPE_TO_STREAM[EventType.container_child_metadata_updated],
        group=CONSUMER_GROUP,
        consumer=CONSUMER_NAME,
    ),
)
async def on_metadata_updated(
    msg: ContainerChildMetadataUpdatedEvent, logger: Logger, event_id: str = EventIdDep
) -> None:
    logger.info("Received metadata_updated event id=%s: %s", event_id, msg)


@broker.subscriber(
    stream=StreamSub(
        EVENT_TYPE_TO_STREAM[EventType.stream_closed],
        group=CONSUMER_GROUP,
        consumer=CONSUMER_NAME,
    ),
)
async def on_stream_closed(
    msg: StreamClosedEvent, logger: Logger, event_id: str = EventIdDep
) -> None:
    logger.info("Received stream_closed event id=%s: %s", event_id, msg)
