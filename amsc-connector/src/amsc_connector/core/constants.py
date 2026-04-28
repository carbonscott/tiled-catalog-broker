from tiled.server.schemas import EventType

STREAM_PREFIX = "amsc:events"
STREAM_SYNC = f"{STREAM_PREFIX}:sync"
STREAM_CLOSED = f"{STREAM_PREFIX}:stream_closed"
STREAM_DLQ = f"{STREAM_PREFIX}:dlq"
SEEN_KEY_PREFIX = "amsc:seen"
SEEN_TTL_SECONDS = 3600
HEADER_EVENT_ID = "x-tiled-event-id"

EVENT_TYPE_TO_STREAM: dict[EventType, str] = {
    EventType.container_child_created: STREAM_SYNC,
    EventType.container_child_metadata_updated: STREAM_SYNC,
    EventType.stream_closed: STREAM_CLOSED,
}
