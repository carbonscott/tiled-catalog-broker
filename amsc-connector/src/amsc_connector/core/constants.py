from tiled.server.schemas import EventType

STREAM_PREFIX = "amsc:events"
STREAM_DLQ = f"{STREAM_PREFIX}:dlq"
SEEN_KEY_PREFIX = "amsc:seen"
SEEN_TTL_SECONDS = 3600
HEADER_EVENT_ID = "x-tiled-event-id"

EVENT_TYPE_TO_STREAM: dict[EventType, str] = {
    e: f"{STREAM_PREFIX}:{e.value}" for e in EventType
}
