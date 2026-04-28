import logging
from typing import Annotated

from fastapi import APIRouter, Header
from pydantic import BaseModel
from tiled.server.schemas import WebhookEvent

from amsc_connector.api.deps import BrokerDep, CheckSignature
from amsc_connector.core.constants import (
    EVENT_TYPE_TO_STREAM,
    HEADER_EVENT_ID,
    STREAM_DLQ,
)
from amsc_connector.core.models import SyncMessage

logger = logging.getLogger(__name__)
router = APIRouter()


class WebhookResponse(BaseModel):
    status: str = "ok"


# dumb pipe, strips everything from the event except path and republish to redis
@router.post("/webhook/event", dependencies=[CheckSignature])
async def receive_webhook_event(
    event: WebhookEvent,
    broker: BrokerDep,
    x_tiled_event_id: Annotated[str, Header()],
) -> WebhookResponse:
    """Receive a webhook event from tiled and publish a sync trigger to Redis."""
    stream = EVENT_TYPE_TO_STREAM.get(event.type)
    if stream is None:
        logger.warning("unknown event.type=%s; routing to DLQ", event.type)
        stream = STREAM_DLQ
    await broker.publish(
        SyncMessage(path=event.path),
        stream=stream,
        headers={HEADER_EVENT_ID: x_tiled_event_id},
    )
    return WebhookResponse()
