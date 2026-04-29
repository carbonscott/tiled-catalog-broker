import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Header
from amsc_connector.api.deps import _event_adapter, check_signature

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/webhook/event")
async def receive_webhook_event(
    x_tiled_event_id: Annotated[str | None, Header()] = None,
    body: Annotated[bytes, Depends(check_signature)] = b"",
) -> dict:
    """Receive and validate a webhook event posted by tiled."""
    event = _event_adapter.validate_json(body)

    logger.info(f"Received webhook event_id={x_tiled_event_id} event={event}")

    # TODO: implement event handling
    return {"status": "ok"}
