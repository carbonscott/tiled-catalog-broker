import hashlib
import hmac
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request
from pydantic import TypeAdapter
from tiled.server.schemas import WebhookEvent

from amsc_connector.core.config import Settings, get_settings

_event_adapter: TypeAdapter[WebhookEvent] = TypeAdapter(WebhookEvent)


def verify_signature(body: bytes, secret: str, signature: str) -> bool:
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


async def check_signature(
    request: Request,
    x_tiled_signature: Annotated[str | None, Header()] = None,
    settings: Settings = Depends(get_settings),
) -> bytes:
    """Dependency: validate HMAC signature and return the raw request body."""
    body = await request.body()

    if settings.webhook_secret is not None:
        if x_tiled_signature is None:
            raise HTTPException(
                status_code=401, detail="Missing X-Tiled-Signature header"
            )
        if not verify_signature(body, settings.webhook_secret, x_tiled_signature):
            raise HTTPException(status_code=401, detail="Invalid signature")

    return body
