import hashlib
import hmac
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request
from faststream.redis import RedisBroker

from amsc_connector.core.broker import get_stream_router
from amsc_connector.core.config import Settings, get_settings


def _get_broker() -> RedisBroker:
    return get_stream_router().broker


BrokerDep = Annotated[RedisBroker, Depends(_get_broker)]


def _verify_signature(body: bytes, secret: str, signature: str) -> bool:
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


async def _check_signature(
    request: Request,
    x_tiled_signature: Annotated[str | None, Header()] = None,
    settings: Settings = Depends(get_settings),
) -> None:
    """Dependency: validate HMAC signature on the raw request body."""
    body = await request.body()

    if settings.webhook_secret is not None:
        if x_tiled_signature is None:
            raise HTTPException(
                status_code=401, detail="Missing X-Tiled-Signature header"
            )
        if not _verify_signature(body, settings.webhook_secret, x_tiled_signature):
            raise HTTPException(status_code=401, detail="Invalid signature")


CheckSignature = Depends(_check_signature)
