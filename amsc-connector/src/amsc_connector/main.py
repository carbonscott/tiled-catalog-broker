import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from tiled.server.schemas import WebhookRegistrationRequest, WebhookResponse

from amsc_connector.api import api_router
from amsc_connector.core.config import Settings, get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def register_webhook(client: httpx.AsyncClient, settings: Settings) -> None:
    """Idempotently register this connector as a webhook target with tiled.

    On startup the connector GETs the list of webhooks already registered on
    ``webhook_target_path``.  If a webhook pointing at ``webhook_external_url``
    is already active it is left untouched; otherwise a new one is created.
    """
    target_path = settings.webhook_target_path
    base_url = settings.tiled_url.unicode_string().rstrip("/")
    list_url = f"{base_url}/api/v1/webhooks/target/{target_path}"
    headers = {
        "Authorization": f"Apikey {settings.tiled_api_key}",
    }

    response = await client.get(list_url, headers=headers)
    if not response.is_success:
        logger.error(
            "Failed to list webhooks [%d]: %s",
            response.status_code,
            response.text,
        )
    response.raise_for_status()

    external_url = str(settings.webhook_external_url)
    for raw in response.json():
        existing = WebhookResponse.model_validate(raw)
        if str(existing.url) == external_url and existing.active:
            logger.info(
                "Webhook already registered (id=%d url=%s); skipping.",
                existing.id,
                external_url,
            )
            return

    body = WebhookRegistrationRequest(
        url=settings.webhook_external_url,
        secret=settings.webhook_secret,
        events=None,  # subscribe to all event types
    )
    post_headers = {**headers, "Content-Type": "application/json"}
    request_body = body.model_dump_json()
    logger.debug("Registering webhook: %s", request_body)
    response = await client.post(
        list_url,
        content=request_body,
        headers=post_headers,
    )
    if not response.is_success:
        logger.error(
            "Failed to register webhook [%d]: %s",
            response.status_code,
            response.text,
        )
    response.raise_for_status()

    created = WebhookResponse.model_validate(response.json())
    logger.info("Registered webhook id=%d url=%s", created.id, external_url)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    async with httpx.AsyncClient() as client:
        await register_webhook(client, get_settings())
    yield


app = FastAPI(title="amsc-connector", lifespan=lifespan)
app.include_router(api_router)
