from fastapi import APIRouter

from amsc_connector.api.routes import webhook

api_router = APIRouter()
api_router.include_router(webhook.router)
