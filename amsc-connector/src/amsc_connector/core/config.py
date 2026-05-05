from functools import lru_cache

from pydantic import AliasChoices, AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Internal URL for making API calls to the tiled server from within Docker
    tiled_url: AnyHttpUrl = AnyHttpUrl("http://tiled:8000")

    # API key — maps to the env var tiled uses for single-user mode
    tiled_api_key: str = Field(
        validation_alias=AliasChoices("TILED_API_KEY", "TILED_SINGLE_USER_API_KEY"),
    )

    # URL that tiled will POST webhook events to
    webhook_external_url: AnyHttpUrl

    # HMAC signing secret — the connector verifies the
    # X-Tiled-Signature header on every incoming webhook event
    webhook_secret: str

    # Tiled node path to watch; empty string means the catalog root
    webhook_target_path: str = ""


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
