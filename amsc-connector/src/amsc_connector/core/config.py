from functools import lru_cache

from pydantic import AliasChoices, AnyHttpUrl, Field, RedisDsn, computed_field
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

    # Optional HMAC signing secret — when set, the connector verifies the
    # X-Tiled-Signature header on every incoming webhook event
    webhook_secret: str | None = None

    # Tiled node path to watch; empty string means the catalog root
    webhook_target_path: str = ""

    # Redis connection parameters
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_password: str

    @computed_field
    @property
    def redis_dsn(self) -> str:
        return RedisDsn(
            f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}"
        ).unicode_string()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
