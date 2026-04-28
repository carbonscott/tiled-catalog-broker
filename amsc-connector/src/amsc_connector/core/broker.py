from functools import lru_cache

from faststream.redis.fastapi import RedisRouter

from amsc_connector.core.config import get_settings


@lru_cache(maxsize=1)
def get_stream_router() -> RedisRouter:
    return RedisRouter(get_settings().redis_dsn)
