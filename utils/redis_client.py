from __future__ import annotations

import redis.asyncio as aioredis

from config import get_settings
from utils.logger import get_logger

log = get_logger(__name__)

_redis: aioredis.Redis | None = None


async def get_redis() -> aioredis.Redis:
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(
            get_settings().redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=2000,
        )
        await _redis.ping()
        log.info("redis.connected", url=get_settings().redis_url)
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None
