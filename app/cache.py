import logging

import redis.asyncio as aioredis

from app.config import get_settings

logger = logging.getLogger(__name__)

_redis: aioredis.Redis | None = None


async def connect_cache() -> None:
    global _redis
    settings = get_settings()
    _redis = await aioredis.from_url(settings.redis_url, decode_responses=True)
    await _redis.ping()
    logger.info("Connected to Redis at %s", settings.redis_url)


async def close_cache() -> None:
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None
        logger.info("Redis connection closed")


def get_redis() -> aioredis.Redis:
    if _redis is None:
        raise RuntimeError("Redis client not initialised — was connect_cache() called?")
    return _redis
