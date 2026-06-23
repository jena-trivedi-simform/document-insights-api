"""
Per-user rate limiting using Redis.

Design: we maintain a counter key `rate_limit:{user_id}` in Redis that tracks
how many of the user's jobs are currently in queued or processing state.

The check-and-increment MUST be atomic.  A naive GET-then-SET would allow two
concurrent requests to both read a count of 2, both decide that 2 < 3, and both
insert — leaving the counter at 4 instead of 3.

We use a Redis Lua script: the script runs atomically on the server, so no other
command can interleave between the read and the write.

Graceful degradation: if Redis is unavailable, we allow the request through and
log a warning.  The alternative (hard-fail) would make the whole API dependent on
Redis uptime, which is a worse trade-off for this service.
"""

import logging

from app.cache import get_redis
from app.config import get_settings

logger = logging.getLogger(__name__)

# Atomically: read current count; if under limit, increment and return 1; else return 0.
_INCREMENT_IF_UNDER_LIMIT = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current >= tonumber(ARGV[1]) then
    return 0
end
redis.call('INCR', KEYS[1])
return 1
"""


def _key(user_id: str) -> str:
    return f"rate_limit:{user_id}"


async def check_and_increment(user_id: str) -> bool:
    """
    Atomically check whether the user is under the rate limit and, if so,
    increment their active-job count.

    Returns True if the slot was acquired (request should proceed),
    False if the user is at the limit (caller should return HTTP 429).
    """
    settings = get_settings()
    try:
        redis = get_redis()
        result = await redis.eval(
            _INCREMENT_IF_UNDER_LIMIT,
            1,                              # number of keys
            _key(user_id),                  # KEYS[1]
            settings.max_active_per_user,   # ARGV[1]
        )
        return bool(result)
    except Exception:
        logger.exception("Redis rate-limit check failed for user %s — allowing through", user_id)
        return True  # fail open


async def decrement(user_id: str) -> None:
    """
    Release one active-job slot when a job reaches a terminal state
    (completed or failed).  Guards against negative counts caused by
    counter drift after crashes.
    """
    try:
        redis = get_redis()
        new_value = await redis.decr(_key(user_id))
        if new_value < 0:
            await redis.set(_key(user_id), 0)
    except Exception:
        logger.exception("Redis rate-limit decrement failed for user %s", user_id)


async def resync_from_db(db) -> None:
    """
    Rebuild Redis counters from MongoDB on startup.  This corrects any drift
    caused by a crash between the Redis increment and the MongoDB insert, or
    between job completion and the Redis decrement.
    """
    pipeline = [
        {"$match": {"status": {"$in": ["queued", "processing"]}}},
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
    ]
    try:
        redis = get_redis()
        async for row in db.documents.aggregate(pipeline):
            await redis.set(_key(row["_id"]), row["count"])
        logger.info("Rate-limit counters resynced from MongoDB")
    except Exception:
        logger.exception("Failed to resync rate-limit counters — counters may be stale")
