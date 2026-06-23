"""
Content-based deduplication cache using Redis.

Cache key: `content_cache:{user_id}:{content_hash}`

Why per-user scope?  Two different users may submit the same document text.
Returning user A's result to user B would be a privacy leak and would pollute
audit trails.  Per-user keys keep caches isolated.

TTL: configurable, defaults to 24 hours.  After TTL expires, the next
submission re-processes the document.  This is appropriate because summaries
are deterministic mock outputs; for a real LLM the TTL trade-off would differ.

Concurrent duplicate submissions (bonus):
  We use a short-lived Redis lock (`content_lock:{user_id}:{content_hash}`)
  with SET NX.  When two identical requests arrive simultaneously, the second
  one sees the lock and receives a 409 Conflict, asking the client to retry.
  On retry the first request will have completed (or at least its document
  record inserted) so the cache will hit.
"""

import json
import logging
from typing import Optional

from app.cache import get_redis
from app.config import get_settings

logger = logging.getLogger(__name__)


def _cache_key(user_id: str, content_hash: str) -> str:
    return f"content_cache:{user_id}:{content_hash}"


def _lock_key(user_id: str, content_hash: str) -> str:
    return f"content_lock:{user_id}:{content_hash}"


async def get_cached_summary(user_id: str, content_hash: str) -> Optional[dict]:
    try:
        data = await get_redis().get(_cache_key(user_id, content_hash))
        return json.loads(data) if data else None
    except Exception:
        logger.exception("Cache read failed for user=%s hash=%s", user_id, content_hash[:8])
        return None


async def set_cached_summary(user_id: str, content_hash: str, payload: dict) -> None:
    try:
        settings = get_settings()
        await get_redis().set(
            _cache_key(user_id, content_hash),
            json.dumps(payload),
            ex=settings.content_cache_ttl,
        )
    except Exception:
        logger.exception("Cache write failed for user=%s hash=%s", user_id, content_hash[:8])


async def acquire_submission_lock(user_id: str, content_hash: str, ttl_seconds: int = 60) -> bool:
    """
    Acquire an exclusive lock for a (user, content_hash) pair.
    SET NX is atomic — only one caller wins.
    Returns True if the lock was acquired, False if already held.
    """
    try:
        result = await get_redis().set(
            _lock_key(user_id, content_hash), "1", ex=ttl_seconds, nx=True
        )
        return result is not None
    except Exception:
        logger.exception("Lock acquisition failed — allowing through")
        return True  # fail open


async def release_submission_lock(user_id: str, content_hash: str) -> None:
    try:
        await get_redis().delete(_lock_key(user_id, content_hash))
    except Exception:
        logger.exception("Lock release failed for user=%s hash=%s", user_id, content_hash[:8])
