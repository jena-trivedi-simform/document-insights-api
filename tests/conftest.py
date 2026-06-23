"""
Integration test configuration.

Tests run against real MongoDB and Redis instances.  For local development,
start the stack first:

    docker-compose up -d mongodb redis

The test suite uses a separate database (`document_insights_test`) and resets
state between every test so tests remain independent.

Worker delays are collapsed to 0–1 s and the failure rate to 0 so tests are
deterministic.
"""

import asyncio
import os

# Must be set before any app module is imported so @lru_cache picks them up
os.environ.setdefault("MONGODB_URL", "mongodb://localhost:27017")
os.environ.setdefault("MONGODB_DB_NAME", "document_insights_test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379")
os.environ.setdefault("WORKER_MIN_DELAY", "0")
os.environ.setdefault("WORKER_MAX_DELAY", "1")
os.environ.setdefault("WORKER_FAILURE_RATE", "0")

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient


@pytest_asyncio.fixture(scope="session")
async def client():
    """
    Session-scoped AsyncClient.

    ASGITransport (httpx ≥ 0.27) no longer sends ASGI lifespan events, so we
    drive startup and shutdown explicitly here instead of relying on the app's
    lifespan context manager.
    """
    import asyncio

    from app.cache import close_cache, connect_cache
    from app.database import (
        close_db,
        connect_db,
        get_db,
        init_indexes,
        reset_stale_processing_documents,
    )
    from app.main import app
    from app.services.rate_limiter import resync_from_db
    from app.services.worker import worker_loop

    await connect_db()
    await connect_cache()
    await init_indexes()
    await reset_stale_processing_documents()
    await resync_from_db(get_db())
    worker_task = asyncio.create_task(worker_loop(), name="document-worker-test")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass
    await close_cache()
    await close_db()


@pytest_asyncio.fixture(autouse=True)
async def clean_state(client):
    """Reset all test data before each test."""
    from app.cache import get_redis
    from app.database import get_db

    yield

    # Guard: unit tests run without live services; skip cleanup when uninitialised.
    try:
        db = get_db()
        redis = get_redis()
    except RuntimeError:
        return

    await db.documents.delete_many({})

    # Remove only test-related Redis keys to avoid touching unrelated data
    async for key in redis.scan_iter("rate_limit:*"):
        await redis.delete(key)
    async for key in redis.scan_iter("content_cache:*"):
        await redis.delete(key)
    async for key in redis.scan_iter("content_lock:*"):
        await redis.delete(key)
