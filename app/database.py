import logging
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import get_settings

logger = logging.getLogger(__name__)

_client: AsyncIOMotorClient | None = None


async def connect_db() -> None:
    global _client
    settings = get_settings()
    _client = AsyncIOMotorClient(settings.mongodb_url)
    # Force a connection to validate the URL is reachable
    await _client.admin.command("ping")
    logger.info("Connected to MongoDB at %s", settings.mongodb_url)


async def close_db() -> None:
    global _client
    if _client:
        _client.close()
        _client = None
        logger.info("MongoDB connection closed")


def get_db() -> AsyncIOMotorDatabase:
    if _client is None:
        raise RuntimeError("MongoDB client not initialised — was connect_db() called?")
    return _client[get_settings().mongodb_db_name]


async def init_indexes() -> None:
    """Create indexes once on startup. safe to call repeatedly (create_index is idempotent)."""
    db = get_db()
    col = db.documents

    # Supports worker's queued-document poll (FIFO)
    await col.create_index([("status", 1), ("created_at", 1)])
    # Supports rate-limit query and filtered user listing
    await col.create_index([("user_id", 1), ("status", 1)])
    # Supports paginated user listing
    await col.create_index([("user_id", 1), ("created_at", -1)])
    # Supports content dedup lookup without Redis (fallback)
    await col.create_index([("user_id", 1), ("content_hash", 1)])

    logger.info("MongoDB indexes created/verified")


async def reset_stale_processing_documents() -> None:
    """
    On startup, any document left in 'processing' state means a previous
    instance crashed mid-job. Reset them to 'queued' so the worker retries.
    """
    db = get_db()
    now = datetime.now(timezone.utc)
    result = await db.documents.update_many(
        {"status": "processing"},
        {"$set": {"status": "queued", "processing_started_at": None, "updated_at": now}},
    )
    if result.modified_count:
        logger.warning(
            "Reset %d stale processing document(s) to queued", result.modified_count
        )
