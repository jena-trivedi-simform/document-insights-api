import asyncio
import json
import logging
import logging.config
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.cache import close_cache, connect_cache
from app.config import get_settings
from app.database import (
    close_db,
    connect_db,
    init_indexes,
    reset_stale_processing_documents,
)
from app.routers import documents, health, users
from app.services.rate_limiter import resync_from_db
from app.services.worker import worker_loop


class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log line — machine-parseable by log aggregators."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "timestamp": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = traceback.format_exception(*record.exc_info)
        return json.dumps(payload)


def _configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.root.setLevel(logging.INFO)
    logging.root.handlers = [handler]


_configure_logging()
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    await connect_db()
    await connect_cache()
    await init_indexes()

    # Recover from a previous crash: reset stuck jobs, resync Redis counters
    from app.database import get_db
    db = get_db()
    await reset_stale_processing_documents()
    await resync_from_db(db)

    worker_task = asyncio.create_task(worker_loop(), name="document-worker")
    logger.info("Application startup complete")

    yield  # ── serving ──────────────────────────────────────────────────────

    # ── Shutdown ─────────────────────────────────────────────────────────────
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

    await close_cache()
    await close_db()
    logger.info("Application shutdown complete")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        description=(
            "Async document processing API with per-user rate limiting "
            "and content-based result caching."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    app.include_router(health.router)
    app.include_router(documents.router)
    app.include_router(users.router)

    return app


app = create_app()
