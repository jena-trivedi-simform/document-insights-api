"""
Background document processing worker.

Design: a single asyncio Task that continuously polls MongoDB for queued
documents and processes them one at a time.

Race condition prevention: `find_one_and_update` is a single atomic MongoDB
operation.  If two worker instances (e.g. two replicas) call it simultaneously,
MongoDB's document-level locking guarantees that only one will receive the
document — the other will get None and move on.  This is far simpler and safer
than application-level locking.

Retry with exponential backoff: on a simulated failure the worker checks
whether the document has retries remaining.  If yes, it resets the status to
'queued' and sets `retry_after` to now + (base * 2^attempt) seconds — the next
poll cycle skips this document until that timestamp passes.  The rate-limit
slot is held across retries (the document stays active).  Only when retries are
exhausted is the slot released and the status set to 'failed'.

Worker vs Celery: for a single-process FastAPI deployment, an asyncio Task is
the right choice — no extra infrastructure, no message broker, low latency.
With horizontal scaling (multiple replicas), a proper task queue (Celery +
Redis broker) would be a better fit.  See README for the full trade-off.
"""

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone

from app.config import get_settings
from app.database import get_db
from app.models.document import DocumentStatus
from app.services.content_cache import set_cached_summary
from app.services.rate_limiter import decrement

logger = logging.getLogger(__name__)


def _build_summary(title: str, content: str) -> str:
    word_count = len(content.split())
    return (
        f"Summary of '{title}': {word_count}-word document analysed. "
        f"Key themes: information processing, content analysis. "
        f"Sentiment: neutral. Readability: standard."
    )


async def _process_one() -> bool:
    """
    Claim one queued document and process it.
    Returns True if a document was processed, False if the queue is empty.
    """
    db = get_db()
    settings = get_settings()
    now = datetime.now(timezone.utc)

    # Atomic claim: only pick up documents whose retry backoff has elapsed.
    # retry_after=None means a first attempt (no backoff yet).
    doc = await db.documents.find_one_and_update(
        {
            "status": DocumentStatus.QUEUED,
            "$or": [{"retry_after": None}, {"retry_after": {"$lte": now}}],
        },
        {
            "$set": {
                "status": DocumentStatus.PROCESSING,
                "processing_started_at": now,
                "updated_at": now,
            }
        },
        sort=[("created_at", 1)],
        return_document=True,
    )

    if doc is None:
        return False

    doc_id = str(doc["_id"])
    user_id = doc["user_id"]
    retry_count = doc.get("retry_count", 0)
    logger.info(
        "Worker claimed document %s for user %s (attempt %d)", doc_id, user_id, retry_count + 1
    )

    delay = random.uniform(settings.worker_min_delay, settings.worker_max_delay)
    await asyncio.sleep(delay)

    now = datetime.now(timezone.utc)
    should_fail = random.random() < settings.worker_failure_rate

    if should_fail:
        if retry_count < settings.max_retries:
            # Exponential backoff: 5s, 10s, 20s for attempts 0, 1, 2
            backoff_seconds = settings.retry_backoff_base * (2 ** retry_count)
            retry_after = now + timedelta(seconds=backoff_seconds)
            await db.documents.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "status": DocumentStatus.QUEUED,
                        "error_message": f"Attempt {retry_count + 1} failed — retrying in {backoff_seconds}s",
                        "retry_count": retry_count + 1,
                        "retry_after": retry_after,
                        "processing_started_at": None,
                        "updated_at": now,
                    }
                },
            )
            logger.warning(
                "Document %s failed (attempt %d/%d) — retry in %ds",
                doc_id, retry_count + 1, settings.max_retries, backoff_seconds,
            )
            # Rate-limit slot stays held — the document is still active
        else:
            await db.documents.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "status": DocumentStatus.FAILED,
                        "error_message": f"Failed after {settings.max_retries + 1} attempts",
                        "updated_at": now,
                        "completed_at": now,
                    }
                },
            )
            logger.error("Document %s permanently failed after %d attempts", doc_id, retry_count + 1)
            await decrement(user_id)
    else:
        summary = _build_summary(doc["title"], doc["content"])
        await db.documents.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "status": DocumentStatus.COMPLETED,
                    "summary": summary,
                    "updated_at": now,
                    "completed_at": now,
                }
            },
        )
        await set_cached_summary(
            user_id,
            doc["content_hash"],
            {"summary": summary, "completed_at": now.isoformat()},
        )
        logger.info("Document %s completed (%.0fs elapsed)", doc_id, delay)
        await decrement(user_id)

    return True


async def worker_loop() -> None:
    settings = get_settings()
    logger.info(
        "Worker started — poll interval %.1fs, delay %d–%ds, failure rate %.0f%%",
        settings.worker_poll_interval,
        settings.worker_min_delay,
        settings.worker_max_delay,
        settings.worker_failure_rate * 100,
    )

    while True:
        try:
            processed = await _process_one()
            if not processed:
                # Queue is empty; back off before polling again
                await asyncio.sleep(settings.worker_poll_interval)
        except asyncio.CancelledError:
            logger.info("Worker received shutdown signal")
            break
        except Exception:
            logger.exception("Unexpected error in worker loop — will retry after backoff")
            await asyncio.sleep(settings.worker_poll_interval)
