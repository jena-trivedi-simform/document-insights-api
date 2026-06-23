import hashlib
import logging
from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import get_settings
from app.database import get_db
from app.models.document import DocumentStatus
from app.schemas.document import DocumentResponse, DocumentSubmitResponse, DocumentCreate
from app.services.content_cache import (
    acquire_submission_lock,
    get_cached_summary,
    release_submission_lock,
)
from app.services.rate_limiter import check_and_increment

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/documents", tags=["documents"])


def _to_response(doc: dict) -> DocumentResponse:
    return DocumentResponse(
        document_id=str(doc["_id"]),
        user_id=doc["user_id"],
        title=doc["title"],
        status=doc["status"],
        summary=doc.get("summary"),
        error_message=doc.get("error_message"),
        created_at=doc["created_at"],
        updated_at=doc["updated_at"],
        completed_at=doc.get("completed_at"),
    )


def _new_doc(payload: DocumentCreate, content_hash: str, now: datetime) -> dict:
    return {
        "user_id": payload.user_id,
        "title": payload.title,
        "content": payload.content,
        "content_hash": content_hash,
        "status": DocumentStatus.QUEUED,
        "summary": None,
        "error_message": None,
        "retry_count": 0,
        "retry_after": None,  # set by worker on each failed attempt
        "created_at": now,
        "updated_at": now,
        "processing_started_at": None,
        "completed_at": None,
    }


@router.post("", status_code=status.HTTP_201_CREATED, response_model=DocumentSubmitResponse)
async def submit_document(
    payload: DocumentCreate,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> DocumentSubmitResponse:
    content_hash = hashlib.sha256(payload.content.encode()).hexdigest()

    # --- Fast path: cache hit (no worker slot consumed) ---
    cached = await get_cached_summary(payload.user_id, content_hash)
    if cached:
        logger.info("Cache hit for user=%s hash=%.8s", payload.user_id, content_hash)
        now = datetime.now(timezone.utc)
        doc = {
            **_new_doc(payload, content_hash, now),
            "status": DocumentStatus.COMPLETED,
            "summary": cached["summary"],
            "completed_at": now,
        }
        result = await db.documents.insert_one(doc)
        return DocumentSubmitResponse(
            document_id=str(result.inserted_id),
            status=DocumentStatus.COMPLETED,
            created_at=now,
        )

    # --- Concurrent-duplicate guard: acquire short-lived lock ---
    # Prevents two simultaneous identical submissions from both missing the cache
    # and both enqueueing separate jobs for the same content.
    lock_acquired = await acquire_submission_lock(payload.user_id, content_hash)
    if not lock_acquired:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A document with identical content is already being submitted. Retry in a moment.",
        )

    try:
        # Double-check cache under lock (another request may have finished while
        # we were waiting for the lock)
        cached = await get_cached_summary(payload.user_id, content_hash)
        if cached:
            now = datetime.now(timezone.utc)
            doc = {
                **_new_doc(payload, content_hash, now),
                "status": DocumentStatus.COMPLETED,
                "summary": cached["summary"],
                "completed_at": now,
            }
            result = await db.documents.insert_one(doc)
            return DocumentSubmitResponse(
                document_id=str(result.inserted_id),
                status=DocumentStatus.COMPLETED,
                created_at=now,
            )

        # --- Rate limit: atomic check-and-increment ---
        allowed = await check_and_increment(payload.user_id)
        if not allowed:
            settings = get_settings()
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    f"Rate limit exceeded: at most {settings.max_active_per_user} documents "
                    "may be queued or processing per user at a time."
                ),
            )

        now = datetime.now(timezone.utc)
        result = await db.documents.insert_one(_new_doc(payload, content_hash, now))
        logger.info("Document %s queued for user=%s", result.inserted_id, payload.user_id)

        return DocumentSubmitResponse(
            document_id=str(result.inserted_id),
            status=DocumentStatus.QUEUED,
            created_at=now,
        )

    except HTTPException:
        raise
    except Exception:
        logger.exception("Unexpected error submitting document for user=%s", payload.user_id)
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        await release_submission_lock(payload.user_id, content_hash)


@router.get("/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: str,
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> DocumentResponse:
    if not ObjectId.is_valid(document_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")

    doc = await db.documents.find_one({"_id": ObjectId(document_id)})
    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Document not found")

    return _to_response(doc)
