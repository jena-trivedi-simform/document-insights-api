import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.database import get_db
from app.models.document import DocumentStatus
from app.schemas.document import DocumentResponse, PaginatedDocumentsResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/users", tags=["users"])


@router.get("/{user_id}/documents", response_model=PaginatedDocumentsResponse)
async def list_user_documents(
    user_id: str,
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(20, ge=1, le=100, description="Documents per page"),
    status: Optional[DocumentStatus] = Query(None, description="Filter by processing status"),
    db: AsyncIOMotorDatabase = Depends(get_db),
) -> PaginatedDocumentsResponse:
    query: dict = {"user_id": user_id}
    if status is not None:
        query["status"] = status

    skip = (page - 1) * page_size

    # count_documents and find can run concurrently — both are non-blocking motor calls
    total = await db.documents.count_documents(query)
    cursor = db.documents.find(query).sort("created_at", -1).skip(skip).limit(page_size)
    docs = await cursor.to_list(length=page_size)

    items = [
        DocumentResponse(
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
        for doc in docs
    ]

    return PaginatedDocumentsResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_next=(skip + len(items)) < total,
    )
