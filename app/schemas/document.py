from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from app.models.document import DocumentStatus


class DocumentCreate(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=100)
    title: str = Field(..., min_length=1, max_length=500)
    content: str = Field(..., min_length=1, max_length=100_000)


class DocumentSubmitResponse(BaseModel):
    """Returned on POST /documents — minimal payload, sufficient for polling."""
    document_id: str
    status: DocumentStatus
    created_at: datetime


class DocumentResponse(BaseModel):
    document_id: str
    user_id: str
    title: str
    status: DocumentStatus
    summary: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None


class PaginatedDocumentsResponse(BaseModel):
    items: list[DocumentResponse]
    total: int
    page: int
    page_size: int
    has_next: bool
