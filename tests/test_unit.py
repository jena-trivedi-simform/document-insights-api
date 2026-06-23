"""
Unit tests — no live services, no I/O.

Exercises pure-Python logic: enum helpers, Pydantic schema validation, the
worker's summary builder, and the router helper functions that map MongoDB
documents to API response models.
"""

import hashlib
from datetime import datetime, timezone

import pytest
from bson import ObjectId
from pydantic import ValidationError

from app.models.document import DocumentStatus
from app.routers.documents import _new_doc, _to_response
from app.schemas.document import DocumentCreate
from app.services.worker import _build_summary


# ── DocumentStatus ─────────────────────────────────────────────────────────────

def test_active_statuses_contains_queued_and_processing():
    active = DocumentStatus.active_statuses()
    assert DocumentStatus.QUEUED in active
    assert DocumentStatus.PROCESSING in active


def test_active_statuses_excludes_terminal_states():
    active = DocumentStatus.active_statuses()
    assert DocumentStatus.COMPLETED not in active
    assert DocumentStatus.FAILED not in active


def test_document_status_string_values():
    assert DocumentStatus.QUEUED == "queued"
    assert DocumentStatus.PROCESSING == "processing"
    assert DocumentStatus.COMPLETED == "completed"
    assert DocumentStatus.FAILED == "failed"


# ── DocumentCreate validation ─────────────────────────────────────────────────

def test_document_create_valid():
    doc = DocumentCreate(user_id="user1", title="My Title", content="Some content")
    assert doc.user_id == "user1"
    assert doc.title == "My Title"
    assert doc.content == "Some content"


def test_document_create_max_length_boundaries_pass():
    """Values exactly at the maximum allowed length must be accepted."""
    DocumentCreate(user_id="u" * 100, title="t" * 500, content="c" * 100_000)


def test_document_create_empty_user_id_rejected():
    with pytest.raises(ValidationError):
        DocumentCreate(user_id="", title="T", content="body")


def test_document_create_user_id_too_long_rejected():
    with pytest.raises(ValidationError):
        DocumentCreate(user_id="x" * 101, title="T", content="body")


def test_document_create_empty_title_rejected():
    with pytest.raises(ValidationError):
        DocumentCreate(user_id="u1", title="", content="body")


def test_document_create_title_too_long_rejected():
    with pytest.raises(ValidationError):
        DocumentCreate(user_id="u1", title="t" * 501, content="body")


def test_document_create_empty_content_rejected():
    with pytest.raises(ValidationError):
        DocumentCreate(user_id="u1", title="T", content="")


def test_document_create_content_too_long_rejected():
    with pytest.raises(ValidationError):
        DocumentCreate(user_id="u1", title="T", content="x" * 100_001)


# ── Worker: _build_summary ────────────────────────────────────────────────────

def test_build_summary_contains_title():
    summary = _build_summary("Quarterly Report", "word " * 20)
    assert "Quarterly Report" in summary


def test_build_summary_includes_correct_word_count():
    content = "one two three four five"
    summary = _build_summary("Test", content)
    assert "5-word" in summary


def test_build_summary_single_word_content():
    summary = _build_summary("Title", "hello")
    assert "1-word" in summary


def test_build_summary_is_non_empty_string():
    result = _build_summary("T", "Some content here")
    assert isinstance(result, str)
    assert len(result) > 0


# ── Router helper: _to_response ───────────────────────────────────────────────

def _make_mongo_doc(**overrides) -> dict:
    now = datetime.now(timezone.utc)
    base = {
        "_id": ObjectId(),
        "user_id": "user1",
        "title": "Title",
        "status": DocumentStatus.QUEUED,
        "summary": None,
        "error_message": None,
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
    }
    base.update(overrides)
    return base


def test_to_response_maps_object_id_to_string():
    doc = _make_mongo_doc()
    resp = _to_response(doc)
    assert resp.document_id == str(doc["_id"])
    assert isinstance(resp.document_id, str)


def test_to_response_optional_fields_are_none_when_absent():
    doc = _make_mongo_doc()
    resp = _to_response(doc)
    assert resp.summary is None
    assert resp.error_message is None
    assert resp.completed_at is None


def test_to_response_includes_summary_when_present():
    doc = _make_mongo_doc(status=DocumentStatus.COMPLETED, summary="The summary text.")
    resp = _to_response(doc)
    assert resp.summary == "The summary text."


def test_to_response_preserves_all_scalar_fields():
    doc = _make_mongo_doc(user_id="alice", title="Report")
    resp = _to_response(doc)
    assert resp.user_id == "alice"
    assert resp.title == "Report"
    assert resp.status == DocumentStatus.QUEUED


# ── Router helper: _new_doc ───────────────────────────────────────────────────

def _make_payload(**kwargs) -> DocumentCreate:
    defaults = {"user_id": "u1", "title": "T", "content": "hello world"}
    defaults.update(kwargs)
    return DocumentCreate(**defaults)


def test_new_doc_status_is_queued():
    payload = _make_payload()
    now = datetime.now(timezone.utc)
    doc = _new_doc(payload, "hash123", now)
    assert doc["status"] == DocumentStatus.QUEUED


def test_new_doc_initial_retry_state():
    payload = _make_payload()
    now = datetime.now(timezone.utc)
    doc = _new_doc(payload, "hash123", now)
    assert doc["retry_count"] == 0
    assert doc["retry_after"] is None


def test_new_doc_preserves_payload_fields():
    payload = _make_payload(user_id="alice", title="Annual Report", content="Analysis text")
    now = datetime.now(timezone.utc)
    h = hashlib.sha256(b"Analysis text").hexdigest()
    doc = _new_doc(payload, h, now)
    assert doc["user_id"] == "alice"
    assert doc["title"] == "Annual Report"
    assert doc["content"] == "Analysis text"
    assert doc["content_hash"] == h


def test_new_doc_summary_and_error_are_none():
    payload = _make_payload()
    doc = _new_doc(payload, "h", datetime.now(timezone.utc))
    assert doc["summary"] is None
    assert doc["error_message"] is None


def test_new_doc_timestamps_match_provided_now():
    payload = _make_payload()
    now = datetime.now(timezone.utc)
    doc = _new_doc(payload, "h", now)
    assert doc["created_at"] == now
    assert doc["updated_at"] == now
