"""
Integration tests for the Document Insights API.

Coverage:
  - POST /documents: success (201), rate limit (429), content cache hit
  - GET /documents/{id}: success (200), not found (404), invalid id (404)
  - GET /users/{user_id}/documents: pagination, status filter
"""

import asyncio
import uuid

import pytest
import pytest_asyncio
from httpx import AsyncClient


def unique_user() -> str:
    """Each test gets an isolated user to avoid cross-test rate-limit interference."""
    return f"test-user-{uuid.uuid4().hex[:8]}"


# ── Submit document ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_submit_document_returns_201(client: AsyncClient):
    user_id = unique_user()
    response = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "Test Doc", "content": "Hello world content"},
    )
    assert response.status_code == 201
    data = response.json()
    assert "document_id" in data
    assert data["status"] == "queued"
    assert "created_at" in data


@pytest.mark.asyncio
async def test_submit_document_missing_fields_returns_422(client: AsyncClient):
    response = await client.post("/documents", json={"user_id": "u1"})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_submit_document_empty_content_returns_422(client: AsyncClient):
    response = await client.post(
        "/documents",
        json={"user_id": "u1", "title": "T", "content": ""},
    )
    assert response.status_code == 422


# ── Rate limiting ────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_rate_limit_blocks_fourth_document(client: AsyncClient):
    user_id = unique_user()
    payload = lambda i: {
        "user_id": user_id,
        "title": f"Doc {i}",
        "content": f"Unique content number {i} — {uuid.uuid4()}",
    }

    # First three should succeed
    responses = []
    for i in range(3):
        r = await client.post("/documents", json=payload(i))
        responses.append(r)

    assert all(r.status_code == 201 for r in responses), [r.json() for r in responses]

    # Fourth must be rejected
    r = await client.post("/documents", json=payload(3))
    assert r.status_code == 429


@pytest.mark.asyncio
async def test_rate_limit_releases_after_completion(client: AsyncClient):
    """Completing a job frees a rate-limit slot."""
    user_id = unique_user()

    # Submit one document and wait for it to complete (worker delay ≤ 1s in tests)
    r = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "First", "content": f"content {uuid.uuid4()}"},
    )
    assert r.status_code == 201
    doc_id = r.json()["document_id"]

    # Poll until completed (max ~5 s even with delay=1)
    for _ in range(10):
        await asyncio.sleep(0.5)
        status_r = await client.get(f"/documents/{doc_id}")
        if status_r.json()["status"] == "completed":
            break

    assert status_r.json()["status"] == "completed"

    # Now we can submit three more (the slot was released)
    for i in range(3):
        r = await client.post(
            "/documents",
            json={
                "user_id": user_id,
                "title": f"Follow-up {i}",
                "content": f"follow up {uuid.uuid4()}",
            },
        )
        assert r.status_code == 201, r.json()


# ── Content caching ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_duplicate_content_returns_cached_summary(client: AsyncClient):
    """Second submission with identical content gets status=completed immediately."""
    user_id = unique_user()
    content = f"Identical content body {uuid.uuid4()}"

    r1 = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "Original", "content": content},
    )
    assert r1.status_code == 201
    doc1_id = r1.json()["document_id"]

    # Wait for first document to complete so the cache is populated
    for _ in range(20):
        await asyncio.sleep(0.5)
        doc1 = (await client.get(f"/documents/{doc1_id}")).json()
        if doc1["status"] == "completed":
            break
    assert doc1["status"] == "completed"

    # Second submission — should hit cache and return completed immediately
    r2 = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "Duplicate", "content": content},
    )
    assert r2.status_code == 201
    assert r2.json()["status"] == "completed"


@pytest.mark.asyncio
async def test_duplicate_content_different_users_processed_separately(client: AsyncClient):
    """Cache is per-user — same content from different users is NOT shared."""
    content = f"Shared content {uuid.uuid4()}"
    user_a = unique_user()
    user_b = unique_user()

    r_a = await client.post(
        "/documents",
        json={"user_id": user_a, "title": "A's doc", "content": content},
    )
    r_b = await client.post(
        "/documents",
        json={"user_id": user_b, "title": "B's doc", "content": content},
    )

    assert r_a.status_code == 201
    assert r_b.status_code == 201
    # Both should start as queued — neither user has a cached result yet
    assert r_a.json()["status"] == "queued"
    assert r_b.json()["status"] == "queued"


# ── Get document ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_document_returns_full_record(client: AsyncClient):
    user_id = unique_user()
    r = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "My Title", "content": "Some body text"},
    )
    doc_id = r.json()["document_id"]

    detail = await client.get(f"/documents/{doc_id}")
    assert detail.status_code == 200
    data = detail.json()
    assert data["document_id"] == doc_id
    assert data["user_id"] == user_id
    assert data["title"] == "My Title"


@pytest.mark.asyncio
async def test_get_nonexistent_document_returns_404(client: AsyncClient):
    fake_id = "000000000000000000000000"
    r = await client.get(f"/documents/{fake_id}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_get_invalid_id_format_returns_404(client: AsyncClient):
    r = await client.get("/documents/not-an-object-id")
    assert r.status_code == 404


# ── List user documents ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_list_user_documents_pagination(client: AsyncClient):
    # Per-user rate limit is 3 active docs.  Use 3 docs with page_size=2 so
    # we can still exercise has_next=True on page 1 and has_next=False on page 2.
    user_id = unique_user()
    for i in range(3):
        await client.post(
            "/documents",
            json={"user_id": user_id, "title": f"Doc {i}", "content": f"body {uuid.uuid4()}"},
        )

    page1 = await client.get(f"/users/{user_id}/documents?page=1&page_size=2")
    assert page1.status_code == 200
    data1 = page1.json()
    assert data1["total"] == 3
    assert len(data1["items"]) == 2
    assert data1["has_next"] is True

    page2 = await client.get(f"/users/{user_id}/documents?page=2&page_size=2")
    data2 = page2.json()
    assert len(data2["items"]) == 1
    assert data2["has_next"] is False


@pytest.mark.asyncio
async def test_list_user_documents_status_filter(client: AsyncClient):
    user_id = unique_user()
    content = f"filterable content {uuid.uuid4()}"

    r = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "T", "content": content},
    )
    doc_id = r.json()["document_id"]

    # Wait for completion
    for _ in range(20):
        await asyncio.sleep(0.5)
        if (await client.get(f"/documents/{doc_id}")).json()["status"] == "completed":
            break

    completed = await client.get(f"/users/{user_id}/documents?status=completed")
    assert completed.status_code == 200
    assert completed.json()["total"] >= 1

    queued = await client.get(f"/users/{user_id}/documents?status=queued")
    assert queued.status_code == 200
    assert queued.json()["total"] == 0


@pytest.mark.asyncio
async def test_list_unknown_user_returns_empty(client: AsyncClient):
    r = await client.get(f"/users/{unique_user()}/documents")
    assert r.status_code == 200
    assert r.json()["total"] == 0
    assert r.json()["items"] == []
