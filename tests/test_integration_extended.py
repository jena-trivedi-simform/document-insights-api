"""
Extended integration tests.

Covers scenarios not present in test_documents.py:
  - Input validation boundary values (422 on over-limit fields)
  - Completed document response: summary content, timestamps, all required fields
  - Content-cache hit returns HTTP 201 (not 200) with status=completed
  - Rate-limit service: check_and_increment / decrement behave atomically
  - Pagination edge cases: exact boundary, beyond-last-page, invalid params
"""

import asyncio
import uuid

import pytest
from httpx import AsyncClient


def unique_user() -> str:
    return f"test-user-{uuid.uuid4().hex[:8]}"


# ── Input validation boundary values ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_submit_empty_user_id_returns_422(client: AsyncClient):
    r = await client.post(
        "/documents",
        json={"user_id": "", "title": "T", "content": "body"},
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_submit_user_id_over_max_length_returns_422(client: AsyncClient):
    r = await client.post(
        "/documents",
        json={"user_id": "x" * 101, "title": "T", "content": "body"},
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_submit_empty_title_returns_422(client: AsyncClient):
    r = await client.post(
        "/documents",
        json={"user_id": "u1", "title": "", "content": "body"},
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_submit_title_over_max_length_returns_422(client: AsyncClient):
    r = await client.post(
        "/documents",
        json={"user_id": "u1", "title": "t" * 501, "content": "body"},
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_submit_content_over_max_length_returns_422(client: AsyncClient):
    r = await client.post(
        "/documents",
        json={"user_id": "u1", "title": "T", "content": "x" * 100_001},
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_submit_at_max_length_boundaries_returns_201(client: AsyncClient):
    """Values exactly at the field limits must be accepted."""
    user_id = unique_user()
    r = await client.post(
        "/documents",
        json={
            "user_id": user_id[:100],
            "title": "t" * 500,
            "content": "c" * 100_000,
        },
    )
    assert r.status_code == 201


# ── Completed document response fields ────────────────────────────────────────

@pytest.mark.asyncio
async def test_completed_document_has_non_empty_summary(client: AsyncClient):
    """After worker processes the doc, the summary field is populated."""
    user_id = unique_user()
    r = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "AI Report", "content": f"text {uuid.uuid4()}"},
    )
    assert r.status_code == 201
    doc_id = r.json()["document_id"]

    doc = {}
    for _ in range(20):
        await asyncio.sleep(0.5)
        doc = (await client.get(f"/documents/{doc_id}")).json()
        if doc["status"] == "completed":
            break

    assert doc["status"] == "completed"
    assert doc["summary"] is not None
    assert len(doc["summary"]) > 0


@pytest.mark.asyncio
async def test_completed_document_summary_contains_title(client: AsyncClient):
    """The worker's mock summary embeds the document title."""
    user_id = unique_user()
    title = f"Report-{uuid.uuid4().hex[:6]}"
    r = await client.post(
        "/documents",
        json={"user_id": user_id, "title": title, "content": f"body {uuid.uuid4()}"},
    )
    doc_id = r.json()["document_id"]

    doc = {}
    for _ in range(20):
        await asyncio.sleep(0.5)
        doc = (await client.get(f"/documents/{doc_id}")).json()
        if doc["status"] == "completed":
            break

    assert doc["status"] == "completed"
    assert title in doc["summary"]


@pytest.mark.asyncio
async def test_completed_document_has_completed_at_timestamp(client: AsyncClient):
    user_id = unique_user()
    r = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "TS Test", "content": f"body {uuid.uuid4()}"},
    )
    doc_id = r.json()["document_id"]

    doc = {}
    for _ in range(20):
        await asyncio.sleep(0.5)
        doc = (await client.get(f"/documents/{doc_id}")).json()
        if doc["status"] == "completed":
            break

    assert doc["completed_at"] is not None


@pytest.mark.asyncio
async def test_get_document_has_all_required_fields(client: AsyncClient):
    """GET /documents/{id} response contains every field in DocumentResponse."""
    user_id = unique_user()
    r = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "Fields Check", "content": "body text"},
    )
    doc_id = r.json()["document_id"]

    doc = (await client.get(f"/documents/{doc_id}")).json()
    required = {"document_id", "user_id", "title", "status", "created_at", "updated_at"}
    assert required.issubset(doc.keys())
    assert doc["document_id"] == doc_id
    assert doc["user_id"] == user_id
    assert doc["title"] == "Fields Check"


# ── Cache hit HTTP status ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_cached_resubmission_returns_201_not_200(client: AsyncClient):
    """A cache-hit must still return HTTP 201, not 200."""
    user_id = unique_user()
    content = f"cache-check {uuid.uuid4()}"

    r1 = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "Original", "content": content},
    )
    assert r1.status_code == 201
    doc1_id = r1.json()["document_id"]

    for _ in range(20):
        await asyncio.sleep(0.5)
        if (await client.get(f"/documents/{doc1_id}")).json()["status"] == "completed":
            break

    r2 = await client.post(
        "/documents",
        json={"user_id": user_id, "title": "Duplicate", "content": content},
    )
    assert r2.status_code == 201
    assert r2.json()["status"] == "completed"


# ── Rate limiter service (requires live Redis) ────────────────────────────────

@pytest.mark.asyncio
async def test_rate_limiter_check_and_increment_allows_up_to_limit(client: AsyncClient):
    """
    Direct service-level test: check_and_increment returns True up to the
    configured limit (3), then False.
    """
    from app.services.rate_limiter import check_and_increment, decrement

    user_id = f"svc-{uuid.uuid4().hex[:8]}"
    results = [await check_and_increment(user_id) for _ in range(4)]
    assert results[:3] == [True, True, True]
    assert results[3] is False

    # Clean up slots
    for _ in range(3):
        await decrement(user_id)


@pytest.mark.asyncio
async def test_rate_limiter_decrement_frees_slot(client: AsyncClient):
    """Decrementing after filling the limit allows one more increment."""
    from app.services.rate_limiter import check_and_increment, decrement

    user_id = f"svc-{uuid.uuid4().hex[:8]}"
    for _ in range(3):
        await check_and_increment(user_id)

    # At limit; next one should fail
    assert await check_and_increment(user_id) is False

    # Release one slot
    await decrement(user_id)
    assert await check_and_increment(user_id) is True

    # Clean up
    for _ in range(3):
        await decrement(user_id)


@pytest.mark.asyncio
async def test_rate_limiter_decrement_does_not_go_below_zero(client: AsyncClient):
    """Decrementing an already-zero counter must not produce a negative count."""
    from app.cache import get_redis
    from app.services.rate_limiter import decrement

    user_id = f"svc-{uuid.uuid4().hex[:8]}"
    await decrement(user_id)  # counter starts at 0 (key absent)

    redis = get_redis()
    raw = await redis.get(f"rate_limit:{user_id}")
    count = int(raw) if raw is not None else 0
    assert count >= 0


# ── Pagination edge cases ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_pagination_has_next_false_when_docs_fill_exactly_one_page(client: AsyncClient):
    # Use 3 docs (= rate limit) with page_size=3 — all items fit on one page.
    user_id = unique_user()
    for i in range(3):
        await client.post(
            "/documents",
            json={"user_id": user_id, "title": f"D{i}", "content": f"body {uuid.uuid4()}"},
        )

    r = await client.get(f"/users/{user_id}/documents?page=1&page_size=3")
    data = r.json()
    assert data["total"] == 3
    assert len(data["items"]) == 3
    assert data["has_next"] is False


@pytest.mark.asyncio
async def test_pagination_page_beyond_last_returns_empty_items(client: AsyncClient):
    user_id = unique_user()
    for i in range(2):
        await client.post(
            "/documents",
            json={"user_id": user_id, "title": f"D{i}", "content": f"body {uuid.uuid4()}"},
        )

    r = await client.get(f"/users/{user_id}/documents?page=99&page_size=10")
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 2
    assert data["items"] == []
    assert data["has_next"] is False


@pytest.mark.asyncio
async def test_pagination_page_zero_returns_422(client: AsyncClient):
    """page is 1-based; page=0 violates the ge=1 constraint."""
    r = await client.get("/users/any-user/documents?page=0")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_pagination_page_size_over_100_returns_422(client: AsyncClient):
    """page_size has an upper bound of 100."""
    r = await client.get("/users/any-user/documents?page_size=101")
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_list_documents_returns_correct_pagination_metadata(client: AsyncClient):
    # 3 docs (= rate limit), page_size=1 → 3 pages.  Page 2 has 1 item, has_next=True.
    user_id = unique_user()
    for i in range(3):
        await client.post(
            "/documents",
            json={"user_id": user_id, "title": f"D{i}", "content": f"body {uuid.uuid4()}"},
        )

    r = await client.get(f"/users/{user_id}/documents?page=2&page_size=1")
    data = r.json()
    assert data["page"] == 2
    assert data["page_size"] == 1
    assert data["total"] == 3
    assert len(data["items"]) == 1
    assert data["has_next"] is True
