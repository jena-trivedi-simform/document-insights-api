"""
Manual test runner for Document Insights API.
Run with: python3 run_tests.py
The API must be running at http://localhost:8000 (docker compose up).
"""

import json
import time
import requests

BASE = "http://localhost:8000"
PASS = "✓ PASS"
FAIL = "✗ FAIL"


def _explain(text):
    print(f"  💡 {text}")


def _print_result(label, response, expected_status, extra=None):
    ok = response.status_code == expected_status
    mark = PASS if ok else FAIL
    print(f"\n{'='*60}")
    print(f"{mark}  {label}")
    print(f"{'='*60}")
    print(f"  Status : {response.status_code} (expected {expected_status})")
    try:
        body = response.json()
        print(f"  Body   : {json.dumps(body, indent=4, default=str)}")
    except Exception:
        print(f"  Body   : {response.text}")
    if extra:
        print(f"  Note   : {extra}")
    return ok


def _poll_until_done(document_id, timeout=60):
    """Poll GET /documents/{id} until status is completed or failed."""
    print(f"\n  [polling document {document_id}]")
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{BASE}/documents/{document_id}")
        status = r.json().get("status")
        print(f"    → status: {status}")
        if status in ("completed", "failed"):
            return r
        time.sleep(3)
    raise TimeoutError(f"Document {document_id} did not complete within {timeout}s")


def run_all():
    results = []

    # Unique suffix per run — prevents content from hitting Redis cache left by a previous run.
    # Without this, tests 2 and 5 would get instant cache hits and never show queued→completed flow.
    run_id = str(int(time.time()))
    alice = f"test_alice_{run_id}"
    bob   = f"test_bob_ratelimit_{run_id}"

    print(f"\n  Run ID: {run_id}  (fresh user IDs and content per run)")

    # ──────────────────────────────────────────────────────────────────────────
    # 1. Health check
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 1: Health Check")
    _explain("Verifies that the API, MongoDB, and Redis are all reachable and healthy.")
    _explain("Expected: 200 with status='healthy' for both services.")
    r = requests.get(f"{BASE}/health")
    ok = _print_result("GET /health — all services healthy", r, 200)
    results.append(("Health Check", ok))

    # ──────────────────────────────────────────────────────────────────────────
    # 2. Submit a document
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 2: Submit Document")
    _explain("Submits a new document for async processing.")
    _explain("The API should accept it immediately and return status='queued'.")
    _explain("The background worker will pick it up and process it asynchronously.")
    payload = {
        "user_id": alice,
        "title": "Q3 Report",
        "content": f"This quarter saw significant growth across all product lines including APAC and EMEA regions. run={run_id}",
    }
    r = requests.post(f"{BASE}/documents", json=payload)
    ok = _print_result("POST /documents — document queued", r, 201)
    results.append(("Submit Document", ok))
    doc_id = r.json().get("document_id") if ok else None

    # ──────────────────────────────────────────────────────────────────────────
    # 3. Poll until completed
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 3: Poll Document Until Completed")
    _explain("Polls GET /documents/{id} every 3 seconds to observe the status transition.")
    _explain("Status flow: queued → processing → completed.")
    _explain("Once completed, the 'summary' field is populated by the worker.")
    if doc_id:
        r = _poll_until_done(doc_id)
        ok = _print_result("GET /documents/{id} — status completed", r, 200,
                           extra="summary field should be populated")
        results.append(("Poll Until Completed", ok and r.json().get("status") == "completed"))
    else:
        print("  SKIPPED — no document_id from previous test")
        results.append(("Poll Until Completed", False))

    # ──────────────────────────────────────────────────────────────────────────
    # 4. Cache hit — same content, different title
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 4: Cache Hit (identical content)")
    _explain("Submits the exact same content as Test 2 (different title, same body).")
    _explain("The API hashes the content and checks Redis before queuing.")
    _explain("A cache hit skips the worker entirely — response is instant with status='completed'.")
    payload_cached = {
        "user_id": alice,
        "title": "Q3 Report - Duplicate",
        "content": f"This quarter saw significant growth across all product lines including APAC and EMEA regions. run={run_id}",
    }
    start = time.time()
    r = requests.post(f"{BASE}/documents", json=payload_cached)
    elapsed = time.time() - start
    body = r.json()
    cache_hit = body.get("status") == "completed"
    ok = _print_result(
        "POST /documents — cache hit returns completed instantly",
        r, 201,
        extra=f"Response time: {elapsed:.3f}s | Cache hit: {cache_hit}"
    )
    results.append(("Cache Hit", ok and cache_hit))

    # ──────────────────────────────────────────────────────────────────────────
    # 5. Rate limiting — 429 on 4th active document
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 5: Rate Limiting (429 on 4th document)")
    _explain("Each user is allowed at most 3 documents in queued/processing state at once.")
    _explain("A Lua script in Redis atomically checks and increments the counter.")
    _explain("The 4th submission must be rejected with 429 Too Many Requests.")
    submitted = []
    for i in range(1, 4):
        rp = requests.post(f"{BASE}/documents", json={
            "user_id": bob,
            "title": f"Doc {i}",
            "content": f"Unique content for rate limit test document number {i} run={run_id} — {'x' * 50}",
        })
        submitted.append(rp.status_code)
        print(f"  Doc {i}: status {rp.status_code}")

    r4 = requests.post(f"{BASE}/documents", json={
        "user_id": bob,
        "title": "Doc 4 — should be rejected",
        "content": f"Unique content for rate limit test document number 4 run={run_id} — {'y' * 50}",
    })
    ok = _print_result("POST /documents — 4th doc rate limited", r4, 429,
                       extra=f"First 3 submissions: {submitted}")
    results.append(("Rate Limiting (429)", ok))

    # ──────────────────────────────────────────────────────────────────────────
    # 6. List user documents with pagination
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 6: List User Documents")
    _explain("Fetches a paginated list of all documents for a user.")
    _explain("Results are sorted by created_at descending (newest first).")
    _explain("Response includes total count, current page, and has_next flag.")
    r = requests.get(f"{BASE}/users/{alice}/documents", params={"page": 1, "page_size": 10})
    ok = _print_result("GET /users/{user_id}/documents — paginated list", r, 200,
                       extra="Should contain documents submitted in tests 2 and 4")
    results.append(("List User Documents", ok))

    # 6b. Filter by status
    print("\n\n>>> TEST 6b: List Documents Filtered by Status")
    _explain("Same endpoint but with ?status=completed query param.")
    _explain("Only documents whose status matches the filter are returned.")
    r = requests.get(f"{BASE}/users/{alice}/documents",
                     params={"status": "completed"})
    ok = _print_result("GET /users/{user_id}/documents?status=completed", r, 200)
    results.append(("List Docs Filtered by Status", ok))

    # ──────────────────────────────────────────────────────────────────────────
    # 7. 404 — invalid document ID
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 7: 404 — Invalid Document ID")
    _explain("Passes a non-ObjectId string as the document ID.")
    _explain("The API validates the ID format before querying MongoDB.")
    _explain("Expected: 404 Not Found (not a 500 crash).")
    r = requests.get(f"{BASE}/documents/notavalidid")
    ok = _print_result("GET /documents/notavalidid — 404 not found", r, 404)
    results.append(("404 Invalid ID", ok))

    # ──────────────────────────────────────────────────────────────────────────
    # 8. 422 — missing required field
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 8: 422 — Missing Required Field")
    _explain("Submits a document without the required 'content' field.")
    _explain("FastAPI/Pydantic validates the request body and rejects it before it reaches business logic.")
    _explain("Expected: 422 Unprocessable Entity with a field-level error detail.")
    r = requests.post(f"{BASE}/documents", json={"user_id": "test_alice", "title": "No content"})
    ok = _print_result("POST /documents without content — 422 validation error", r, 422)
    results.append(("422 Missing Field", ok))

    # ──────────────────────────────────────────────────────────────────────────
    # 9. 422 — empty content string
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 9: 422 — Empty Content String")
    _explain("Submits content='' which violates the min_length=1 constraint on the schema.")
    _explain("An empty string is present but invalid — different from a missing field.")
    _explain("Expected: 422 Unprocessable Entity.")
    r = requests.post(f"{BASE}/documents",
                      json={"user_id": "test_alice", "title": "Empty", "content": ""})
    ok = _print_result("POST /documents with empty content — 422 validation error", r, 422)
    results.append(("422 Empty Content", ok))

    # ──────────────────────────────────────────────────────────────────────────
    # 10. Empty list for unknown user
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n>>> TEST 10: Empty List for Unknown User")
    _explain("Queries documents for a user who has never submitted anything.")
    _explain("The API should return 200 with an empty items list and total=0.")
    _explain("Expected: 200 (not 404) — an empty list is a valid response.")
    r = requests.get(f"{BASE}/users/unknown_user_xyz/documents")
    body = r.json()
    ok = r.status_code == 200 and body.get("total") == 0
    _print_result("GET /users/unknown_user/documents — empty list", r, 200,
                  extra=f"total={body.get('total')} (expected 0)")
    results.append(("Empty List Unknown User", ok))

    # ──────────────────────────────────────────────────────────────────────────
    # Summary
    # ──────────────────────────────────────────────────────────────────────────
    print("\n\n" + "="*60)
    print("  TEST SUMMARY")
    print("="*60)
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    for name, ok in results:
        mark = PASS if ok else FAIL
        print(f"  {mark}  {name}")
    print(f"\n  Result: {passed}/{total} passed")
    print("="*60 + "\n")


if __name__ == "__main__":
    run_all()
