# Document Insights API

Async document processing service built with FastAPI, MongoDB, and Redis.  
Documents are accepted via REST, queued for background processing, and retrievable once a mock summary is generated.

---

## Quick start

```bash
cp .env.example .env
docker-compose up --build
```

The API is available at `http://localhost:8000`.  
Interactive docs: `http://localhost:8000/docs`.

---

## API reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/documents` | Submit a document for processing |
| `GET` | `/documents/{id}` | Poll status (and retrieve summary when done) |
| `GET` | `/users/{user_id}/documents` | Paginated document list with optional `status` filter |
| `GET` | `/health` | Service health (MongoDB + Redis) |

### POST /documents

```json
// Request
{ "user_id": "alice", "title": "Q3 Report", "content": "..." }

// Response 201
{ "document_id": "664a...", "status": "queued", "created_at": "..." }
```

**Status codes**
- `201` — document accepted and queued  
- `201` — identical content (by hash) already processed → returns `status: completed` immediately  
- `409` — two identical submissions arrived simultaneously (retry in a moment)  
- `422` — validation error (missing/invalid fields)  
- `429` — rate limit: user already has 3 active documents  

### GET /documents/{id}

Status transitions: `queued → processing → completed | failed`

```json
{
  "document_id": "664a...",
  "status": "completed",
  "summary": "Summary of 'Q3 Report': 142-word document ...",
  "created_at": "...",
  "completed_at": "..."
}
```

### GET /users/{user_id}/documents

Query parameters: `page` (default 1), `page_size` (default 20, max 100), `status` (optional filter).

---

## Features implemented

| Features | Implementation |
|-------|---------------|
| `GET /health` | Pings MongoDB and Redis; returns `503` if either is unhealthy |
| Retry with backoff | Exponential backoff (`base * 2^attempt`); `retry_after` field keeps failed jobs invisible to the worker until the window expires |
| Concurrent duplicate submissions | `SET NX` Redis lock prevents two simultaneous identical submissions from both enqueueing; second caller gets `409 Conflict` |
| Integration tests (pytest) | `tests/test_documents.py`, `test_health.py`, `test_integration_extended.py` — run against a live MongoDB + Redis via `AsyncClient` |
| `.env.example` | All 13 config variables documented with inline comments |

---

## Production readiness

### Structured logging

Every log line is a JSON object emitted to stdout — machine-parseable by any log aggregator (Datadog, CloudWatch, Loki):

```json
{"timestamp": "2026-06-23T04:07:43", "level": "INFO", "logger": "app.services.worker", "message": "Worker started — poll interval 2.0s"}
```

Exceptions include a full `exception` array with traceback frames.

### Environment-based configuration

All tuneable values are read from environment variables via Pydantic `BaseSettings` (`app/config.py`). No hardcoded connection strings anywhere in the codebase. The `.env.example` documents every variable with its default and purpose.

### Dependency injection

Database and Redis connections are exposed as FastAPI dependencies (`Depends(get_db)`, `Depends(get_redis)`). This keeps routers stateless, makes connections easy to mock in tests, and ensures connections are created once at startup — not per request.

### Error handling

- Pydantic validation runs before any business logic; invalid requests get `422` with field-level detail automatically.
- `ObjectId.is_valid()` is checked before any MongoDB query; malformed IDs return `404` (not `500`).
- Redis failures in `check_and_increment` and `decrement` are caught and logged; rate limiting fails open rather than taking the API down.
- The worker catches all exceptions per job, marks the document `failed` (or schedules a retry), and continues — it never crashes the process.

### Crash recovery

On startup the app:
1. Resets any document stuck in `processing` back to `queued` (previous instance died mid-job).
2. Resyncs Redis rate-limit counters from MongoDB to correct any counter inflation from a prior crash.

### Health check

`GET /health` independently pings both MongoDB and Redis and returns per-service status. Returns `200` when fully healthy, `503` when either service is degraded — suitable for a load-balancer readiness probe.

---

## Design decisions

### Background worker: asyncio Task vs Celery

I chose a single `asyncio.create_task(worker_loop())` launched inside the FastAPI lifespan context.

**Why not Celery?** Celery requires a separate broker process (Redis or RabbitMQ acting as a task queue, separate from the Redis we use for caching) and a separate worker process.  For a single-process service at this scale that is unnecessary complexity.  The asyncio task runs in the same event loop as the API, shares the same connections, and shuts down cleanly when the server stops.

**The trade-off:** A single asyncio worker processes one document at a time.  With `asyncio.sleep` yielding control, the API remains fully responsive while waiting.  The throughput ceiling is low — for production I'd move to Celery + multiple replicas (see [What I'd do differently](#what-id-do-differently)).

### Race condition prevention in the worker: `find_one_and_update`

The worker uses a single MongoDB operation to both *find* a queued document and *mark it as processing*:

```python
doc = await db.documents.find_one_and_update(
    {"status": "queued"},
    {"$set": {"status": "processing", ...}},
    sort=[("created_at", 1)],   # FIFO
    return_document=True,
)
```

MongoDB's document-level locking guarantees that only one caller wins this race, even if multiple worker instances call it simultaneously.  The "loser" simply gets `None` and sleeps.  No application-level locks, no distributed mutexes.

### Rate limiting: atomic Lua script

A naive Python check-then-increment:

```python
count = await redis.get(key)   # reads 2
if count < 3:
    await redis.incr(key)      # another request does the same thing here — both succeed at 2
```

…has a TOCTOU race.  Two concurrent requests both read `2 < 3` and both increment, leaving the counter at `4` when it should be `3`.

**Fix:** a Lua script that the Redis server executes atomically.  No other command can interleave between the read and the write:

```lua
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current >= tonumber(ARGV[1]) then return 0 end
redis.call('INCR', KEYS[1])
return 1
```

**Counter drift recovery:** if the process crashes after Redis is incremented but before MongoDB is written, the counter is inflated.  On startup `resync_from_db()` aggregates the true count of queued/processing documents per user from MongoDB and resets the Redis counters.

**Graceful degradation:** if Redis is unreachable, both `check_and_increment` and `decrement` log a warning and continue (fail-open).  Rate limiting becomes temporarily unenforced, which is preferable to making the entire API unavailable because Redis had a blip.

### Content caching: per-user scope

Cache key: `content_cache:{user_id}:{sha256_of_content}`.

**Scope decision — per-user, not global.**  A global cache keyed only on content hash would also work technically, but it introduces a subtle problem: user B submitting the same text as user A would receive a `completed` document in their history whose summary was generated from user A's title — leaking cross-user information. 

**Cache hit flow:** hash the incoming content → check Redis → if hit, write a completed document to MongoDB immediately and return `status: completed`.  No worker slot consumed, sub-millisecond response.

**Concurrent duplicate submissions:**  If two requests for `(user_id, content_hash)` arrive simultaneously, both will miss the cache.  A `SET NX` lock prevents both from enqueueing two jobs.  The second caller gets a `409 Conflict` and should retry; by then the first caller's document record exists.

### MongoDB schema and indexes

```
documents collection
  _id             ObjectId
  user_id         str
  title           str
  content         str
  content_hash    str  (SHA-256 hex)
  status          str  (queued | processing | completed | failed)
  summary         str | null
  error_message   str | null
  retry_count     int
  retry_after     datetime | null  (earliest time the worker may retry)
  created_at      datetime
  updated_at      datetime
  processing_started_at  datetime | null
  completed_at    datetime | null
```

Indexes created on startup (idempotent):

| Index | Purpose |
|-------|---------|
| `(status, created_at)` | Worker FIFO poll for queued docs |
| `(user_id, status)` | Rate-limit aggregate + filtered listing |
| `(user_id, created_at desc)` | Paginated user listing |
| `(user_id, content_hash)` | Fallback dedup check without Redis |

### Retry with exponential backoff

When a job fails and the document still has retries remaining (`retry_count < MAX_RETRIES`), the worker resets its status back to `queued` and sets a `retry_after` timestamp:

```
backoff_seconds = RETRY_BACKOFF_BASE * 2^attempt
```

| Attempt | Delay |
|---------|-------|
| 1st retry | 5 s |
| 2nd retry | 10 s |
| 3rd retry | 20 s |
| Exhausted | permanent `failed` |

The worker's MongoDB query filters `retry_after <= now OR retry_after IS NULL`, so retried documents sit invisible to the worker until their backoff window expires — no busy-polling.  The rate-limit slot is held across retries (the document is still active); it is only released when the job permanently succeeds or fails.

### Crash recovery

On startup the application:
1. Resets any document stuck in `processing` back to `queued` (a previous instance crashed mid-job).
2. Resyncs Redis rate-limit counters from MongoDB (corrects inflated counters from prior crashes).

---

## Running tests

### Automated test suite

The suite has two layers:

| Layer | File | Needs services? |
|-------|------|-----------------|
| **Unit** | `tests/test_unit.py` | No — pure Python, no I/O |
| **Integration** | `tests/test_documents.py`, `test_health.py`, `test_integration_extended.py` | Yes — MongoDB + Redis |

```bash
# Unit tests only (no Docker required)
pytest tests/test_unit.py -v

# Full suite (start dependencies first)
docker-compose up -d mongodb redis
pytest -v
```

Integration tests use a separate `document_insights_test` database, collapse worker delays to 0–1 s, and set `WORKER_FAILURE_RATE=0` so results are deterministic.  Each test cleans up its own state via an `autouse` fixture.

---

### Manual test runner

`run_tests.py` is a self-contained script that exercises all major API behaviours against a live stack and prints a pass/fail summary.  Each test case prints an explanation of what is being tested and why before executing.

**Prerequisites:** the full stack must be running.

```bash
docker compose up -d
pip install requests      # if not already installed
python3 run_tests.py
```

**What it covers (11 test cases):**

| # | Test | Expected result |
|---|------|----------------|
| 1 | Health check | `200` — MongoDB and Redis both healthy |
| 2 | Submit document | `201 queued` — accepted for async processing |
| 3 | Poll until completed | `200 completed` — worker generates summary in 10–30 s |
| 4 | Cache hit (identical content) | `201 completed` instantly — Redis cache hit, no worker slot used |
| 5 | Rate limit — 4th active document | `429` — atomic Lua script enforces the 3-doc-per-user limit |
| 6 | List user documents (paginated) | `200` — newest-first, with `total` and `has_next` |
| 6b | List filtered by status | `200` — only documents matching `?status=completed` |
| 7 | Invalid document ID | `404` — non-ObjectId string handled gracefully |
| 8 | Missing required field | `422` — Pydantic rejects request before business logic runs |
| 9 | Empty content string | `422` — `min_length=1` constraint enforced |
| 10 | Unknown user document list | `200` with `total: 0` — empty list, not 404 |

Each run generates a unique `run_id` (Unix timestamp) appended to user IDs and content strings.  This prevents content cached from a previous run from producing false cache hits in tests 2–5.

**Sample output:**

```
>>> TEST 4: Cache Hit (identical content)
  💡 Submits the exact same content as Test 2 (different title, same body).
  💡 The API hashes the content and checks Redis before queuing.
  💡 A cache hit skips the worker entirely — response is instant with status='completed'.

============================================================
✓ PASS  POST /documents — cache hit returns completed instantly
============================================================
  Status : 201 (expected 201)
  Body   : { "document_id": "...", "status": "completed", ... }
  Note   : Response time: 0.013s | Cache hit: True

============================================================
  TEST SUMMARY
============================================================
  ✓ PASS  Health Check
  ✓ PASS  Submit Document
  ✓ PASS  Poll Until Completed
  ✓ PASS  Cache Hit
  ✓ PASS  Rate Limiting (429)
  ...
  Result: 11/11 passed
============================================================
```

---

## What I'd do differently with more time

**Celery for the worker** — horizontal scaling requires a proper task queue so multiple API replicas share a single work queue.  Celery + Redis broker is the natural fit; it also gives retries, task monitoring (Flower), and dead-letter queues out of the box.

**MongoDB transactions for rate-limit + insert** — currently there is a small window between `check_and_increment` (Redis) and `insert_one` (MongoDB) where a crash leaves the counter high.  `resync_from_db` heals this on next startup, but a multi-document MongoDB transaction plus removing the Redis counter would be fully consistent (at the cost of transaction overhead).

**Sliding-window rate limit** — the current counter counts *total* active jobs.  A sliding window (e.g. "no more than N submissions per minute") would provide better abuse protection.

**Authentication** — `user_id` is caller-supplied with no verification.  A real service would issue JWTs and derive `user_id` from the token, preventing users from submitting under another user's ID.

**Observability** — add request-ID propagation through log lines (currently each log has `logger` and `message` but no correlation ID tying a request's logs together), and instrument with Prometheus counters (queue depth, processing latency, cache hit rate).

**Content hash scoping** — currently only content is hashed, not the title.  Submitting "same body, different title" returns the cached summary labelled with the original title.  Including `(user_id, title, content)` in the hash would avoid this, at the cost of fewer cache hits.

---

## Assumptions

- `user_id` is a trusted string provided by the caller (no auth in scope).
- "Identical content" means byte-identical after UTF-8 encoding (whitespace differences produce different hashes).
- A global cache would leak one user's document title into another user's history so cache is implemented per users.
- `completed_at` is set when status transitions to `completed` or `failed`.
