import json
from uuid import UUID
from fastapi import FastAPI, HTTPException, Request
from app.db import connect
from app.engine import submit, IdempotencyConflict
from app.core import canonicalize

app = FastAPI(title="Sliding-window deduplication engine")


@app.post("/events", status_code=202, openapi_extra={"requestBody": {
    "required": True, "content": {"application/json": {"schema": {"type": "object", "additionalProperties": True}}}}})
async def post_event(request: Request):
    try:
        raw = await request.body()
        payload = json.loads(raw, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        if not isinstance(payload, dict):
            raise ValueError("Expected a JSON object")
        canonicalize(payload)
    except (ValueError, UnicodeDecodeError):
        raise HTTPException(422, "Body must be a valid, finite JSON object")
    # Run blocking storage work outside the ASGI event loop.
    from starlette.concurrency import run_in_threadpool
    key = request.headers.get("Idempotency-Key")
    if key is not None and (not key.strip() or len(key) > 200):
        raise HTTPException(422, "Idempotency-Key must contain 1 to 200 characters")
    try:
        sid = await run_in_threadpool(submit, payload, idempotency_key=key)
    except IdempotencyConflict as exc:
        raise HTTPException(409, str(exc))
    except Exception:
        raise HTTPException(503, "Durable storage unavailable; submission was not acknowledged")
    return {"submission_id": sid, "status": "pending"}


@app.get("/events/{submission_id}")
def get_event(submission_id: UUID):
    with connect() as conn:
        row = conn.execute("SELECT * FROM submissions WHERE id=%s", (submission_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Submission not found")
    return row


@app.get("/health")
def health():
    with connect() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    with connect() as conn:
        rows = conn.execute("SELECT decision,count(*) AS count FROM submissions GROUP BY decision").fetchall()
        pending = conn.execute("SELECT count(*) AS count FROM outbox WHERE published_at IS NULL").fetchone()
    return {"decisions": {r["decision"]: r["count"] for r in rows}, "pending_outbox": pending["count"]}
