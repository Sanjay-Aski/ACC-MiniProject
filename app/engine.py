import logging
import uuid
import redis
from psycopg.types.json import Jsonb
from app import config
from app.core import fingerprint, decision, RotatingBloom
from app.db import connect, lock

log = logging.getLogger(__name__)
cache = redis.Redis.from_url(config.REDIS_URL, socket_connect_timeout=1, socket_timeout=1)
bloom = RotatingBloom()

# Redis is repaired from the locked PostgreSQL state before applying the arrival.
# A previous Redis write may have survived a rolled-back PostgreSQL transaction.
REFRESH = """
local old = redis.call('GET', KEYS[1])
local previous = tonumber(ARGV[1])
local arrival = tonumber(ARGV[2])
local expiry = tonumber(ARGV[3])
local version = tonumber(ARGV[4])
local t = redis.call('TIME')
local now = tonumber(t[1])*1000 + math.floor(tonumber(t[2])/1000)
local duplicate = 0
if previous > arrival then duplicate = 1 end
if expiry > now then
 redis.call('SET', KEYS[1], tostring(expiry), 'PXAT', expiry)
else
 redis.call('DEL', KEYS[1])
end
return {duplicate, old or '', version}
"""


def add_outbox(conn, submission_id, kind, body):
    conn.execute("INSERT INTO outbox(submission_id,kind,body) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                 (submission_id, kind, Jsonb(body)))


class IdempotencyConflict(ValueError):
    pass


def submit(payload, arrival_ms=None, idempotency_key=None):
    fp = fingerprint(payload)
    sid = uuid.uuid4()
    with connect() as conn:
        if idempotency_key:
            lock(conn, "request:" + idempotency_key)
            existing = conn.execute("SELECT * FROM api_requests WHERE token=%s", (idempotency_key,)).fetchone()
            if existing:
                if existing["fingerprint"] != fp:
                    raise IdempotencyConflict("Idempotency-Key was used with different content")
                return str(existing["submission_id"])
        lock(conn, fp)
        conn.execute("INSERT INTO fingerprints(fingerprint) VALUES (%s) ON CONFLICT DO NOTHING", (fp,))
        state = conn.execute("SELECT * FROM fingerprints WHERE fingerprint=%s FOR UPDATE", (fp,)).fetchone()
        if arrival_ms is None:
            arrival_ms = conn.execute("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint AS ms").fetchone()["ms"]
        # Monotonic under a per-fingerprint lock, including equal-millisecond arrivals.
        arrival_ms = max(arrival_ms, state["last_arrival_ms"])
        conn.execute("UPDATE fingerprints SET last_arrival_ms=%s WHERE fingerprint=%s", (arrival_ms, fp))
        conn.execute("INSERT INTO submissions(id,fingerprint,payload,arrival_ms) VALUES (%s,%s,%s,%s)",
                     (sid, fp, Jsonb(payload), arrival_ms))
        add_outbox(conn, sid, "input", {"submission_id": str(sid)})
        if idempotency_key:
            conn.execute("INSERT INTO api_requests(token,submission_id,fingerprint) VALUES (%s,%s,%s)",
                         (idempotency_key, sid, fp))
    return str(sid)


def process(sid):
    # Find the target, then drain its predecessors one transaction at a time.
    with connect() as conn:
        target = conn.execute("SELECT fingerprint,seq FROM submissions WHERE id=%s", (sid,)).fetchone()
    if not target:
        return  # A late broker retry can outlive the retained database log.
    while True:
        with connect() as conn:
            lock(conn, target["fingerprint"])
            state = conn.execute("SELECT * FROM fingerprints WHERE fingerprint=%s FOR UPDATE", (target["fingerprint"],)).fetchone()
            row = conn.execute("SELECT * FROM submissions WHERE fingerprint=%s AND seq<=%s AND decision='pending' ORDER BY seq LIMIT 1 FOR UPDATE",
                               (target["fingerprint"], target["seq"])).fetchone()
            if not row:
                return
            result = decision(state["expires_ms"], row["arrival_ms"])
            expiry = row["arrival_ms"] + config.WINDOW_MS
            positive = bloom.contains(row["fingerprint"])
            available = True
            try:
                cache.eval(REFRESH, 1, "dedupe:" + row["fingerprint"],
                           state["expires_ms"] or 0, row["arrival_ms"], expiry, state["version"] + 1)
            except redis.RedisError:
                available = False
                log.warning("Redis unavailable; durable state decides submission %s", row["id"])
            conn.execute("UPDATE fingerprints SET expires_ms=%s,version=version+1 WHERE fingerprint=%s", (expiry, row["fingerprint"]))
            conn.execute("UPDATE submissions SET decision=%s,delivery_status=%s,bloom_positive=%s,redis_available=%s,error=NULL WHERE id=%s",
                         (result, "awaiting_output" if result == "unique" else "not_applicable", positive, available, row["id"]))
            if result == "unique":
                add_outbox(conn, row["id"], "output", {
                    "acceptance_id": str(row["id"]), "fingerprint": row["fingerprint"],
                    "arrival_ms": row["arrival_ms"], "payload": row["payload"]})
        bloom.add(row["fingerprint"], row["arrival_ms"])
        log.info("submission=%s decision=%s bloom_positive=%s redis_available=%s", row["id"], result, positive, available)


def consume(body):
    with connect() as conn:
        inserted = conn.execute("INSERT INTO consumed(acceptance_id) VALUES (%s) ON CONFLICT DO NOTHING RETURNING acceptance_id",
                                (body["acceptance_id"],)).fetchone()
        conn.execute("UPDATE submissions SET delivery_status='consumed' WHERE id=%s", (body["acceptance_id"],))
    if inserted:
        log.info("Accepted message: %s", body)
    return bool(inserted)
