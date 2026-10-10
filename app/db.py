import psycopg
from psycopg.rows import dict_row
from app.config import DATABASE_URL

SCHEMA = """
CREATE TABLE IF NOT EXISTS fingerprints (
 fingerprint TEXT PRIMARY KEY, last_arrival_ms BIGINT NOT NULL DEFAULT 0,
 expires_ms BIGINT, version BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS submissions (
 seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 id UUID UNIQUE NOT NULL, fingerprint TEXT NOT NULL,
 payload JSONB NOT NULL, arrival_ms BIGINT NOT NULL,
 decision TEXT NOT NULL DEFAULT 'pending' CHECK (decision IN ('pending','unique','duplicate','dead_letter')),
 delivery_status TEXT NOT NULL DEFAULT 'pending',
 bloom_positive BOOLEAN, redis_available BOOLEAN,
 attempts INTEGER NOT NULL DEFAULT 0, error TEXT
);
CREATE INDEX IF NOT EXISTS submission_order ON submissions(fingerprint, seq);
CREATE INDEX IF NOT EXISTS submission_age ON submissions(arrival_ms);
CREATE TABLE IF NOT EXISTS api_requests (
 token TEXT PRIMARY KEY, submission_id UUID NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
 fingerprint TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
 id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 submission_id UUID NOT NULL REFERENCES submissions(id) ON DELETE CASCADE,
 kind TEXT NOT NULL CHECK (kind IN ('input','output','dead')),
 body JSONB NOT NULL, published_at TIMESTAMPTZ,
 UNIQUE(submission_id, kind)
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(id) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS consumed (
 acceptance_id UUID PRIMARY KEY REFERENCES submissions(id) ON DELETE CASCADE,
 consumed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
"""


def connect():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def lock(conn, fp):
    # All ingestion, processing, reconciliation, and state cleanup share this lock.
    conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (fp,))


def initialize():
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(71289012)")
        conn.execute(SCHEMA)


if __name__ == "__main__":
    initialize()
