"""Tests use unique payloads and real PostgreSQL/Redis/RabbitMQ via Compose."""
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
import pytest
import redis
from fastapi.testclient import TestClient
from app import config, engine
from app.api import app
from app.db import connect
from app.services import broker, publish_one, cleanup, reconcile, record_failure, QUEUES


def payload():
    return {"test": str(uuid.uuid4()), "value": [1, True]}


def row(sid):
    with connect() as conn:
        return conn.execute("SELECT * FROM submissions WHERE id=%s", (sid,)).fetchone()


def decisions(ids):
    return [row(sid)["decision"] for sid in ids]


def test_api_validation_and_durable_submission():
    client = TestClient(app)
    for raw in ('[]', 'null', '{"x":NaN}', '{"x":1e400}', 'broken'):
        assert client.post("/events", content=raw).status_code == 422
    response = client.post("/events", json=payload())
    assert response.status_code == 202
    sid = response.json()["submission_id"]
    assert client.get(f"/events/{sid}").json()["decision"] == "pending"
    with connect() as conn:
        assert conn.execute("SELECT 1 FROM outbox WHERE submission_id=%s", (sid,)).fetchone()


def test_http_retry_preserves_submission_and_arrival():
    client = TestClient(app)
    p = payload()
    headers = {"Idempotency-Key": str(uuid.uuid4())}
    first = client.post("/events", json=p, headers=headers).json()
    second = client.post("/events", json=p, headers=headers).json()
    assert first == second
    assert client.post("/events", json={"different": True}, headers=headers).status_code == 409
    sid = first["submission_id"]
    before = row(sid)["arrival_ms"]
    engine.process(sid)
    client.post("/events", json=p, headers=headers)
    assert row(sid)["arrival_ms"] == before


def test_real_redis_expiry_and_new_acceptance(monkeypatch):
    monkeypatch.setattr(config, "WINDOW_MS", 150)
    p = payload()
    sid = engine.submit(p)
    engine.process(sid)
    key = "dedupe:" + engine.fingerprint(p)
    assert 0 < engine.cache.pttl(key) <= 150
    time.sleep(0.2)
    assert engine.cache.get(key) is None
    second = engine.submit(p)
    engine.process(second)
    assert decisions([sid, second]) == ["unique", "unique"]


def test_sliding_expiry_order_and_retry():
    p = payload()
    start = int(time.time() * 1000)
    ids = [engine.submit(p, start), engine.submit(p, start + 3000000),
           engine.submit(p, start + 6599999), engine.submit(p, start + 10199999)]
    # Processing only the last message must drain predecessors in arrival order.
    engine.process(ids[-1])
    assert decisions(ids) == ["unique", "duplicate", "duplicate", "unique"]
    fp = engine.fingerprint(p)
    before = engine.cache.get("dedupe:" + fp)
    engine.process(ids[0])
    assert engine.cache.get("dedupe:" + fp) == before
    assert int(before) == start + 13799999


def test_concurrent_identical_submissions():
    p = payload()
    with ThreadPoolExecutor(max_workers=8) as executor:
        ids = list(executor.map(lambda _: engine.submit(p), range(16)))
        list(executor.map(engine.process, reversed(ids)))
    assert decisions(ids).count("unique") == 1
    assert decisions(ids).count("duplicate") == 15


def test_false_positive_and_redis_outage(monkeypatch):
    p = payload()
    monkeypatch.setattr(engine.bloom, "contains", lambda _: True)
    monkeypatch.setattr(engine.cache, "eval", lambda *args: (_ for _ in ()).throw(redis.ConnectionError("offline")))
    a, b = engine.submit(p), engine.submit(p)
    engine.process(b)
    assert decisions([a, b]) == ["unique", "duplicate"]
    assert row(a)["redis_available"] is False


def test_redis_flush_recovery():
    p = payload()
    sid = engine.submit(p)
    engine.process(sid)
    key = "dedupe:" + engine.fingerprint(p)
    engine.cache.delete(key)
    reconcile()
    assert engine.cache.pttl(key) > 0
    second = engine.submit(p)
    engine.process(second)
    assert row(second)["decision"] == "duplicate"


def test_old_queue_arrivals_do_not_create_fresh_redis_ttl():
    p = payload()
    a = engine.submit(p, int(time.time() * 1000) - 7200000)
    engine.process(a)
    assert engine.cache.get("dedupe:" + engine.fingerprint(p)) is None
    b = engine.submit(p)
    engine.process(b)
    assert decisions([a, b]) == ["unique", "unique"]


def test_database_rollback_after_redis_write(monkeypatch):
    p = payload()
    sid = engine.submit(p)
    original = engine.add_outbox
    monkeypatch.setattr(engine, "add_outbox", lambda *args: (_ for _ in ()).throw(RuntimeError("crash before commit")))
    with pytest.raises(RuntimeError):
        engine.process(sid)
    assert row(sid)["decision"] == "pending"
    monkeypatch.setattr(engine, "add_outbox", original)
    engine.process(sid)
    assert row(sid)["decision"] == "unique"
    assert engine.consume({"acceptance_id": sid, "payload": p})
    assert not engine.consume({"acceptance_id": sid, "payload": p})


def test_cleanup_keeps_unfinished_logs_and_removes_finished():
    p = payload()
    old = int(time.time() * 1000) - 8 * 86400000
    sid = engine.submit(p, old)
    engine.process(sid)
    cleanup()
    assert row(sid)
    engine.consume({"acceptance_id": sid})
    with connect() as conn:
        conn.execute("UPDATE outbox SET published_at=clock_timestamp() WHERE submission_id=%s", (sid,))
    cleanup()
    assert row(sid) is None


def test_terminal_failure_creates_durable_dead_letter():
    sid = engine.submit(payload())
    for _ in range(config.MAX_ATTEMPTS):
        terminal = record_failure(sid, RuntimeError("poison message"), "worker")
    assert terminal
    assert row(sid)["decision"] == "dead_letter"
    with connect() as conn:
        assert conn.execute("SELECT * FROM outbox WHERE submission_id=%s AND kind='dead'", (sid,)).fetchone()


def test_publisher_confirms_and_repeated_delivery():
    connection, channel = broker()
    try:
        sid = engine.submit(payload())
        engine.process(sid)
        # Services should be stopped when tests run; drain the database outbox.
        while publish_one(channel):
            pass
        with connect() as conn:
            assert conn.execute("SELECT count(*) AS n FROM outbox WHERE submission_id=%s AND published_at IS NULL", (sid,)).fetchone()["n"] == 0
        engine.consume({"acceptance_id": sid})
        assert not engine.consume({"acceptance_id": sid})
    finally:
        connection.close()


def test_crash_after_publish_confirmation_before_outbox_commit():
    import json
    connection, channel = broker()
    try:
        while publish_one(channel):
            pass
        sid = engine.submit(payload())
        engine.process(sid)
        assert publish_one(channel)  # input notification

        class CrashAfterConfirmation:
            def basic_publish(self, **kwargs):
                channel.basic_publish(**kwargs)
                raise RuntimeError("crash after broker confirmation")

        with pytest.raises(RuntimeError):
            publish_one(CrashAfterConfirmation())
        with connect() as conn:
            assert conn.execute("SELECT * FROM outbox WHERE submission_id=%s AND kind='output' AND published_at IS NULL", (sid,)).fetchone()
        assert publish_one(channel)
        copies = 0
        while True:
            method, properties, body = channel.basic_get(QUEUES["output"], auto_ack=False)
            if method is None:
                break
            if json.loads(body)["acceptance_id"] == sid:
                copies += 1
            channel.basic_ack(method.delivery_tag)
        assert copies == 2
        assert engine.consume({"acceptance_id": sid})
        assert not engine.consume({"acceptance_id": sid})
    finally:
        connection.close()
