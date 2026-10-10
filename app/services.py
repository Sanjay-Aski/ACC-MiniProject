import json
import logging
import sys
import time
import pika
import redis
from app import config
from app.db import connect, lock
from app.engine import process, consume, cache, add_outbox

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)
QUEUES = {kind: config.QUEUE_PREFIX + name for kind, name in
          {"input": "incoming-events", "output": "unique-events", "dead": "dead-events"}.items()}


def broker():
    params = pika.URLParameters(config.RABBITMQ_URL)
    params.heartbeat = 120
    params.blocked_connection_timeout = 30
    params.socket_timeout = 10
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    dead_exchange = config.QUEUE_PREFIX + "dedup-dead"
    channel.exchange_declare(exchange=dead_exchange, exchange_type="fanout", durable=True)
    for queue in QUEUES.values():
        channel.queue_declare(queue=queue, durable=True)
    channel.queue_bind(queue=QUEUES["dead"], exchange=dead_exchange)
    for queue in (QUEUES["input"], QUEUES["output"]):
        channel.queue_declare(queue=queue + "-retry", durable=True, arguments={
            "x-message-ttl": 5000, "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": queue})
    channel.confirm_delivery()
    channel.basic_qos(prefetch_count=1)
    return connection, channel


def publish(channel, queue, body, headers=None):
    channel.basic_publish(exchange="", routing_key=queue, body=json.dumps(body), mandatory=True,
                          properties=pika.BasicProperties(delivery_mode=2, content_type="application/json", headers=headers or {}))


def publish_one(channel):
    with connect() as conn:
        row = conn.execute("SELECT * FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED").fetchone()
        if not row:
            return False
        publish(channel, QUEUES[row["kind"]], row["body"])
        # A crash before this commit causes a safe repeat with the same IDs.
        conn.execute("UPDATE outbox SET published_at=clock_timestamp() WHERE id=%s", (row["id"],))
        if row["kind"] == "output":
            conn.execute("UPDATE submissions SET delivery_status='published' WHERE id=%s AND delivery_status='awaiting_output'", (row["submission_id"],))
    return True


def record_failure(sid, error, role):
    with connect() as conn:
        target = conn.execute("SELECT * FROM submissions WHERE id=%s", (sid,)).fetchone()
        if not target:
            return True
        lock(conn, target["fingerprint"])
        if role == "worker":
            # The failed operation may have concerned an earlier pending arrival.
            row = conn.execute("SELECT * FROM submissions WHERE fingerprint=%s AND seq<=%s AND decision='pending' ORDER BY seq LIMIT 1 FOR UPDATE",
                               (target["fingerprint"], target["seq"])).fetchone()
            if not row:
                return True
        else:
            row = conn.execute("SELECT * FROM submissions WHERE id=%s FOR UPDATE", (sid,)).fetchone()
            if row["delivery_status"] in ("consumed", "dead_letter"):
                return True
        attempts = row["attempts"] + 1
        conn.execute("UPDATE submissions SET attempts=%s,error=%s WHERE id=%s", (attempts, str(error)[:2000], row["id"]))
        if attempts >= config.MAX_ATTEMPTS:
            if role == "worker":
                conn.execute("UPDATE submissions SET decision='dead_letter',delivery_status='dead_letter' WHERE id=%s", (row["id"],))
            else:
                conn.execute("UPDATE submissions SET delivery_status='dead_letter' WHERE id=%s", (row["id"],))
            add_outbox(conn, row["id"], "dead", {"submission_id": str(row["id"]), "role": role,
                       "error": str(error)[:2000], "payload": row["payload"]})
            return str(row["id"]) == str(sid)
        return False


def callback(role):
    queue = QUEUES["input" if role == "worker" else "output"]
    def handle(channel, method, properties, raw):
        body = None
        try:
            body = json.loads(raw)
            sid = body["submission_id" if role == "worker" else "acceptance_id"]
            from uuid import UUID
            UUID(sid)
        except (ValueError, KeyError, TypeError):
            publish(channel, QUEUES["dead"], {"error": "Invalid broker envelope", "raw": raw.decode(errors="replace")})
            channel.basic_ack(method.delivery_tag)
            return
        try:
            (process if role == "worker" else consume)(body["submission_id"] if role == "worker" else body)
        except Exception as exc:
            log.exception("Delivery failed for %s", sid)
            try:
                terminal = record_failure(sid, exc, role)
                if not terminal:
                    publish(channel, queue + "-retry", body)
                channel.basic_ack(method.delivery_tag)
            except Exception:
                # If durable recording or retry publication failed, retain broker ownership.
                channel.basic_nack(method.delivery_tag, requeue=True)
                time.sleep(1)
            return
        channel.basic_ack(method.delivery_tag)
    return handle


def reconcile():
    with connect() as conn:
        states = conn.execute("SELECT fingerprint FROM fingerprints WHERE expires_ms IS NOT NULL").fetchall()
    for item in states:
        with connect() as conn:
            lock(conn, item["fingerprint"])
            state = conn.execute("SELECT * FROM fingerprints WHERE fingerprint=%s", (item["fingerprint"],)).fetchone()
            if state:
                # Holding the same lock as the worker prevents stale repairs.
                cache.eval("""
                local t=redis.call('TIME')
                local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
                if tonumber(ARGV[1]) > now then
                  redis.call('SET',KEYS[1],ARGV[1],'PXAT',ARGV[1])
                else redis.call('DEL',KEYS[1]) end
                return 1
                """, 1, "dedupe:" + state["fingerprint"], state["expires_ms"])


def cleanup():
    with connect() as conn:
        # Never delete logs with pending publication, processing, or consumption.
        conn.execute("""DELETE FROM submissions s
          WHERE arrival_ms < floor(extract(epoch FROM clock_timestamp()-make_interval(days => %s))*1000)
          AND decision <> 'pending' AND delivery_status IN ('consumed','not_applicable','dead_letter')
          AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.submission_id=s.id AND o.published_at IS NULL)""", (config.RETENTION_DAYS,))
        candidates = conn.execute("SELECT fingerprint FROM fingerprints WHERE expires_ms < floor(extract(epoch FROM clock_timestamp())*1000)").fetchall()
    for row in candidates:
        with connect() as conn:
            lock(conn, row["fingerprint"])
            conn.execute("""DELETE FROM fingerprints f WHERE fingerprint=%s
              AND expires_ms < floor(extract(epoch FROM clock_timestamp())*1000)
              AND NOT EXISTS (SELECT 1 FROM submissions s WHERE s.fingerprint=f.fingerprint
                AND (s.decision='pending' OR s.delivery_status IN ('pending','awaiting_output','published')
                  OR EXISTS (SELECT 1 FROM outbox o WHERE o.submission_id=s.id AND o.published_at IS NULL)))""", (row["fingerprint"],))


def run(role):
    if role == "maintenance":
        last_cleanup = 0
        while True:
            try:
                if time.monotonic() - last_cleanup >= 3600:
                    cleanup()
                    last_cleanup = time.monotonic()
                reconcile()
            except Exception:
                log.exception("Maintenance will retry")
            time.sleep(30)
    while True:
        connection = None
        try:
            connection, channel = broker()
            if role == "publisher":
                while True:
                    worked = publish_one(channel)
                    connection.process_data_events(time_limit=0 if worked else 0.5)
            else:
                channel.basic_consume(queue=QUEUES["input" if role == "worker" else "output"], on_message_callback=callback(role))
                channel.start_consuming()
        except Exception:
            log.exception("%s reconnecting", role)
            time.sleep(3)
        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    role = sys.argv[1]
    if role not in ("publisher", "worker", "consumer", "maintenance"):
        raise SystemExit("Unknown role")
    run(role)
