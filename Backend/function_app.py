"""Azure Functions backend for the Azure Deduplication Virtual Lab.

SQL is the correctness boundary. Redis Bloom hits are visualized as hints only.
All HTTP routes are Function-key protected by default; do not expose function keys
inside a browser bundle. Put an authenticated backend/API gateway in front of this app.
"""
from __future__ import annotations

import hashlib
import ipaddress
import json
import logging
import math
import os
import re
import socket
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

import azure.functions as func
import mssql_python
import redis
from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSubQueue
from azure.servicebus.management import ServiceBusAdministrationClient
from redis_entraid.cred_provider import create_from_default_azure_credential

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "3600"))
BUCKET_SECONDS = int(os.getenv("BUCKET_SECONDS", "300"))
BLOOM_CAPACITY = int(os.getenv("BLOOM_CAPACITY_PER_BUCKET", "10000"))
BLOOM_FP_RATE = float(os.getenv("BLOOM_FALSE_POSITIVE_RATE", "0.01"))
MAX_RECORDS_PER_REQUEST = int(os.getenv("MAX_RECORDS_PER_REQUEST", "100"))
QUEUE_NAME = os.getenv("SERVICEBUS_QUEUE", "dedup-ingestion")
SB_NAMESPACE = os.getenv("ServiceBusConnection__fullyQualifiedNamespace") or os.getenv("SERVICEBUS_FULLY_QUALIFIED_NAMESPACE")
SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", "10000"))

_REDIS_DIAGNOSTIC_REDACTIONS = (
    # Mask credentials embedded in URLs, authorization headers, or exception text.
    (re.compile(r"(?i)((?:redis|rediss)://)([^:@/\s]*):([^@/\s]*)@"), r"\1\2:[REDACTED]@"),
    (re.compile(r"(?i)((?:access[_-]?token|refresh[_-]?token|id[_-]?token|token|password|passwd|secret|client[_-]?secret|api[_-]?key|credential|authorization)\s*[:=]\s*)(?:\"[^\"]*\"|'[^']*'|[^\s,;]+)"), r"\1[REDACTED]"),
    (re.compile(r"(?i)(\bbearer\s+)[A-Za-z0-9._~+/-]+=*"), r"\1[REDACTED]"),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}(?:\.[A-Za-z0-9_-]{10,})?\b"), "[REDACTED_TOKEN]"),
)

def _json_response(body: Any, status_code: int = 200, headers: dict[str, str] | None = None) -> func.HttpResponse:
    all_headers = {"Cache-Control": "no-store"}
    if headers:
        all_headers.update(headers)
    return func.HttpResponse(
        body=json.dumps(body, ensure_ascii=False, default=_json_default),
        status_code=status_code,
        mimetype="application/json",
        headers=all_headers,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    return str(value)


def _parse_uuid(value: str | None, name: str) -> str:
    try:
        return str(uuid.UUID(value or ""))
    except (ValueError, TypeError, AttributeError):
        raise ValueError(f"{name} must be a UUID")


def sql_connection():
    """Use Microsoft's Python SQL driver: Entra CLI locally, managed identity in Azure."""
    if not SQL_SERVER or not SQL_DATABASE:
        raise RuntimeError("SQL_SERVER and SQL_DATABASE must be configured")
    in_azure = bool(os.getenv("WEBSITE_INSTANCE_ID"))
    auth_mode = "ActiveDirectoryMSI" if in_azure else os.getenv("SQL_AUTH_MODE", "ActiveDirectoryDefault")
    return mssql_python.connect(
        server=SQL_SERVER,
        database=SQL_DATABASE,
        authentication=auth_mode,
        encrypt="yes",
        trust_server_certificate="no",
        timeout=30,
        autocommit=False,
    )


def redis_client():
    if not REDIS_HOST:
        raise RuntimeError("REDIS_HOST is not configured")
    provider = create_from_default_azure_credential(("https://redis.azure.com/.default",))
    # Azure Managed Redis OSS Cluster can advertise shard IPs even though its TLS
    # certificate is issued for the service hostname. Map only IPs that resolve
    # from our configured seed hostname back to that hostname, keeping each shard
    # port. This preserves TLS certificate hostname verification.
    try:
        seed_ips = {
            ipaddress.ip_address(result[4][0])
            for result in socket.getaddrinfo(REDIS_HOST, REDIS_PORT, type=socket.SOCK_STREAM)
        }
    except (OSError, ValueError):
        seed_ips = set()

    def remap_cluster_address(address: tuple[str, int]) -> tuple[str, int]:
        node_host, node_port = address
        try:
            node_ip = ipaddress.ip_address(node_host)
        except ValueError:
            return address
        if node_ip in seed_ips:
            return REDIS_HOST, node_port
        return address

    # Azure Managed Redis is configured with OSSCluster for this project.
    return redis.RedisCluster(
        host=REDIS_HOST,
        port=REDIS_PORT,
        ssl=True,
        ssl_cert_reqs="required",
        decode_responses=True,
        credential_provider=provider,
        socket_timeout=5,
        socket_connect_timeout=5,
        health_check_interval=30,
        address_remap=remap_cluster_address,
        dynamic_startup_nodes=False,
    )


def _safe_redis_diagnostic(value: str) -> str:
    """Remove common credential and token forms before returning or logging Redis errors."""
    for pattern, replacement in _REDIS_DIAGNOSTIC_REDACTIONS:
        value = pattern.sub(replacement, value)
    return value


def _log_redis_failure(exc: Exception) -> str:
    """Log a sanitized Redis exception and traceback, returning its safe message."""
    message = _safe_redis_diagnostic(str(exc))[:1000]
    if not message:
        message = "No exception message supplied"
    formatted_traceback = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    formatted_traceback = _safe_redis_diagnostic(formatted_traceback)
    logging.error(
        "Redis health check failed (%s): %s\nSanitized traceback:\n%s",
        type(exc).__name__,
        message,
        formatted_traceback,
    )
    return message


def service_bus_credential():
    return DefaultAzureCredential(exclude_interactive_browser_credential=True)


def send_messages_and_mark_queued(messages: list[dict[str, Any]]) -> int:
    if not SB_NAMESPACE:
        raise RuntimeError("ServiceBusConnection__fullyQualifiedNamespace is not configured")
    if not messages:
        return 0
    queued = 0
    with ServiceBusClient(fully_qualified_namespace=SB_NAMESPACE, credential=service_bus_credential()) as client:
        with client.get_queue_sender(queue_name=QUEUE_NAME) as sender:
            batch = sender.create_message_batch()
            batch_records: list[dict[str, Any]] = []
            for item in messages:
                msg = ServiceBusMessage(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
                try:
                    batch.add_message(msg)
                    batch_records.append(item)
                except ValueError:
                    if len(batch_records) == 0:
                        raise ValueError("A single record is larger than the Service Bus message limit")
                    sender.send_messages(batch)
                    _mark_queued(batch_records)
                    queued += len(batch_records)
                    batch = sender.create_message_batch()
                    batch_records = []
                    batch.add_message(msg)
                    batch_records.append(item)
            if batch_records:
                sender.send_messages(batch)
                _mark_queued(batch_records)
                queued += len(batch_records)
    return queued


def _mark_queued(messages: list[dict[str, Any]]) -> None:
    with sql_connection() as cn:
        cur = cn.cursor()
        for m in messages:
            cur.execute(
                "UPDATE dbo.TestMessages SET "
                "QueuedAt=COALESCE(QueuedAt,SYSUTCDATETIME()), "
                "Status=CASE WHEN Status IN ('registered','queued') THEN 'queued' ELSE Status END "
                "WHERE RecordId=?",
                (m["record_id"],),
            )
            insert_event(cur, m["test_run_id"], m["record_id"], "servicebus_queued", "queued", {
                "queue": QUEUE_NAME, "requestId": m["request_id"]
            })
        cn.commit()


def canonical_value(payload: Any) -> str:
    """Return the value used for equality. Text remains exact; JSON gets stable normalization."""
    if isinstance(payload, str):
        return "txt:" + payload
    if isinstance(payload, (int, float, bool)) or payload is None:
        return "scalar:" + json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if not isinstance(payload, dict):
        raise ValueError("Payload must be a string, JSON object, or JSON scalar")

    data = dict(payload)
    # Transport identity and event time describe delivery, not event content.
    # Exclude their common names so retries with new metadata deduplicate alike.
    for metadata_name in ("event_id", "eventId", "event_time", "eventTime", "timestamp"):
        data.pop(metadata_name, None)
    if "event_name" in data and isinstance(data["event_name"], str):
        event = re.sub(r"[\s-]+", "_", data["event_name"].strip().lower())
        aliases = {
            "payment_failure": "payment_failed",
            "payment_successful": "payment_success",
        }
        data["event_name"] = aliases.get(event, event)
        event = data["event_name"]
        if "order" in event:
            data = {k: data.get(k) for k in ("event_name", "user_id", "order_id")}
    return "json:" + json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def fingerprint(payload: Any) -> str:
    return hashlib.sha256(canonical_value(payload).encode("utf-8")).hexdigest()


def bloom_parameters(capacity: int, false_positive_rate: float) -> tuple[int, int]:
    if capacity <= 0 or not 0 < false_positive_rate < 1:
        raise ValueError("Invalid Bloom filter capacity or false-positive rate")
    bits = math.ceil(-capacity * math.log(false_positive_rate) / (math.log(2) ** 2))
    hashes = max(1, round(bits / capacity * math.log(2)))
    return bits, hashes


def bloom_positions(fp: str, bit_count: int, hash_count: int):
    raw = bytes.fromhex(fp)
    h1 = int.from_bytes(raw[:16], "big")
    h2 = int.from_bytes(raw[16:], "big") | 1
    for i in range(hash_count):
        yield (h1 + i * h2) % bit_count


def bloom_check_and_add(client, fp: str) -> bool:
    """Redis is only a hint; the SQL procedure decides accepted versus duplicate."""
    bits, hashes = bloom_parameters(BLOOM_CAPACITY, BLOOM_FP_RATE)
    now = int(time.time())
    current = (now // BUCKET_SECONDS) * BUCKET_SECONDS
    oldest = ((now - WINDOW_SECONDS) // BUCKET_SECONDS) * BUCKET_SECONDS
    positions = list(bloom_positions(fp, bits, hashes))
    buckets = list(range(oldest, current + 1, BUCKET_SECONDS))
    # Batch all bit reads into a single cluster-safe pipeline. All keys include the
    # same Redis hash tag ({dedup}), so Redis Cluster routes them to one slot.
    read_pipe = client.pipeline(transaction=False)
    read_buckets: list[int] = []
    for bucket in buckets:
        key = f"bloom:{{dedup}}:{bucket}"
        for pos in positions:
            read_pipe.getbit(key, pos)
            read_buckets.append(bucket)
    values = read_pipe.execute()
    bucket_matches: dict[int, bool] = {bucket: True for bucket in buckets}
    for bucket, value in zip(read_buckets, values):
        bucket_matches[bucket] = bucket_matches[bucket] and bool(value)
    possible = any(bucket_matches.values())
    current_key = f"bloom:{{dedup}}:{current}"
    pipe = client.pipeline(transaction=False)
    for pos in positions:
        pipe.setbit(current_key, pos, 1)
    pipe.expire(current_key, WINDOW_SECONDS + 2 * BUCKET_SECONDS)
    pipe.execute()
    return possible


def insert_event(cur, test_run_id: str, record_id: str | None, stage: str, status: str, detail: dict[str, Any] | None = None):
    cur.execute(
        "INSERT INTO dbo.ProcessingEvents(TestRunId, RecordId, Stage, Status, DetailJson) VALUES(?,?,?,?,?)",
        (
            test_run_id,
            record_id,
            stage,
            status,
            json.dumps(detail or {}, ensure_ascii=False, separators=(",", ":")),
        ),
    )


def emit_event(test_run_id: str, record_id: str | None, stage: str, status: str, detail: dict[str, Any] | None = None):
    with sql_connection() as cn:
        cur = cn.cursor()
        insert_event(cur, test_run_id, record_id, stage, status, detail)
        cn.commit()


def register_input(test_run_id: str, expected_count: int | None, records: list[dict[str, Any]]):
    """Register each record idempotently; reject reuse of an ID for different data."""
    queue_items: list[dict[str, Any]] = []
    with sql_connection() as cn:
        cur = cn.cursor()
        cur.execute("SELECT ExpectedCount FROM dbo.TestRuns WHERE TestRunId=?", (test_run_id,))
        run = cur.fetchone()
        if run is None:
            cur.execute(
                "INSERT dbo.TestRuns(TestRunId, ExpectedCount) VALUES(?,?)",
                (test_run_id, expected_count),
            )
        else:
            stored_expected = int(run.ExpectedCount) if run.ExpectedCount is not None else None
            if expected_count is not None and stored_expected is not None and stored_expected != expected_count:
                raise ValueError("expectedCount does not match the value already registered for this testRunId")
            if expected_count is not None and stored_expected is None:
                cur.execute("UPDATE dbo.TestRuns SET ExpectedCount=? WHERE TestRunId=?", (expected_count, test_run_id))

        for item in records:
            cur.execute(
                "SELECT Status,TestRunId,LineNumber,PayloadJson,OutputText FROM dbo.TestMessages WHERE RecordId=?",
                (item["record_id"],),
            )
            existing = cur.fetchone()
            if existing is None:
                cur.execute(
                    "INSERT dbo.TestMessages(TestRunId,RecordId,LineNumber,RequestId,PayloadJson,OutputText,Status) "
                    "VALUES(?,?,?,?,?,?, 'registered')",
                    (
                        item["test_run_id"], item["record_id"], item["line_number"], item["request_id"],
                        item["payload_json"], item["output_text"],
                    ),
                )
                insert_event(cur, test_run_id, item["record_id"], "ingest_received", "received", {
                    "lineNumber": item["line_number"], "requestId": item["request_id"]
                })
                insert_event(cur, test_run_id, item["record_id"], "validated", "valid", {
                    "payloadType": item["payload_type"]
                })
                insert_event(cur, test_run_id, item["record_id"], "registered", "registered", {})
                queue_items.append(item)
                continue

            # Stable IDs make a retry idempotent, but must never allow data/run substitution.
            if (
                str(existing.TestRunId) != test_run_id
                or int(existing.LineNumber) != int(item["line_number"])
                or str(existing.PayloadJson) != item["payload_json"]
                or str(existing.OutputText) != item["output_text"]
            ):
                raise ValueError(
                    "A recordId/line number was already registered with different run or payload data"
                )

            if existing.Status == "failed":
                # Explicit re-submission of failed data preserves prior timeline history.
                cur.execute(
                    "UPDATE dbo.TestMessages SET Status='registered', Fingerprint=NULL, ProcessedAt=NULL, "
                    "ErrorMessage=NULL WHERE RecordId=? AND Status='failed'",
                    (item["record_id"],),
                )
                insert_event(cur, test_run_id, item["record_id"], "retry_submitted", "registered", {
                    "requestId": item["request_id"], "lineNumber": item["line_number"]
                })
                queue_items.append(item)
            elif existing.Status == "registered":
                # Supports retry after SQL registration succeeded but queue publication did not.
                queue_items.append(item)
            elif existing.Status in ("queued", "accepted", "duplicate"):
                # Already dispatched or processed: never enqueue a duplicate merely because
                # the client retried its HTTP request.
                continue
            else:
                raise ValueError(f"Unsupported stored message status: {existing.Status}")
        cn.commit()
    return queue_items


def service_bus_peek():
    if not SB_NAMESPACE:
        raise RuntimeError("Service Bus namespace setting is missing")
    with ServiceBusClient(fully_qualified_namespace=SB_NAMESPACE, credential=service_bus_credential()) as client:
        with client.get_queue_receiver(queue_name=QUEUE_NAME, max_wait_time=3) as receiver:
            receiver.peek_messages(max_message_count=1)
    return {"healthy": True, "queue": QUEUE_NAME, "namespace": SB_NAMESPACE}


def _sql_counts(test_run_id: str) -> dict[str, Any] | None:
    with sql_connection() as cn:
        cur = cn.cursor()
        cur.execute("SELECT TestRunId, ExpectedCount, CreatedAt FROM dbo.TestRuns WHERE TestRunId=?", (test_run_id,))
        run = cur.fetchone()
        if not run:
            return None
        cur.execute(
            """SELECT COUNT(*) AS submitted,
                      SUM(CASE WHEN QueuedAt IS NOT NULL THEN 1 ELSE 0 END) AS queued,
                      SUM(CASE WHEN Status='accepted' THEN 1 ELSE 0 END) AS accepted,
                      SUM(CASE WHEN Status='duplicate' THEN 1 ELSE 0 END) AS duplicates,
                      SUM(CASE WHEN Status='failed' THEN 1 ELSE 0 END) AS failed,
                      SUM(CASE WHEN Status IN ('registered','queued') THEN 1 ELSE 0 END) AS pending,
                      SUM(CASE WHEN Status IN ('accepted','duplicate') THEN 1 ELSE 0 END) AS processed
               FROM dbo.TestMessages WHERE TestRunId=?""", (test_run_id,))
        row = cur.fetchone()
        submitted = int(row.submitted or 0)
        queued = int(row.queued or 0)
        accepted = int(row.accepted or 0)
        duplicates = int(row.duplicates or 0)
        failed = int(row.failed or 0)
        pending = int(row.pending or 0)
        processed = int(row.processed or 0)
        expected = int(run.ExpectedCount) if run.ExpectedCount is not None else submitted
        complete = processed + failed >= expected and submitted >= expected
        return {
            "testRunId": str(run.TestRunId), "status": "completed" if complete else "processing",
            "expectedCount": expected, "submittedCount": submitted, "queuedCount": queued,
            "processedCount": processed, "acceptedCount": accepted, "duplicateCount": duplicates,
            "failedCount": failed, "pendingCount": pending,
            "createdAt": run.CreatedAt.replace(tzinfo=timezone.utc).isoformat(),
            # A terminal failure is resolved for progress purposes, even though it
            # is reported separately from successful accepted/duplicate processing.
            "progressPercent": 100 if expected == 0 else min(100, round((processed + failed) / expected * 100, 1)),
        }


@app.route(route="health", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def health(req: func.HttpRequest) -> func.HttpResponse:
    deps: dict[str, Any] = {}
    try:
        with sql_connection() as cn:
            cur = cn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        deps["sql"] = {"healthy": True}
    except Exception as exc:
        logging.exception("SQL health check failed")
        deps["sql"] = {"healthy": False, "errorType": type(exc).__name__}
    try:
        r = redis_client()
        try:
            r.ping()
        finally:
            r.close()
        deps["redis"] = {"healthy": True, "mode": "Entra ID + TLS", "role": "Bloom hint only"}
    except Exception as exc:
        error_message = _log_redis_failure(exc)
        deps["redis"] = {
            "healthy": False,
            "errorType": type(exc).__name__,
            "errorMessage": error_message,
            "fallback": "SQL remains authoritative",
        }
    try:
        deps["serviceBus"] = service_bus_peek()
    except Exception as exc:
        logging.exception("Service Bus health check failed")
        deps["serviceBus"] = {"healthy": False, "errorType": type(exc).__name__}
    required_ready = bool(deps.get("sql", {}).get("healthy") and deps.get("serviceBus", {}).get("healthy"))
    all_healthy = required_ready and bool(deps.get("redis", {}).get("healthy"))
    return _json_response({
        "healthy": all_healthy,
        "ready": required_ready,
        "status": "healthy" if all_healthy else ("degraded" if required_ready else "unavailable"),
        "dependencies": deps,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }, 200 if required_ready else 503)


@app.route(route="ingest", methods=["POST"], auth_level=func.AuthLevel.FUNCTION)
def ingest(req: func.HttpRequest) -> func.HttpResponse:
    request_id = str(uuid.uuid4())
    try:
        body = req.get_json()
        if not isinstance(body, dict):
            return _json_response({"error": "Request body must be a JSON object"}, 400)
        records_in = body.get("records")
        if not isinstance(records_in, list) or not records_in:
            return _json_response({"error": "Provide a non-empty records array"}, 400)
        if len(records_in) > MAX_RECORDS_PER_REQUEST:
            return _json_response({"error": f"Maximum {MAX_RECORDS_PER_REQUEST} records per request"}, 413)
        test_run_id = _parse_uuid(body.get("testRunId") or str(uuid.uuid4()), "testRunId")
        expected_count = body.get("expectedCount")
        if expected_count is not None and (isinstance(expected_count, bool) or not isinstance(expected_count, int) or expected_count < len(records_in)):
            return _json_response({"error": "expectedCount must be an integer at least as large as this batch"}, 400)

        items = []
        start_line = int(body.get("lineNumberStart", 1))
        if start_line < 1:
            return _json_response({"error": "lineNumberStart must be >= 1"}, 400)
        seen_lines: set[int] = set()
        seen_record_ids: set[str] = set()
        for i, raw in enumerate(records_in):
            line_number = start_line + i
            payload = raw
            requested_line = line_number
            record_id = None
            if isinstance(raw, dict) and "payload" in raw and set(raw.keys()).issubset({"payload", "lineNumber", "recordId"}):
                payload = raw["payload"]
                requested_line = int(raw.get("lineNumber", line_number))
                record_id = raw.get("recordId")
            if not (isinstance(payload, (str, dict, int, float, bool)) or payload is None):
                return _json_response({"error": f"Record at index {i} must be text, a JSON object, or scalar"}, 400)
            if requested_line < 1:
                return _json_response({"error": f"Record at index {i} has an invalid line number"}, 400)
            if requested_line in seen_lines:
                return _json_response({"error": f"Duplicate line number in request: {requested_line}"}, 400)
            seen_lines.add(requested_line)
            if record_id:
                record_id = _parse_uuid(record_id, "recordId")
            else:
                record_id = str(uuid.uuid5(uuid.UUID(test_run_id), f"line:{requested_line}"))
            if record_id in seen_record_ids:
                return _json_response({"error": f"Duplicate recordId in request: {record_id}"}, 400)
            seen_record_ids.add(record_id)
            payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            output_text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            items.append({
                "test_run_id": test_run_id, "request_id": request_id, "record_id": record_id,
                "line_number": requested_line, "payload": payload, "payload_json": payload_json,
                "output_text": output_text, "payload_type": "text" if isinstance(payload, str) else "json",
            })
        queue_items = register_input(test_run_id, expected_count, items)
        logging.info("dedup_stage stage=registered test_run_id=%s request_id=%s count=%d", test_run_id, request_id, len(queue_items))
        try:
            queued_count = send_messages_and_mark_queued(queue_items)
        except Exception as exc:
            logging.exception("Service Bus queue send failed test_run_id=%s", test_run_id)
            for item in queue_items:
                try:
                    emit_event(test_run_id, item["record_id"], "queue_publish", "failed", {"errorType": type(exc).__name__})
                except Exception:
                    logging.exception("Could not persist queue-failure event")
            return _json_response({
                "testRunId": test_run_id, "requestId": request_id, "status": "queue_failed",
                "registeredCount": len(queue_items), "message": "Messages remain registered and can be retried using the same testRunId and line numbers.",
            }, 503)
        emit_event(test_run_id, None, "ingest_completed", "queued", {"requestId": request_id, "queuedCount": queued_count})
        return _json_response({
            "testRunId": test_run_id, "requestId": request_id, "status": "queued",
            "recordsSubmitted": len(items), "recordsNewlyQueued": queued_count,
            "statusUrl": f"/api/test-status?testRunId={test_run_id}",
            "eventsUrl": f"/api/test-events?testRunId={test_run_id}",
            "outputUrl": f"/api/test-output?testRunId={test_run_id}",
        }, 202)
    except (ValueError, TypeError) as exc:
        return _json_response({"error": str(exc)}, 400)
    except Exception:
        logging.exception("Ingestion failed")
        return _json_response({"error": "Internal server error"}, 500)


@app.service_bus_queue_trigger(arg_name="msg", queue_name="%SERVICEBUS_QUEUE%", connection="ServiceBusConnection")
def dedup_worker(msg: func.ServiceBusMessage):
    data = json.loads(msg.get_body().decode("utf-8"))
    test_run_id = _parse_uuid(data["test_run_id"], "test_run_id")
    record_id = _parse_uuid(data["record_id"], "record_id")
    payload = data["payload"]
    started = time.perf_counter()
    try:
        emit_event(test_run_id, record_id, "worker_received", "received", {
            "deliveryCount": msg.delivery_count, "sequenceNumber": getattr(msg, "sequence_number", None),
        })
        fp = fingerprint(payload)
        emit_event(test_run_id, record_id, "fingerprint_generated", "generated", {
            "algorithm": "SHA-256", "fingerprint": fp, "canonicalization": "text-exact/json-normalized",
        })
        bloom_hint: bool | None = None
        try:
            r = redis_client()
            try:
                bloom_hint = bloom_check_and_add(r, fp)
            finally:
                r.close()
            bloom_status = "possible_match" if bloom_hint else "no_match"
            emit_event(test_run_id, record_id, "bloom_checked", bloom_status, {
                "possibleMatch": bloom_hint, "note": "Bloom is probabilistic; SQL decides the result.",
                "windowSeconds": WINDOW_SECONDS, "bucketSeconds": BUCKET_SECONDS,
            })
        except Exception as redis_exc:
            logging.warning("Redis Bloom fallback test_run_id=%s record_id=%s error=%s", test_run_id, record_id, type(redis_exc).__name__)
            emit_event(test_run_id, record_id, "bloom_checked", "unavailable_fallback", {
                "possibleMatch": None, "errorType": type(redis_exc).__name__,
                "note": "Redis failed; continue to authoritative SQL decision.",
            })
        with sql_connection() as cn:
            cur = cn.cursor()
            cur.execute("EXEC dbo.ProcessDedupRecord @RecordId=?, @Fingerprint=?, @WindowSeconds=?, @BloomPossibleMatch=?",
                        (record_id, fp, WINDOW_SECONDS, bloom_hint))
            row = cur.fetchone()
            cn.commit()
        if row is None or getattr(row, "Status", None) is None:
            raise RuntimeError("SQL deduplication procedure returned no decision")
        result = str(row.Status).lower()
        if result not in ("accepted", "duplicate"):
            raise RuntimeError(f"SQL deduplication procedure returned an unexpected status: {result}")
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        emit_event(test_run_id, record_id, "output_route", "accepted_output" if result == "accepted" else "duplicate_sink", {
            "lineNumber": data["line_number"], "elapsedMs": elapsed_ms,
        })
        logging.info("dedup_result test_run_id=%s record_id=%s result=%s bloom_possible=%s delivery_count=%s elapsed_ms=%s",
                     test_run_id, record_id, result, bloom_hint, msg.delivery_count, elapsed_ms)
    except Exception as exc:
        logging.exception("Dedup worker failed test_run_id=%s record_id=%s", test_run_id, record_id)
        try:
            with sql_connection() as cn:
                cur = cn.cursor()
                # Do not mark the record permanently failed during a retryable delivery.
                # The timer reconciler marks it failed only after it appears in the DLQ.
                cur.execute("UPDATE dbo.TestMessages SET ErrorMessage=? WHERE RecordId=? AND Status NOT IN ('accepted','duplicate','failed')",
                            (f"{type(exc).__name__}: {str(exc)[:900]}", record_id))
                cur.execute("INSERT dbo.ProcessingFailures(TestRunId,RecordId,Stage,ErrorType,ErrorMessage) VALUES(?,?,?,?,?)",
                            (test_run_id, record_id, "worker_attempt", type(exc).__name__, str(exc)[:1000]))
                insert_event(cur, test_run_id, record_id, "processing_failed", "retry_pending", {"errorType": type(exc).__name__})
                cn.commit()
        except Exception:
            logging.exception("Failed to persist worker failure")
        raise  # Service Bus trigger retries and eventually dead-letters the message.


@app.timer_trigger(schedule="0 */1 * * * *", arg_name="timer", run_on_startup=False, use_monitor=True)
def reconcile_deadletter(timer: func.TimerRequest):
    """Peek DLQ messages incrementally, record terminal failures, and checkpoint sequence numbers.

    Peek is deliberately non-destructive so operators can inspect DLQ messages in Service Bus.
    The checkpoint prevents the same earliest messages from starving later dead-lettered messages.
    """
    if not SB_NAMESPACE:
        logging.error("DLQ reconciler skipped: Service Bus namespace setting is missing")
        return
    try:
        with sql_connection() as cn:
            cur = cn.cursor()
            cur.execute("SELECT LastSequenceNumber FROM dbo.DeadLetterCheckpoint WHERE QueueName=?", (QUEUE_NAME,))
            row = cur.fetchone()
            last_sequence = int(row.LastSequenceNumber) if row else 0
            if row is None:
                cur.execute("INSERT dbo.DeadLetterCheckpoint(QueueName, LastSequenceNumber) VALUES(?,0)", (QUEUE_NAME,))
                cn.commit()
        with ServiceBusClient(fully_qualified_namespace=SB_NAMESPACE, credential=service_bus_credential()) as client:
            with client.get_queue_receiver(queue_name=QUEUE_NAME, sub_queue=ServiceBusSubQueue.DEAD_LETTER, max_wait_time=2) as receiver:
                messages = receiver.peek_messages(max_message_count=100, sequence_number=last_sequence + 1)
        for message in messages:
            sequence = getattr(message, "sequence_number", None)
            if sequence is None:
                logging.error("DLQ message has no sequence number; can't checkpoint safely")
                continue
            try:
                data = json.loads(message.get_body().decode("utf-8"))
                test_run_id = _parse_uuid(data["test_run_id"], "test_run_id")
                record_id = _parse_uuid(data["record_id"], "record_id")
            except Exception as bad_message:
                # Record the malformed DLQ entry in logs and advance so it doesn't block newer entries.
                logging.error("Dead-letter payload could not be correlated (sequence=%s, error=%s)", sequence, type(bad_message).__name__)
                with sql_connection() as cn:
                    cur = cn.cursor()
                    cur.execute("UPDATE dbo.DeadLetterCheckpoint SET LastSequenceNumber=CASE WHEN LastSequenceNumber<? THEN ? ELSE LastSequenceNumber END, UpdatedAt=SYSUTCDATETIME() WHERE QueueName=?", (sequence, sequence, QUEUE_NAME))
                    cn.commit()
                continue
            with sql_connection() as cn:
                cur = cn.cursor()
                cur.execute("SELECT Status FROM dbo.TestMessages WHERE RecordId=?", (record_id,))
                prior = cur.fetchone()
                if prior and prior.Status not in ("accepted", "duplicate", "failed"):
                    cur.execute("UPDATE dbo.TestMessages SET Status='failed', ProcessedAt=SYSUTCDATETIME(), ErrorMessage=? WHERE RecordId=?",
                                (str(getattr(message, "dead_letter_error_description", "Dead-lettered by Service Bus"))[:900], record_id))
                    insert_event(cur, test_run_id, record_id, "dead_lettered", "failed", {
                        "reason": getattr(message, "dead_letter_reason", None),
                        "deliveryCount": getattr(message, "delivery_count", None),
                        "sequenceNumber": sequence,
                    })
                cur.execute("UPDATE dbo.DeadLetterCheckpoint SET LastSequenceNumber=CASE WHEN LastSequenceNumber<? THEN ? ELSE LastSequenceNumber END, UpdatedAt=SYSUTCDATETIME() WHERE QueueName=?", (sequence, sequence, QUEUE_NAME))
                cn.commit()
    except Exception:
        logging.exception("Dead-letter reconciliation check failed")


@app.route(route="test-status", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def test_status(req: func.HttpRequest) -> func.HttpResponse:
    try:
        test_run_id = _parse_uuid(req.params.get("testRunId"), "testRunId")
        status = _sql_counts(test_run_id)
        if status is None:
            return _json_response({"error": "Test run not found", "testRunId": test_run_id}, 404)
        with sql_connection() as cn:
            cur = cn.cursor()
            cur.execute("SELECT TOP (12) Stage,Status,COUNT(*) AS EventCount FROM dbo.ProcessingEvents WHERE TestRunId=? GROUP BY Stage,Status ORDER BY Stage,Status", (test_run_id,))
            stage_counts = [{"stage": r.Stage, "status": r.Status, "count": int(r.EventCount)} for r in cur.fetchall()]
        status["stageCounts"] = stage_counts
        return _json_response(status)
    except ValueError as exc:
        return _json_response({"error": str(exc)}, 400)
    except Exception:
        logging.exception("Test status read failed")
        return _json_response({"error": "Internal server error"}, 500)


@app.route(route="test-events", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def test_events(req: func.HttpRequest) -> func.HttpResponse:
    try:
        test_run_id = _parse_uuid(req.params.get("testRunId"), "testRunId")
        after_id = max(0, int(req.params.get("afterEventId", "0")))
        limit = min(500, max(1, int(req.params.get("limit", "100"))))
        with sql_connection() as cn:
            cur = cn.cursor()
            cur.execute(
                "SELECT TOP (?) EventId,RecordId,Stage,Status,DetailJson,CreatedAt FROM dbo.ProcessingEvents "
                "WHERE TestRunId=? AND EventId>? ORDER BY EventId ASC",
                (limit, test_run_id, after_id),
            )
            rows = cur.fetchall()
        events = []
        for r in rows:
            try:
                detail = json.loads(r.DetailJson or "{}")
            except json.JSONDecodeError:
                detail = {"raw": r.DetailJson}
            events.append({
                "eventId": int(r.EventId), "testRunId": test_run_id,
                "recordId": str(r.RecordId) if r.RecordId else None,
                "stage": r.Stage, "status": r.Status, "detail": detail,
                "occurredAt": r.CreatedAt.replace(tzinfo=timezone.utc).isoformat(),
                "visualNode": _visual_node(r.Stage, r.Status),
            })
        return _json_response({"testRunId": test_run_id, "events": events,
                                "nextAfterEventId": events[-1]["eventId"] if events else after_id,
                                "hasMore": len(events) == limit})
    except (ValueError, TypeError) as exc:
        return _json_response({"error": str(exc)}, 400)
    except Exception:
        logging.exception("Test event read failed")
        return _json_response({"error": "Internal server error"}, 500)


def _visual_node(stage: str, status: str) -> str:
    mapping = {
        "ingest_received": "client", "validated": "api", "registered": "sql_tracking",
        "retry_submitted": "sql_tracking",
        "queue_publish": "service_bus", "servicebus_queued": "service_bus",
        "worker_received": "function_worker", "fingerprint_generated": "fingerprint",
        "bloom_checked": "redis_bloom", "sql_decision": "azure_sql",
        "output_route": "accepted_output" if status == "accepted_output" else "duplicate_sink",
        "processing_failed": "dead_letter_or_retry", "dead_lettered": "dead_letter",
        "ingest_completed": "service_bus",
    }
    return mapping.get(stage, "pipeline")


@app.route(route="test-record", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def test_record(req: func.HttpRequest) -> func.HttpResponse:
    """Detailed inspector endpoint for one record in a test run.

    Payload text is omitted by default; callers must explicitly request includePayload=true.
    """
    try:
        test_run_id = _parse_uuid(req.params.get("testRunId"), "testRunId")
        record_id = _parse_uuid(req.params.get("recordId"), "recordId")
        include_payload = req.params.get("includePayload", "false").lower() == "true"
        with sql_connection() as cn:
            cur = cn.cursor()
            cur.execute(
                "SELECT RecordId,LineNumber,Status,Fingerprint,CreatedAt,QueuedAt,ProcessedAt,ErrorMessage,PayloadJson,OutputText "
                "FROM dbo.TestMessages WHERE TestRunId=? AND RecordId=?",
                (test_run_id, record_id),
            )
            row = cur.fetchone()
            if row is None:
                return _json_response({"error": "Record not found for this test run", "testRunId": test_run_id, "recordId": record_id}, 404)
            cur.execute(
                "SELECT TOP (200) EventId,Stage,Status,DetailJson,CreatedAt FROM dbo.ProcessingEvents "
                "WHERE TestRunId=? AND RecordId=? ORDER BY EventId ASC",
                (test_run_id, record_id),
            )
            event_rows = cur.fetchall()
        timeline = []
        for event in event_rows:
            try:
                detail = json.loads(event.DetailJson or "{}")
            except json.JSONDecodeError:
                detail = {"raw": event.DetailJson}
            timeline.append({
                "eventId": int(event.EventId), "stage": event.Stage, "status": event.Status,
                "detail": detail, "occurredAt": event.CreatedAt.replace(tzinfo=timezone.utc).isoformat(),
                "visualNode": _visual_node(event.Stage, event.Status),
            })
        record = {
            "testRunId": test_run_id, "recordId": record_id, "lineNumber": int(row.LineNumber),
            "status": row.Status, "fingerprint": row.Fingerprint,
            "createdAt": row.CreatedAt.replace(tzinfo=timezone.utc).isoformat(),
            "queuedAt": row.QueuedAt.replace(tzinfo=timezone.utc).isoformat() if row.QueuedAt else None,
            "processedAt": row.ProcessedAt.replace(tzinfo=timezone.utc).isoformat() if row.ProcessedAt else None,
            "errorMessage": row.ErrorMessage, "timeline": timeline,
        }
        if include_payload:
            record["payload"] = json.loads(row.PayloadJson)
            record["outputText"] = row.OutputText
        return _json_response({"record": record})
    except ValueError as exc:
        return _json_response({"error": str(exc)}, 400)
    except Exception:
        logging.exception("Test record inspector failed")
        return _json_response({"error": "Internal server error"}, 500)


@app.route(route="test-output", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def test_output(req: func.HttpRequest) -> func.HttpResponse:
    try:
        test_run_id = _parse_uuid(req.params.get("testRunId"), "testRunId")
        with sql_connection() as cn:
            cur = cn.cursor()
            cur.execute("SELECT ExpectedCount FROM dbo.TestRuns WHERE TestRunId=?", (test_run_id,))
            run = cur.fetchone()
            if not run:
                return _json_response({"error": "Test run not found", "testRunId": test_run_id}, 404)
            run_status = _sql_counts(test_run_id)
            if run_status and run_status.get("status") != "completed":
                return _json_response({"error": "Test run is not complete yet", "testRun": run_status}, 409)
            cur.execute("SELECT OutputText FROM dbo.AcceptedRecords WHERE TestRunId=? ORDER BY LineNumber, AcceptedAt, RecordId", (test_run_id,))
            lines = [str(row.OutputText) for row in cur.fetchall()]
        output = "\n".join(lines) + ("\n" if lines else "")
        return func.HttpResponse(
            body=output, status_code=200, mimetype="text/plain; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="dedup-output-{test_run_id}.txt"', "Cache-Control": "no-store"},
        )
    except ValueError as exc:
        return _json_response({"error": str(exc)}, 400)
    except Exception:
        logging.exception("Test output read failed")
        return _json_response({"error": "Internal server error"}, 500)


@app.route(route="lab/overview", methods=["GET"], auth_level=func.AuthLevel.FUNCTION)
def lab_overview(req: func.HttpRequest) -> func.HttpResponse:
    """Aggregated virtual-lab overview for the frontend. Does not expose payloads/secrets."""
    results: dict[str, Any] = {"timestamp": datetime.now(timezone.utc).isoformat()}
    try:
        with sql_connection() as cn:
            cur = cn.cursor()
            cur.execute("SELECT COUNT(*) FROM dbo.TestRuns WHERE CreatedAt>=DATEADD(HOUR,-1,SYSUTCDATETIME())")
            results["runsLastHour"] = int(cur.fetchone()[0])
            cur.execute("""SELECT
                SUM(CASE WHEN Status='accepted' THEN 1 ELSE 0 END),
                SUM(CASE WHEN Status='duplicate' THEN 1 ELSE 0 END),
                SUM(CASE WHEN Status='queued' THEN 1 ELSE 0 END),
                SUM(CASE WHEN Status='failed' THEN 1 ELSE 0 END)
                FROM dbo.TestMessages WHERE CreatedAt>=DATEADD(HOUR,-1,SYSUTCDATETIME())""")
            row = cur.fetchone()
            results["lastHour"] = {"accepted": int(row[0] or 0), "duplicates": int(row[1] or 0),
                                   "queued": int(row[2] or 0), "failed": int(row[3] or 0)}
            cur.execute("SELECT TOP (10) TestRunId,ExpectedCount,CreatedAt FROM dbo.TestRuns ORDER BY CreatedAt DESC")
            recent = cur.fetchall()
            results["recentRuns"] = []
            for rr in recent:
                status = _sql_counts(str(rr.TestRunId))
                if status:
                    results["recentRuns"].append(status)
        results["sql"] = {"healthy": True}
    except Exception as exc:
        logging.exception("Lab overview SQL failed")
        results["sql"] = {"healthy": False, "errorType": type(exc).__name__}
    try:
        r = redis_client()
        try:
            results["redis"] = {"healthy": bool(r.ping()), "role": "Bloom pre-check hint"}
        finally:
            r.close()
    except Exception as exc:
        error_message = _log_redis_failure(exc)
        results["redis"] = {
            "healthy": False,
            "errorType": type(exc).__name__,
            "errorMessage": error_message,
            "role": "Bloom hint only; SQL remains authoritative",
        }
    try:
        with ServiceBusAdministrationClient(fully_qualified_namespace=SB_NAMESPACE, credential=service_bus_credential()) as admin:
            p = admin.get_queue_runtime_properties(QUEUE_NAME)
        results["serviceBus"] = {"healthy": True, "queue": QUEUE_NAME,
                                 "active": int(p.active_message_count),
                                 "deadLettered": int(p.dead_letter_message_count),
                                 "scheduled": int(p.scheduled_message_count)}
    except Exception as exc:
        logging.warning("Queue runtime metric read unavailable: %s", type(exc).__name__)
        results["serviceBus"] = {"healthy": False, "errorType": type(exc).__name__,
                                 "note": "The Service Bus namespace also exposes metrics in Azure Monitor."}
    return _json_response(results, 200 if results.get("sql", {}).get("healthy") else 503)
