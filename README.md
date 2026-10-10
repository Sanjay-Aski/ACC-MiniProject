# Sliding-window event deduplication

Python/FastAPI + RabbitMQ + Redis + PostgreSQL, managed with Docker Compose.

Every **new API submission** extends its content fingerprint's deduplication window by one hour. An event at 10:00 is unique; a repeat at 10:50 is duplicate and moves expiry to 11:50. An arrival exactly at expiry is unique again. This is a sliding inactivity window, so continuously repeated messages remain blocked.

Redis automatically expires fingerprint keys, not database message logs or queued messages. Logs retain payloads for seven days, with hourly cleanup. Unfinished delivery work is retained beyond seven days until it finishes or is dead-lettered.

## Start and operate

Install Docker Desktop with Linux containers, then run from this directory:

```powershell
docker compose up -d --build
docker compose ps
docker compose logs -f api worker publisher consumer maintenance
```

API docs: http://localhost:8000/docs

RabbitMQ management: http://localhost:15672 (local demo credentials: `dedup` / `dedup`). Queues are `incoming-events`, `unique-events`, their delayed retry queues, and `dead-events`.

```powershell
# Stop everything, preserving data
docker compose stop
# Restart existing stopped containers
docker compose start
# Rebuild and redeploy after code changes
docker compose up -d --build
# Remove containers and network; named data volumes remain
docker compose down
```

The services bind exposed ports to localhost. Credentials are demonstration defaults; configure secrets and authentication before exposing the system outside your machine. A single local PostgreSQL/RabbitMQ/Redis instance is not a highly available deployment.

## Send and inspect messages

```powershell
$message = @{ type = 'PAYMENT_SUCCESS'; amount = 500; eventId = 'TXN-101' } | ConvertTo-Json
$first = Invoke-RestMethod http://localhost:8000/events -Method Post -ContentType 'application/json' -Body $message
$second = Invoke-RestMethod http://localhost:8000/events -Method Post -ContentType 'application/json' -Body $message
Invoke-RestMethod "http://localhost:8000/events/$($first.submission_id)"
Invoke-RestMethod "http://localhost:8000/events/$($second.submission_id)"
Invoke-RestMethod http://localhost:8000/metrics
```

`POST /events` requires a finite JSON object. It returns `202` with `submission_id` only after the payload, API arrival timestamp, and ingestion outbox entry commit together. `GET /events/{id}` exposes the payload, fingerprint, arrival time in UTC epoch milliseconds, decision, delivery status, attempts, error, Bloom result, and Redis availability. Unknown or expired IDs return `404`. `/health` checks PostgreSQL; it does not assert that the broker or Redis is available. `/metrics` reports retained log counts, not lifetime counters.

For HTTP retries, send the same optional `Idempotency-Key` header (1–200 characters) with the same content. It returns the original submission ID without creating another arrival or refreshing expiry. Reusing a key with different content returns `409`. Without this header, every POST is a new arrival. Idempotency keys remain as long as their submission logs remain.

Duplicate detection hashes the **whole canonical JSON payload**. Object key order and whitespace do not matter. Array order, strings, booleans, null, and numeric values/types matter (`1` and `1.0` differ). All fields participate, including `eventId` and timestamps supplied in the payload. Python's JSON floating-point representation applies; the system does not implement arbitrary-precision numeric canonicalization. The authoritative arrival time is server-assigned, never taken from the payload.

```powershell
# Database message logs
docker compose exec postgres psql -U dedup -d dedup -c "SELECT id,arrival_ms,decision,delivery_status,attempts FROM submissions ORDER BY seq DESC LIMIT 20;"
# Queue counts
docker compose exec rabbitmq rabbitmqctl list_queues name messages consumers
# Expiring Redis fingerprint keys (SCAN, not FLUSHALL)
docker compose exec redis redis-cli --scan --pattern 'dedupe:*'
docker compose exec redis redis-cli PTTL dedupe:YOUR_FINGERPRINT
```

The demo consumer logs each accepted message once per retained acceptance ID and records consumption in PostgreSQL. All submissions, including duplicates, are logged in the database. RabbitMQ publication is at least once; custom consumers must also persist acceptance IDs to make their own side effects idempotent.

## Data flow and recovery

1. The API locks each fingerprint, saves a monotonic server arrival timestamp and submission sequence, and writes an input outbox record in one transaction.
2. The publisher delivers committed outbox entries with persistent RabbitMQ messages, mandatory routing, and publisher confirmations. It marks the record published after confirmation. A crash in between can repeat the delivery with the same ID.
3. The worker locks the fingerprint and processes pending submissions up through the delivered sequence, ordered by submission sequence. This includes earlier arrivals whose broker notification has not arrived yet.
4. For every previously unprocessed submission, a Lua operation atomically checks the durable prior expiry and sets Redis expiry to `arrival + window`. It removes the Redis key if that absolute expiry is already in the past. PostgreSQL records the final decision, updated expiry, and accepted-output outbox in one transaction.
5. The consumer inserts the acceptance ID and marks the message consumed together before acknowledging RabbitMQ. Repeated deliveries do not repeat the demo's database side effect.

**PostgreSQL is the durable authority.** A Redis write cannot participate in its transaction. The Lua operation therefore repairs Redis from the locked durable state when processing each new arrival. Redis outages do not drop messages or prevent deduplication: the worker records `redis_available=false`, decides from PostgreSQL, and maintenance reconciles the Redis cache every 30 seconds. The cache can temporarily be ahead of a rolled-back transaction or behind during an outage; no correctness decision relies on it alone. Broker retries of an already decided submission do not refresh its expiry.

Each worker maintains its own bounded Bloom filters: one-minute buckets, current plus the preceding 60 minutes, sized for 10,000 entries per bucket and a target 1% false-positive rate per bucket. Actual aggregate false-positive rate grows across buckets and with workload. Filter saturation, false positives, expired buckets, and worker restarts do not affect correctness. This implementation uses Bloom results for observation and experimentation; it does **not** claim to reduce Redis traffic, since every arrival needs an atomic expiry refresh.

Transient processing failures retry through durable queues with a five-second delay. After five recorded failures, the affected submission is marked dead-lettered and a durable dead-letter outbox entry is created. Infrastructure failures that prevent recording the failure retain broker ownership and retry. Inspect `dead-events` and database error logs for manual investigation; no automatic replay endpoint is provided. Outbox publication keeps retrying rather than losing unpublished messages during broker outages.

Application arrival timestamps use PostgreSQL's clock and per-fingerprint locking. Redis expiry uses Redis's clock; keep host clocks synchronized when deploying across hosts. Database log retention and consumer idempotency last seven days; deliveries older than that retention may no longer have a log. Use longer-lived consumer idempotency storage if downstream systems retain or replay messages longer.

## Configuration and tests

Create `.env` to shorten the window for an interactive demo:

```dotenv
DEDUP_WINDOW_SECONDS=10
LOG_RETENTION_DAYS=7
```

Apply it with `docker compose up -d --build`. Changing the window affects newly processed arrivals; existing recorded expiries remain until another arrival updates them. Never use a client-controlled timestamp to test production expiry.

Run tests with application background services stopped so they cannot consume test events:

```powershell
docker compose stop api publisher worker consumer maintenance
docker compose up -d --build postgres redis rabbitmq init
docker compose --profile test run --rm -e DEDUP_WINDOW_SECONDS=3600 -e LOG_RETENTION_DAYS=7 tests
docker compose up -d
```

Check the running HTTP-to-queue-to-consumer pipeline with `python scripts/smoke.py`. Save the returned `first_id`, stop and start the stack, then run `python scripts/smoke.py --verify-first YOUR_FIRST_ID` to verify that consumption and duplicate state survived (within the configured window).

Tests use unique payloads and real infrastructure with an isolated `dedup_tests` database, Redis database 1, and `test-` prefixed queues. They cover canonicalization, sliding boundaries, concurrent arrivals, reversed notification order, expired queued submissions, redelivery, Redis loss/reconciliation, Bloom false positives, database rollback after a Redis write, durable dead-letter creation, confirmed publication, consumer idempotency, and retention of unfinished work. They leave test records and queue messages in the local volumes.

Deleting Compose volumes destroys database logs and queued messages; ordinary stop/start, rebuild, and `down` preserve them. Azure integration and a dashboard are deferred.

Reference semantics: [RabbitMQ acknowledgements and publisher confirms](https://www.rabbitmq.com/docs/confirms), [psycopg transaction contexts](https://www.psycopg.org/psycopg3/docs/basic/transactions.html), [Redis SET absolute expiry](https://redis.io/docs/latest/commands/set/).
