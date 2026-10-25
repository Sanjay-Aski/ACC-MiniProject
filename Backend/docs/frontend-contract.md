# Frontend virtual lab contract

This document is the contract for a frontend that will be generated separately. The frontend should present **real backend events** from SQL rather than a pretend animation.

## Screen areas

1. **Input panel**: upload/select TXT, choose N lines, show raw lines, start a run.
2. **Pipeline canvas**: show the message travelling through these nodes:
   `Client → API validation → SQL run ledger → Service Bus queue → Function worker → Fingerprint → Redis Bloom pre-check → Azure SQL decision → Accepted output OR Duplicate sink`.
3. **Inspector panel**: selected record's line number, record ID, request/run IDs, SHA-256 fingerprint, Bloom result, SQL final status, elapsed time, retry count and timeline.
4. **Metrics panel**: submitted, queued/pending, processed, accepted, duplicate, failed, processing percentage, active queue depth, dead-letter count, Redis health, SQL health, throughput and recent runs.
5. **Output panel**: download `text/plain` from `/api/test-output` after the run is complete.

## Endpoints to call

Set a base URL from the exact `defaultHostName` in `deployment-summary.json`, for example `https://<generated-host>.westus3-01.azurewebsites.net/api`. Do not infer the hostname from the Function App resource name on Flex Consumption. All routes require the Function key in `x-functions-key` in this starter. The key must be supplied by a trusted backend/proxy, not shipped inside a public SPA bundle.

### Start a run

`POST /ingest`

```json
{
  "testRunId": "51f88be4-d81e-4f11-8e8f-722f5fa69e0c",
  "expectedCount": 3,
  "records": [
    {"lineNumber": 1, "payload": "alpha"},
    {"lineNumber": 2, "payload": "beta"},
    {"lineNumber": 3, "payload": "alpha"}
  ]
}
```

Use a new UUID per run. Chunked requests must reuse the same `testRunId`, have stable `lineNumber` values, and include the full `expectedCount`. The backend derives a stable `recordId` from the run ID and line number unless explicitly provided.

Response is `202 Accepted` with `testRunId`, `requestId`, `recordsSubmitted`, `recordsNewlyQueued`, and URLs for status/events/output. If it returns `503 queue_failed`, use the same test run ID and line numbers to resume safely.

### Poll events for the animation

`GET /test-events?testRunId=<UUID>&afterEventId=0&limit=100`

Poll every 500–1000 ms while a run is active, using `nextAfterEventId` as the next `afterEventId`. `eventId` is a monotonic SQL identity. Since a Service Bus worker can start while the sender is recording the queued event, render by `occurredAt` when you need wall-clock ordering; never assume the animation is a perfectly serialized distributed trace.

Example event:

```json
{
  "eventId": 41,
  "testRunId": "51f88be4-d81e-4f11-8e8f-722f5fa69e0c",
  "recordId": "e6c72e99-89f7-5d84-a7d8-c7a9b6d81a9e",
  "stage": "bloom_checked",
  "status": "possible_match",
  "detail": {
    "possibleMatch": true,
    "note": "Bloom is probabilistic; SQL decides the result.",
    "windowSeconds": 3600,
    "bucketSeconds": 300
  },
  "occurredAt": "2026-10-25T10:10:00+00:00",
  "visualNode": "redis_bloom"
}
```

### Inspect one selected record

`GET /test-record?testRunId=<UUID>&recordId=<UUID>` returns status, fingerprint, lifecycle timestamps and a per-record timeline. Payload text is omitted by default; explicitly pass `includePayload=true` only when the user chooses to inspect it.

### Poll counters

`GET /test-status?testRunId=<UUID>` returns `expectedCount`, `submittedCount`, `queuedCount`, `processedCount`, `acceptedCount`, `duplicateCount`, `failedCount`, `pendingCount`, `progressPercent`, and `stageCounts`. Stop polling when `status` is `completed`.

### Update the global dashboard

`GET /lab/overview` returns last-hour counters, recent runs, Redis/SQL health and Service Bus active/dead-letter/scheduled message counts. Poll slower (e.g. every 5–10 seconds) than the per-run event feed.

### Download output

`GET /test-output?testRunId=<UUID>` returns only records accepted during this run as UTF-8 `text/plain`; the response uses `Content-Disposition` for download. It returns HTTP 409 while the run is still processing, so the frontend should wait for `/test-status` to report `completed`. A run containing only duplicates can legitimately return an empty file because the one-hour window applies across runs.

## Stage-to-node mapping

| `stage` | Canvas node | Display meaning |
|---|---|---|
| `ingest_received` | client | Backend received this record. |
| `validated` | api | Request shape and record type passed validation. |
| `registered` | sql_tracking | Stable run/record identity is recorded for tracking. |
| `servicebus_queued` | service_bus | Queue send succeeded. |
| `worker_received` | function_worker | A Service Bus-triggered worker started the record. |
| `fingerprint_generated` | fingerprint | Show SHA-256 fingerprint from `detail.fingerprint`. |
| `bloom_checked` | redis_bloom | Show “possibly seen”, “no match”, or “fallback”; it is not the final result. |
| `sql_decision` | azure_sql | Final authoritative `accepted` or `duplicate` result. |
| `output_route` | accepted_output / duplicate_sink | Animate to the final branch. |
| `processing_failed` | retry/queue | A processing attempt failed and may be retried. |
| `dead_lettered` | dead_letter | Service Bus gave up after retries; final failed status is recorded. |

## Critical frontend semantics

- A Bloom “possible match” is not synonymous with duplicate. Show it as a provisional yellow/blue state; only `sql_decision` decides the result.
- If Redis is unavailable, the message can still reach SQL. Show `unavailable_fallback` and keep the path moving.
- If SQL or Service Bus is unavailable, block new runs until health recovers.
- Use output only after the run completes. It is scoped to the current `testRunId`, not the entire database.
- The backend fingerprints exact text lines. Trim only line terminators from an uploaded file; do not trim leading/trailing spaces unless the user explicitly opts into normalization.
- Do not display full sensitive payloads automatically. The app should let a user select a line to inspect it and should avoid retaining private personal data unnecessarily.
