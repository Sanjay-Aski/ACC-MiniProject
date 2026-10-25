# Implementation and execution plan

## Goal

Deploy one self-contained Azure resource group that hosts the API, queue, worker, Bloom pre-check, authoritative SQL decision, output endpoint, and monitoring. A separately generated frontend consumes the documented HTTP API and renders persisted pipeline events.

## Phase 1 — Provision the stack

Run `scripts/deploy.sh` from Git Bash after signing in with `az login`. It creates a fresh, uniquely named resource group and the supporting resources, then asks for explicit `DEPLOY` confirmation before provisioning. It does not modify the existing `rg-dedup-dev` stack. Stop and review if your Azure for Students quota or budget can't support another Redis and Service Bus resource.

## Phase 2 — Establish identity and schema

The script enables system-assigned identity on the Function App; grants Service Bus data access at queue scope; grants Redis Entra data-plane access; configures SQL Entra administration; applies the idempotent schema; and gives the Function identity the table/procedure permissions it needs. Local development uses Azure CLI authentication. Hosted execution uses managed identity.

## Phase 3 — Deploy and wire functions

The script publishes the root Python Functions project using remote build. Remote build is recommended for Python deployments from Windows because Azure builds dependencies in its Linux deployment environment. It verifies that the HTTP functions, Service Bus trigger, and timer function are indexed before it writes `deployment-summary.json`.

## Phase 4 — Verify the pipeline

Run `scripts/verify-deployment.sh` with `FUNCTION_APP_NAME` and `RESOURCE_GROUP`. Then run `test_runner.py` against `examples/duplicates.txt`. Check that health is ready, the queue receives messages, the worker generates fingerprints, Redis emits a Bloom hint, SQL emits the final decision, the status reaches `completed`, and output validation succeeds.

## Phase 5 — Connect the frontend

Use `openapi.yaml` and `docs/frontend-contract.md`. The visual animation should use `/test-events` as its event source and `/test-record` for the selected-record inspector. Poll `/test-status` for progress, `/lab/overview` for dashboard cards, and download output only after status is `completed`.

## Phase 6 — Monitoring and production hardening

Use Application Insights queries in `monitoring.kql` and Azure Monitor metric charts for Service Bus, SQL, and Managed Redis. Before public multi-user use, put a secure API gateway/backend in front of the Function endpoints. Do not bake a Function key into browser JavaScript. Add per-user/tenant authorization, rate limiting, quotas, retention, private network access, and durable queue-publication reconciliation before production.

## Pipeline stages visible to the frontend

`ingest_received → validated → registered → servicebus_queued → worker_received → fingerprint_generated → bloom_checked → sql_decision → output_route`

Failure paths emit `processing_failed` with retry state; permanent dead-letter observation emits `dead_lettered`. Event IDs come from SQL identity values. Because sender and worker run asynchronously, render stages using the event timestamp and event ID without assuming strict wall-clock ordering across separate Azure components.

## Deduplication invariants

- The SHA-256 fingerprint follows the documented text/JSON canonicalization rules.
- Redis Bloom results are probabilistic hints, never the final decision.
- SQL locks the fingerprint and transactionally writes the accepted or duplicate result.
- A stable record ID preserves idempotency across redelivery and retries.
- Output is scoped to the selected test run and is only downloadable after the run finishes.
