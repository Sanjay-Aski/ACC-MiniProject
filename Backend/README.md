# Azure Deduplication Virtual Lab — deployment kit

This repository is a runnable starter for the pipeline described in `project-summary`:

**TXT/HTTP → authenticated HTTPS Function API → Service Bus queue → Function worker → Redis Bloom hint → transactional SQL decision → accepted-output file**, with persisted timeline events for a frontend virtual-lab visualization.

The frontend is not included; `openapi.yaml` and `docs/frontend-contract.md` define the contract for it. The deployment is a protected prototype: function keys must remain server-side. It is not yet a per-user multi-tenant public service; add Entra/JWT/API gateway and tenant isolation before opening it to unrelated users. The backend includes persisted stage events and a per-record inspector so your frontend can render the actual route taken by each message. The phased runbook is in `docs/implementation-plan.md`.

## Important behavior

- SQL is the source of truth for the one-hour deduplication window. Redis Bloom matches are only hints and never cause rejection on their own.
- Plain-text payloads are fingerprinted exactly, including spaces. JSON payloads use stable key ordering, ignore top-level event metadata (`event_id`, `eventId`, `event_time`, `eventTime`, and `timestamp`), normalize `event_name`, map `payment_failure` → `payment_failed` and `payment_successful` → `payment_success`, and match order events by event type, user ID, and order ID. The 100-event sample with 50 duplicate pairs is `examples/event-duplicates-100.json`.
- The Bloom filter uses rotating five-minute buckets and an hourly default window.
- The queue is asynchronous; `POST /api/ingest` returns `202`, and clients poll run status/events until the run is complete. The `queuedCount` is a historical count of records whose queue send was confirmed, while `pendingCount` is the current unresolved registered/queued population.
- Each stage is saved in `dbo.ProcessingEvents` so the frontend can replay actual persisted events instead of faking a pipeline animation.
- Redis failure is a degraded state: the worker records a fallback event and still makes the deduplication decision in SQL. SQL failure is retryable; messages are only marked permanently failed after they are seen in the Service Bus dead-letter queue by a timer reconciliation function.

## Resources the deployment script creates

`scripts/deploy.sh` provisions a *new dedicated lab resource group* containing:

- Storage account required by Azure Functions
- Service Bus Standard namespace and `dedup-ingestion` queue
- Azure SQL logical server and Basic database
- Azure Managed Redis (`Balanced_B0` default, encrypted transport, Entra authentication)
- Linux Azure Functions Flex Consumption app running Python 3.13
- System-assigned managed identity and scoped Service Bus / Redis permissions
- SQL schema and permissions for the Function App identity
- Published Python Function code

**Cost / cleanup:** this creates billable cloud resources. Redis and Service Bus have ongoing charges; Azure SQL can also incur charges. The script requires typing `DEPLOY` before it creates anything. It prints a resource-group deletion command at the end. Before deleting, double-check that the group contains only the lab resources you want removed.

## Prerequisites

1. Azure CLI signed in to the intended subscription (`az login` and `az account show`). Your account needs permission to create resources, assign roles, configure the SQL Entra administrator, and add Redis data-access assignments.
2. Azure Functions Core Tools v4 (`func`).
3. Python 3.13 and an active virtual environment.
4. A working network connection to Azure SQL. The setup script adds the current workstation IP if it can discover it, plus an Azure-services firewall rule for this **development lab only**.
5. Azure region capacity for Azure Managed Redis `Balanced_B0`. Change `AZURE_LOCATION` or `REDIS_SKU` if that SKU is not available in your chosen region/subscription.

## Deploy from Windows Git Bash

Open Git Bash and go to this folder. You can extract it anywhere; spaces in the parent path are okay.

```bash
python -m venv myenv
source myenv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -c "import sys,mssql_python; print(sys.executable); print('mssql-python import OK')"
az login
az account show --output table
```

Make sure the printed Python path points inside `myenv` and all dependencies import successfully. This kit uses Microsoft’s `mssql-python` package, which bundles the SQL driver and uses `ActiveDirectoryDefault` locally / `ActiveDirectoryMSI` in Azure, so you do not need to install the ODBC driver separately. Then run:

```bash
bash scripts/deploy.sh
```

You can override names and deployment region through environment variables:

```bash
export AZURE_LOCATION=westus
export REDIS_SKU=Balanced_B0
bash scripts/deploy.sh
```

The deployment script uses `westus` by default and passes that same location to every newly created resource. It creates a fresh unique resource group by default and does **not** overwrite the existing `rg-dedup-dev` environment. The default SQL bootstrap allows Azure-hosted clients through the SQL firewall for lab convenience; for production, use private networking and a least-privilege configuration instead.

If provisioning stops partway due subscription quota, regional SKU availability, or permission limits, some resources may already exist and may be billed. Read the error, inspect the created resource group, and clean up that dedicated group if you do not intend to continue.

## Apply schema manually (only if the script says schema setup failed)

Ensure the account you used with `az login` is the SQL Server Microsoft Entra administrator and that the current IP is allowed through the firewall. From the repository root:

```bash
export SQL_SERVER="<server-name>.database.windows.net"
export SQL_DATABASE="sqldb-dedup"
export FUNCTION_APP_NAME="<function-app-name>"
python scripts/apply_schema.py
```

This applies `schema.sql` and grants the Function identity table access + execution rights for `dbo.ProcessDedupRecord`. Do not run this with an unrelated SQL Server/database.

## Local development

For local Functions execution, have Azurite running because `AzureWebJobsStorage` uses `UseDevelopmentStorage=true` in the example. Copy the example settings and replace placeholders; keep `local.settings.json` out of source control. The database connection uses Microsoft’s `mssql-python` driver and `ActiveDirectoryDefault` locally, which should use your Azure CLI sign-in.

```bash
cp local.settings.example.json local.settings.json
func start
```

Local access uses Azure CLI credentials via `DefaultAzureCredential`. The signed-in developer must be added as a Redis data-access user and have appropriate SQL permissions. For the Service Bus trigger, the local binding uses the same `ServiceBusConnection__fullyQualifiedNamespace` setting and your signed-in developer identity; in Azure the deployed app uses its system-assigned managed identity. Do not add a `ServiceBusConnection__credential=AzureCli` setting unless you have verified that your installed host extension explicitly supports it.

## Verify the deployed service

After deployment, the script writes `deployment-summary.json` using the Function App's actual `defaultHostName` (Flex Consumption hostnames can include a generated suffix). To inspect the registered functions and test both dependency endpoints, set the names printed by deployment and run:

```bash
export RESOURCE_GROUP="<resource-group-from-deployment>"
export FUNCTION_APP_NAME="<function-app-from-deployment>"
bash scripts/verify-deployment.sh
```

## Smoke test with a TXT input

Set the deployed URL and a function key in the environment (don't paste the key into a frontend bundle):

```bash
export DEDUP_API_URL="https://<defaultHostName-from-deployment-summary.json>/api"
export DEDUP_FUNCTION_KEY="<function-key>"
python test_runner.py examples/duplicates.txt --limit 5 --rate 0 \
  --output output.txt --report report.json
```

For local execution, keep `DEDUP_API_URL=http://localhost:7071/api`. The runner calls health, uploads TXT lines in batches, polls test status, downloads the accepted output, and writes a JSON report. Existing fingerprints accepted in the preceding hour may cause all lines to be duplicates; that is valid behavior for a sliding-window deduper.

## API endpoints

| Endpoint | Verb | Purpose |
|---|---|---|
| `/api/health` | GET | Checks SQL, Redis, and Service Bus connectivity; a Redis-only outage is reported as degraded and does not block processing if SQL and Service Bus are ready. |
| `/api/ingest` | POST | Register records and enqueue them asynchronously. Max 100 records/request. |
| `/api/test-status?testRunId=<UUID>` | GET | Run counts, progress, and stage summaries. |
| `/api/test-events?testRunId=<UUID>&afterEventId=0` | GET | Ordered event stream for frontend visualization. Poll using `nextAfterEventId`. |
| `/api/test-record?testRunId=<UUID>&recordId=<UUID>` | GET | Detailed selected-record inspector: fingerprint, final decision and timeline; payload excluded by default. |
| `/api/test-output?testRunId=<UUID>` | GET | Downloads accepted lines for that run only as `text/plain`; returns HTTP 409 until the run is complete. |
| `/api/lab/overview` | GET | Aggregate recent activity, dependency status, queue counts and recent runs. |

See `openapi.yaml` for the request/response schema. A minimal ingestion request is:

```json
{
  "testRunId": "51f88be4-d81e-4f11-8e8f-722f5fa69e0c",
  "expectedCount": 3,
  "records": [
    {"lineNumber": 1, "payload": "amit@example.com"},
    {"lineNumber": 2, "payload": "rahul@example.com"},
    {"lineNumber": 3, "payload": "amit@example.com"}
  ]
}
```

Send the `x-functions-key` header from a trusted backend for this starter. Do not embed a Function key in browser JavaScript or a public frontend repository. For real multi-user/public deployment, place an API gateway/authorized backend in front and validate caller identity (Microsoft Entra ID/JWT); add per-user tenant scoping/rate limits before allowing different customers to share the database.

## Frontend virtual-lab contract

Read `docs/frontend-contract.md`. Its primary animation feed is the persisted event sequence. Each event includes a `visualNode` value such as `client`, `service_bus`, `fingerprint`, `redis_bloom`, `azure_sql`, `accepted_output`, or `duplicate_sink`. The frontend can animate edges based on those events and show the fingerprint and Bloom hint from `detail`, then treat `sql_decision` as final.

## Monitoring

This deployment does not create Log Analytics or Application Insights resources. Use the Function App deployment logs and the dependency health endpoint for basic verification.

## API security and production notes

- Function-level keys are a starter authentication mechanism, not per-customer identity. Keep keys in environment variables/server-side secrets.
- The lab's SQL public firewall setting is for development only. Use private endpoints/VNet integration and a narrow firewall strategy in production.
- For long-running production workloads, introduce an outbox/reconciliation mechanism between SQL registration and queue publish; this starter leaves `registered` records retryable and logs queue failures, but does not provide a distributed transaction across SQL and Service Bus.
- Add per-tenant database scoping, pagination, retention limits and resource-cost monitoring before opening the service to unrelated customers.
- Bloom false positives are expected by design. Always show them as “possibly present” and wait for the SQL decision before drawing the accepted/duplicate destination.

```
python test_runner.py examples/duplicates.txt \
  --base-url "$DEDUP_API_URL" \
  --function-key "$DEDUP_FUNCTION_KEY" \
  --limit 5 \
  --rate 0 \
  --output output.txt \
  --report report.json
  ```