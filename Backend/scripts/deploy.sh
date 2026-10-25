#!/usr/bin/env bash
# Creates an isolated dev stack, assigns identities/roles, applies SQL schema, and deploys Function code.
# Run from Git Bash / WSL. This script creates billable Azure resources.
set -Eeuo pipefail

# Prevent Git Bash from converting Azure resource IDs such as
# /subscriptions/... into local Windows paths.
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

fail() { echo "ERROR: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"; }

need az
need func
need python
# The redisenterprise command group is delivered as an Azure CLI extension.
# Allow the CLI to install the official extension without stopping for an interactive prompt.
az config set extension.use_dynamic_install=yes_without_prompt >/dev/null

if ! az account show >/dev/null 2>&1; then
  echo "Azure CLI is not signed in. Starting az login..."
  az login
fi

# Ensure the active interpreter has dependencies. mssql-python bundles the SQL driver,
# avoiding a separate ODBC Driver installation on the workstation and Linux Function host.
python - <<'PY' || exit 1
import sys
print("Python:", sys.executable)
for module in ("mssql_python", "azure.functions", "azure.identity", "azure.servicebus", "redis_entraid"):
    try:
        __import__(module)
    except Exception as e:
        raise SystemExit(f"{module} is missing from this Python environment. Run: python -m pip install -r requirements.txt ({type(e).__name__}: {e})")
PY

SUBSCRIPTION_ID="${AZURE_SUBSCRIPTION_ID:-$(az account show --query id -o tsv)}"
LOCATION="${AZURE_LOCATION:-westus}"
STAMP="$(date +%m%d%H%M)$(printf '%03d' "$((RANDOM % 1000))")"
SUFFIX="${DEDUP_SUFFIX:-$STAMP}"
RG="${RESOURCE_GROUP:-rg-dedup-lab-${SUFFIX}}"
STORAGE="${STORAGE_ACCOUNT:-stdedup${SUFFIX}}"
SB="${SERVICEBUS_NAMESPACE:-sb-dedup-lab-${SUFFIX}}"
QUEUE="${SERVICEBUS_QUEUE:-dedup-ingestion}"
SQL_SERVER="${SQL_SERVER_NAME:-sqldedup${SUFFIX}}"
SQL_DB="${SQL_DATABASE:-sqldb-dedup}"
REDIS="${REDIS_CLUSTER:-redis-dedup-lab-${SUFFIX}}"
FUNCTION_APP="${FUNCTION_APP_NAME:-func-dedup-lab-${SUFFIX}}"
SQL_ADMIN_LOGIN="${SQL_ADMIN_LOGIN:-dedupadmin${SUFFIX}}"

# Guard naming constraints before the first create call.
[[ "$STORAGE" =~ ^[a-z0-9]{3,24}$ ]] || fail "Storage account name must be 3-24 lowercase letters/numbers: $STORAGE"
[[ "$SQL_SERVER" =~ ^[a-z0-9-]{1,63}$ ]] || fail "Invalid SQL server name: $SQL_SERVER"
[[ "$FUNCTION_APP" =~ ^[a-zA-Z0-9-]{2,60}$ ]] || fail "Invalid Function App name: $FUNCTION_APP"

cat <<EOF

Azure Dedup Virtual Lab deployment
Subscription: $SUBSCRIPTION_ID
Region:       $LOCATION
Resource RG:  $RG
Storage:      $STORAGE
Service Bus:  $SB / queue $QUEUE
Managed Redis:$REDIS (SKU ${REDIS_SKU:-Balanced_B0})
SQL:          $SQL_SERVER / $SQL_DB (Basic)
Function App: $FUNCTION_APP (Flex Consumption, Python 3.13)

This will create billable Azure resources. Managed Redis and Service Bus have ongoing charges.
The lab uses public HTTPS endpoints with Function keys; do not put the Function key in browser JavaScript.
EOF
read -r -p 'Type DEPLOY to continue: ' CONFIRM
[[ "$CONFIRM" == "DEPLOY" ]] || fail "Cancelled; no resources were created."

# SQL credentials are used only to create the logical server; app connections use Entra ID tokens.
# Validate this before creating any resources so a typo does not leave a partially billed lab.
SQL_ADMIN_PASSWORD="${SQL_ADMIN_PASSWORD:-}"
if [[ -z "$SQL_ADMIN_PASSWORD" ]]; then
  read -r -s -p "Choose SQL bootstrap admin password for login '$SQL_ADMIN_LOGIN': " SQL_ADMIN_PASSWORD; echo
  read -r -s -p "Confirm password: " SQL_ADMIN_PASSWORD_2; echo
  [[ "$SQL_ADMIN_PASSWORD" == "$SQL_ADMIN_PASSWORD_2" ]] || fail "Passwords did not match; no Azure resources have been created."
  unset SQL_ADMIN_PASSWORD_2
fi
(( ${#SQL_ADMIN_PASSWORD} >= 12 )) || fail "SQL admin password must be at least 12 characters; no Azure resources have been created."
[[ "$SQL_ADMIN_PASSWORD" =~ [A-Z] && "$SQL_ADMIN_PASSWORD" =~ [a-z] && "$SQL_ADMIN_PASSWORD" =~ [0-9] && "$SQL_ADMIN_PASSWORD" =~ [^A-Za-z0-9] ]] || fail "SQL admin password must include uppercase, lowercase, digit, and symbol; no Azure resources have been created."

az account set --subscription "$SUBSCRIPTION_ID"
echo "[1/11] Creating resource group..."
az group create --name "$RG" --location "$LOCATION" --tags app=azure-dedup-virtual-lab environment=dev -o none

echo "[2/11] Creating storage for Functions..."
az storage account create --name "$STORAGE" --resource-group "$RG" --location "$LOCATION" \
  --sku Standard_LRS --kind StorageV2 --min-tls-version TLS1_2 --allow-blob-public-access false -o none
STORAGE_CONN="$(az storage account show-connection-string --name "$STORAGE" --resource-group "$RG" --query connectionString -o tsv)"


echo "[3/11] Creating Service Bus namespace and queue..."
az servicebus namespace create --resource-group "$RG" --name "$SB" --location "$LOCATION" --sku Standard -o none
az servicebus queue create --resource-group "$RG" --namespace-name "$SB" --name "$QUEUE" \
  --max-size 1024 --lock-duration PT5M --max-delivery-count 10 \
  --enable-dead-lettering-on-message-expiration true -o none
SB_FQNS="${SB}.servicebus.windows.net"
SB_ID="$(az servicebus namespace show --resource-group "$RG" --name "$SB" --query id -o tsv)"
SB_QUEUE_ID="${SB_ID}/queues/${QUEUE}"

echo "[4/11] Creating Azure SQL server/database..."
az sql server create --resource-group "$RG" --name "$SQL_SERVER" --location "$LOCATION" \
  --admin-user "$SQL_ADMIN_LOGIN" --admin-password "$SQL_ADMIN_PASSWORD" --enable-public-network true --minimal-tls-version 1.2 -o none
unset SQL_ADMIN_PASSWORD
az sql db create --resource-group "$RG" --server "$SQL_SERVER" --name "$SQL_DB" --edition Basic --service-objective Basic -o none

# Development convenience only: allow Azure-hosted clients through the SQL firewall.
# Entra authentication and SQL permissions still apply. For production, use private networking.
if [[ "${ALLOW_AZURE_SERVICES_FIREWALL:-true}" == "true" ]]; then
  az sql server firewall-rule create --resource-group "$RG" --server "$SQL_SERVER" \
    --name AllowAzureServicesForLab --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0 -o none
fi
# Allow the deploying workstation to apply schema, if its public IP can be discovered.
if command -v curl >/dev/null 2>&1; then
  DEV_IP="$(curl -fsS --max-time 5 https://api.ipify.org 2>/dev/null || true)"
  if [[ "$DEV_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    az sql server firewall-rule create --resource-group "$RG" --server "$SQL_SERVER" \
      --name "DeveloperIP${SUFFIX: -6}" --start-ip-address "$DEV_IP" --end-ip-address "$DEV_IP" -o none || true
  fi
fi
SQL_SERVER_FQDN="${SQL_SERVER}.database.windows.net"

echo "[5/11] Creating Azure Managed Redis..."
# The Azure CLI installs the redisenterprise extension when first required.
az redisenterprise create --resource-group "$RG" --cluster-name "$REDIS" --location "$LOCATION" \
  --sku "${REDIS_SKU:-Balanced_B0}" --minimum-tls-version 1.2 --public-network-access Enabled -o none
az redisenterprise database create --resource-group "$RG" --cluster-name "$REDIS" \
  --client-protocol Encrypted --clustering-policy OSSCluster --port 10000 \
  --access-keys-authentication Disabled --eviction-policy NoEviction -o none
REDIS_HOST="$(az redisenterprise show --resource-group "$RG" --cluster-name "$REDIS" --query hostName -o tsv)"
[[ -n "$REDIS_HOST" && "$REDIS_HOST" != "None" ]] || fail "Could not read the Redis hostname from Azure. Check az redisenterprise show output."

echo "[6/11] Creating Python 3.13 Function App on Flex Consumption..."
az functionapp create --resource-group "$RG" --name "$FUNCTION_APP" --storage-account "$STORAGE" \
  --flexconsumption-location "$LOCATION" --runtime python --runtime-version 3.13 --functions-version 4 -o none
FUNCTION_PRINCIPAL="$(az functionapp identity assign --resource-group "$RG" --name "$FUNCTION_APP" --query principalId -o tsv)"
[[ -n "$FUNCTION_PRINCIPAL" && "$FUNCTION_PRINCIPAL" != "None" ]] || fail "Could not enable Function App managed identity."

echo "[7/11] Granting Function identity Service Bus access..."
# The worker needs send + receive on the queue. Data Owner at this dedicated namespace
# scope also allows the Functions host to read runtime properties for correct scaling and
# lets the lab overview query active/dead-letter counts. This namespace contains only this lab queue.
az role assignment create --assignee-object-id "$FUNCTION_PRINCIPAL" --assignee-principal-type ServicePrincipal \
  --role "Azure Service Bus Data Sender" --scope "$SB_QUEUE_ID" -o none
az role assignment create --assignee-object-id "$FUNCTION_PRINCIPAL" --assignee-principal-type ServicePrincipal \
  --role "Azure Service Bus Data Receiver" --scope "$SB_QUEUE_ID" -o none
az role assignment create --assignee-object-id "$FUNCTION_PRINCIPAL" --assignee-principal-type ServicePrincipal \
  --role "Azure Service Bus Data Owner" --scope "$SB_ID" -o none

echo "[8/11] Granting Redis Entra data access..."
# Access assignment is a Redis data-plane ACL, distinct from Azure control-plane RBAC.
az redisenterprise database access-policy-assignment create --resource-group "$RG" --cluster-name "$REDIS" \
  --database-name default --access-policy-assignment-name "function-${SUFFIX}" \
  --object-id "$FUNCTION_PRINCIPAL" --access-policy-name default -o none
# Local developer identity also needs Redis data access for local testing.
DEV_OBJECT_ID="$(az ad signed-in-user show --query id -o tsv 2>/dev/null || true)"
if [[ -n "$DEV_OBJECT_ID" && "$DEV_OBJECT_ID" != "None" && "$DEV_OBJECT_ID" != "$FUNCTION_PRINCIPAL" ]]; then
  az redisenterprise database access-policy-assignment create --resource-group "$RG" --cluster-name "$REDIS" \
    --database-name default --access-policy-assignment-name "developer-${SUFFIX}" \
    --object-id "$DEV_OBJECT_ID" --access-policy-name default -o none || \
    echo "WARNING: could not add the local developer to Redis; add the signed-in user in Azure Portal > Redis > Authentication."
fi

echo "[9/11] Configuring SQL Entra administrator and app settings..."
ENTRA_ID="$(az ad signed-in-user show --query id -o tsv 2>/dev/null || true)"
ENTRA_NAME="$(az ad signed-in-user show --query displayName -o tsv 2>/dev/null || true)"
if [[ -z "$ENTRA_ID" || "$ENTRA_ID" == "None" ]]; then
  read -r -p "Enter Entra admin object ID (from az ad signed-in-user show): " ENTRA_ID
  read -r -p "Enter Entra admin display name: " ENTRA_NAME
fi
az sql server ad-admin create --resource-group "$RG" --server "$SQL_SERVER" --display-name "$ENTRA_NAME" --object-id "$ENTRA_ID" -o none

az functionapp config appsettings set --resource-group "$RG" --name "$FUNCTION_APP" --settings \
  "AzureWebJobsStorage=$STORAGE_CONN" \
  "SERVICEBUS_QUEUE=$QUEUE" \
  "ServiceBusConnection__fullyQualifiedNamespace=$SB_FQNS" \
  "SQL_SERVER=$SQL_SERVER_FQDN" \
  "SQL_DATABASE=$SQL_DB" \
  "REDIS_HOST=$REDIS_HOST" \
  "REDIS_PORT=10000" \
  "WINDOW_SECONDS=3600" \
  "BUCKET_SECONDS=300" \
  "BLOOM_CAPACITY_PER_BUCKET=10000" \
  "BLOOM_FALSE_POSITIVE_RATE=0.01" \
  "MAX_RECORDS_PER_REQUEST=100" -o none

# Add current Function outbound addresses to the SQL firewall, in addition to the explicit Azure-services rule.
OUTBOUND_IPS="$(az functionapp show --resource-group "$RG" --name "$FUNCTION_APP" --query outboundIpAddresses -o tsv 2>/dev/null || true)"
if [[ -n "$OUTBOUND_IPS" && "$OUTBOUND_IPS" != "None" ]]; then
  IFS=',' read -r -a IP_ARRAY <<< "$OUTBOUND_IPS"
  idx=0
  for ip in "${IP_ARRAY[@]}"; do
    [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || continue
    idx=$((idx+1))
    az sql server firewall-rule create --resource-group "$RG" --server "$SQL_SERVER" \
      --name "FunctionOutbound${idx}" --start-ip-address "$ip" --end-ip-address "$ip" -o none >/dev/null 2>&1 || true
  done
fi

echo "[10/11] Applying SQL schema and granting Function identity permissions..."
export SQL_SERVER="$SQL_SERVER_FQDN" SQL_DATABASE="$SQL_DB" FUNCTION_APP_NAME="$FUNCTION_APP"
schema_ok=false
for attempt in $(seq 1 10); do
  # Use a repository-relative path so Git Bash does not rewrite /d/... into D:\d\....
  if python scripts/apply_schema.py; then schema_ok=true; break; fi
  echo "SQL bootstrap attempt $attempt/10 failed; retrying after Entra/RBAC propagation delay..."
  sleep 15
done
[[ "$schema_ok" == "true" ]] || fail "SQL schema/bootstrap did not succeed. Fix the reported error before publishing; no Function code was published."

echo "[11/11] Publishing Function code..."
func azure functionapp publish "$FUNCTION_APP" --python --build remote

FUNCTIONS_LIST=""
FUNCTIONS_OK=false
for attempt in $(seq 1 12); do
  FUNCTIONS_LIST="$(func azure functionapp list-functions "$FUNCTION_APP" 2>&1 || true)"
  if grep -qE '(^|[[:space:]])ingest([[:space:]]|$)' <<< "$FUNCTIONS_LIST" \
    && grep -qE 'dedup_worker' <<< "$FUNCTIONS_LIST" \
    && grep -qE 'health' <<< "$FUNCTIONS_LIST" \
    && grep -qE 'test_events' <<< "$FUNCTIONS_LIST" \
    && grep -qE 'test_record' <<< "$FUNCTIONS_LIST" \
    && grep -qE 'test_status' <<< "$FUNCTIONS_LIST" \
    && grep -qE 'test_output' <<< "$FUNCTIONS_LIST" \
    && grep -qE 'lab_overview' <<< "$FUNCTIONS_LIST" \
    && grep -qE 'reconcile_deadletter' <<< "$FUNCTIONS_LIST"; then
    FUNCTIONS_OK=true
    break
  fi
  echo "Function indexing check $attempt/12; waiting for trigger synchronization..."
  sleep 10
done
echo "$FUNCTIONS_LIST"
[[ "$FUNCTIONS_OK" == "true" ]] || fail "Deployment returned, but one or more expected functions were not indexed. Inspect the Function App deployment logs before retrying."

DEFAULT_HOSTNAME="$(az functionapp show --resource-group "$RG" --name "$FUNCTION_APP" --query defaultHostName -o tsv)"
[[ -n "$DEFAULT_HOSTNAME" && "$DEFAULT_HOSTNAME" != "None" ]] || fail "Could not determine the Function App default hostname."
BASE_URL="https://${DEFAULT_HOSTNAME}"
cat > "$ROOT/deployment-summary.json" <<EOF
{
  "resourceGroup": "$RG",
  "location": "$LOCATION",
  "storageAccount": "$STORAGE",
  "serviceBusNamespace": "$SB_FQNS",
  "serviceBusQueue": "$QUEUE",
  "redisHost": "$REDIS_HOST",
  "sqlServer": "$SQL_SERVER_FQDN",
  "sqlDatabase": "$SQL_DB",
  "functionApp": "$FUNCTION_APP",
  "baseUrl": "$BASE_URL/api",
  "endpoints": {
    "health": "$BASE_URL/api/health",
    "ingest": "$BASE_URL/api/ingest",
    "labOverview": "$BASE_URL/api/lab/overview",
    "testStatus": "$BASE_URL/api/test-status?testRunId=<UUID>",
    "testEvents": "$BASE_URL/api/test-events?testRunId=<UUID>&afterEventId=0",
    "testRecord": "$BASE_URL/api/test-record?testRunId=<UUID>&recordId=<UUID>",
    "testOutput": "$BASE_URL/api/test-output?testRunId=<UUID>"
  }
}
EOF

echo
printf 'Deployment resources summary saved to %s/deployment-summary.json\n' "$ROOT"
echo "Base API URL: $BASE_URL/api"
echo "Retrieve a function key only when needed; do not place it in frontend JavaScript:"
echo "az functionapp keys list --resource-group '$RG' --name '$FUNCTION_APP' --query functionKeys.default -o tsv"
echo "To clean up this dedicated lab and stop charges, verify the resource group name, then run:"
echo "az group delete --name '$RG' --yes --no-wait"
echo "Deployment script complete. Wait for role assignments to propagate before smoke tests."
