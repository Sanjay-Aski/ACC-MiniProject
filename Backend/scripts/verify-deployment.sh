#!/usr/bin/env bash
set -Eeuo pipefail

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1" >&2; exit 1; }; }
need az
need func
need curl

FUNCTION_APP="${FUNCTION_APP_NAME:?Set FUNCTION_APP_NAME to the deployed Function App name}"
RG="${RESOURCE_GROUP:?Set RESOURCE_GROUP to its resource group}"
BASE_HOST="$(az functionapp show --resource-group "$RG" --name "$FUNCTION_APP" --query defaultHostName -o tsv)"
if [[ -z "$BASE_HOST" || "$BASE_HOST" == "None" ]]; then
  # Flex Consumption can omit defaultHostName from this CLI response even though
  # the public Function hostname remains the standard Azure App Service hostname.
  BASE_HOST="${FUNCTION_APP}.azurewebsites.net"
fi
BASE_URL="https://${BASE_HOST}"
KEY="${DEDUP_FUNCTION_KEY:-$(az functionapp keys list --resource-group "$RG" --name "$FUNCTION_APP" --query functionKeys.default -o tsv)}"
[[ -n "$KEY" && "$KEY" != "None" ]] || { echo "Could not obtain a Function key" >&2; exit 3; }

headers=(-H "x-functions-key: $KEY" -H 'Accept: application/json')
echo "Function host: $BASE_URL"
echo "Registered Azure Functions:"
func azure functionapp list-functions "$FUNCTION_APP"

echo
echo "Health response:"
curl --fail-with-body --silent --show-error "${headers[@]}" "$BASE_URL/api/health"
echo

echo "Lab overview response:"
curl --fail-with-body --silent --show-error "${headers[@]}" "$BASE_URL/api/lab/overview"
echo

echo "Verification complete. This checks API reachability/dependencies, not a full TXT run."
echo "Run test_runner.py for an end-to-end ingestion/processing/output test."
