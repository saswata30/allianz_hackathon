#!/usr/bin/env bash
# =============================================================================
# Provision the Lakebase (Autoscaling Postgres) project that lands claims data.
# Idempotent: safe to re-run — skips creation if the project already exists.
# Requires: Databricks CLI >= 0.285.0, an authenticated profile.
#   source config/settings.env && bash setup/00_provision_lakebase.sh
# =============================================================================
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${here}/config/settings.env"

P="-p ${DATABRICKS_PROFILE}"
PROJ="projects/${LB_PROJECT}"
BRANCH="${PROJ}/branches/${LB_BRANCH}"
ENDPOINT="${BRANCH}/endpoints/${LB_ENDPOINT}"

echo ">> CLI: $(databricks --version)"

if databricks postgres get-project "${PROJ}" ${P} -o json >/dev/null 2>&1; then
  echo ">> Project ${PROJ} already exists — skipping create."
else
  echo ">> Creating project ${LB_PROJECT} ..."
  databricks postgres create-project "${LB_PROJECT}" \
    --json "{\"spec\": {\"display_name\": \"Allianz Hackathon — Claims OLTP\"}}" \
    ${P} >/dev/null
fi

echo ">> Waiting for branch + endpoint to become ready ..."
for i in $(seq 1 60); do
  bstate=$(databricks postgres list-branches "${PROJ}" ${P} -o json 2>/dev/null | jq -r '.[0].status.current_state // "PENDING"')
  estate=$(databricks postgres list-endpoints "${BRANCH}" ${P} -o json 2>/dev/null | jq -r '.[0].status.current_state // "PENDING"')
  echo "   [$i] branch=${bstate} endpoint=${estate}"
  if [[ "${estate}" == "ACTIVE" || "${estate}" == "IDLE" ]]; then break; fi
  sleep 5
done

echo ">> Scaling endpoint to ${LB_MIN_CU}-${LB_MAX_CU} CU ..."
databricks postgres update-endpoint "${ENDPOINT}" \
  "spec.autoscaling_limit_min_cu,spec.autoscaling_limit_max_cu" \
  --json "{\"spec\": {\"autoscaling_limit_min_cu\": ${LB_MIN_CU}, \"autoscaling_limit_max_cu\": ${LB_MAX_CU}}}" \
  ${P} >/dev/null || echo "   (scale step skipped — endpoint may already match)"

HOST=$(databricks postgres list-endpoints "${BRANCH}" ${P} -o json | jq -r '.[0].status.hosts.host')
echo ""
echo ">> Lakebase ready."
echo "   project : ${PROJ}"
echo "   endpoint: ${ENDPOINT}"
echo "   host    : ${HOST}"
echo ""
echo "Next: python setup/01_create_postgres_schema.py"
