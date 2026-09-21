#!/usr/bin/env bash
# =============================================================================
# End-to-end orchestrator for the Allianz claims streaming workshop.
# Runs the whole pipeline against the workspace in config/settings.env.
#
#   source config/settings.env
#   bash scripts/run_end_to_end.sh
#
# Steps 1-4 are fully automated via CLI. Steps 5-7 (DLT pipeline, dashboard,
# Genie) are created via CLI where possible and otherwise printed as the exact
# manual actions. Re-runnable: every step is idempotent.
# =============================================================================
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${here}/config/settings.env"
P="-p ${DATABRICKS_PROFILE}"

echo "==================================================================="
echo " Allianz Hackathon — end-to-end run"
echo " workspace: ${DATABRICKS_HOST}"
echo "==================================================================="

echo; echo "### 0) Auth check"
databricks current-user me ${P} -o json | jq -r '"   authenticated as \(.userName)"'

echo; echo "### 1) Provision Lakebase (Postgres)"
bash "${here}/setup/00_provision_lakebase.sh"

echo; echo "### 2) Create Postgres schema"
python3 "${here}/setup/01_create_postgres_schema.py"

echo; echo "### 3) Register zero-copy UC catalog + medallion schemas"
bash "${here}/setup/02_register_uc_catalog.sh"

echo; echo "### 4) Load firmwide reference (Delta)"
if [[ -n "${SQL_WAREHOUSE_ID}" ]]; then
  databricks sql-statements execute \
    --warehouse-id "${SQL_WAREHOUSE_ID}" \
    --statement "$(cat "${here}/reference/firmwide_reference.sql")" ${P} -o json \
    | jq -r '.status.state // "SUBMITTED"' | sed 's/^/   firmwide load: /'
else
  echo "   !! SQL_WAREHOUSE_ID not set — run reference/firmwide_reference.sql in a SQL editor."
fi

echo; echo "### 5) Import notebooks to the workspace"
WS_DIR="/Workspace/Users/$(databricks current-user me ${P} -o json | jq -r .userName)/allianz_hackathon"
databricks workspace mkdirs "${WS_DIR}/pipelines" ${P} 2>/dev/null || true
for nb in pipelines/01_bronze_ingest pipelines/medallion_dlt; do
  databricks workspace import "${WS_DIR}/${nb}" \
    --file "${here}/${nb}.py" --language PYTHON --format SOURCE --overwrite ${P} \
    && echo "   imported ${nb}"
done
export WORKSPACE_DIR="${WS_DIR}"

echo; echo "### 6) Create the Lakeflow (DLT) medallion pipeline"
envsubst < "${here}/pipelines/pipeline.json" > /tmp/pipeline.rendered.json
databricks pipelines create --json @/tmp/pipeline.rendered.json ${P} -o json \
  | jq -r '"   pipeline_id: \(.pipeline_id)"' || echo "   (pipeline may already exist — check 'databricks pipelines list-pipelines')"

echo; echo "### 7) Create the bronze ingest job (every 2 min)"
envsubst < "${here}/pipelines/bronze_job.json" > /tmp/bronze_job.rendered.json
databricks jobs create --json @/tmp/bronze_job.rendered.json ${P} -o json \
  | jq -r '"   job_id: \(.job_id)"' || echo "   (job may already exist — check 'databricks jobs list')"

echo; echo "==================================================================="
echo " Automated steps done. Now:"
echo "   * Start the generator:   python3 streaming/claims_generator.py"
echo "   * Start the DLT pipeline: databricks pipelines start-update <pipeline_id> ${P}"
echo "   * Build the dashboard:    see README §7 (dashboard/queries.sql)"
echo "   * Create the Genie space: see genie/genie_space_setup.md"
echo "==================================================================="
