#!/usr/bin/env bash
# =============================================================================
# 1) Register the Lakebase Postgres database into Unity Catalog  == ZERO-COPY ==
#    (a UC "database catalog" over Lakebase — query live OLTP rows from the
#     lakehouse with no ETL/no data movement).
# 2) Create the medallion catalog + bronze/silver/gold/reference schemas.
#
#   source config/settings.env && bash setup/02_register_uc_catalog.sh
# =============================================================================
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${here}/config/settings.env"
P="-p ${DATABRICKS_PROFILE}"

echo ">> [zero-copy] registering Lakebase '${LB_PROJECT}' as UC catalog '${LAKEBASE_UC_CATALOG}' ..."
# Autoscaling Lakebase exposes its database to UC as a database catalog. If your
# workspace/CLI names this differently, create it from the UI:
#   Catalog Explorer > Create catalog > type "Database (Lakebase)" > pick project/database.
if databricks catalogs get "${LAKEBASE_UC_CATALOG}" ${P} -o json >/dev/null 2>&1; then
  echo "   catalog '${LAKEBASE_UC_CATALOG}' already exists — skipping."
else
  databricks database create-database-catalog "${LAKEBASE_UC_CATALOG}" "${LB_PROJECT}" "${LB_DATABASE}" \
    --create-database-if-not-exists ${P} >/dev/null 2>&1 \
    || echo "   !! CLI registration failed — register the Lakebase catalog via Catalog Explorer UI (see comment above), then re-run the rest."
fi

echo ">> creating medallion catalog + schemas ..."
databricks catalogs create "${UC_CATALOG}" ${P} >/dev/null 2>&1 || echo "   catalog ${UC_CATALOG} exists"
for s in "${BRONZE_SCHEMA}" "${SILVER_SCHEMA}" "${GOLD_SCHEMA}" "${REFERENCE_SCHEMA}"; do
  databricks schemas create "${s}" "${UC_CATALOG}" ${P} >/dev/null 2>&1 && echo "   + ${UC_CATALOG}.${s}" \
    || echo "   ${UC_CATALOG}.${s} exists"
done

echo ""
echo ">> Done. Verify zero-copy read from a Databricks SQL editor / notebook:"
echo "     SELECT count(*) FROM ${LAKEBASE_UC_CATALOG}.${LB_SCHEMA}.${LB_TABLE};"
