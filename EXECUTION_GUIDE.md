# Execution Guide — Allianz Claims Streaming Workshop

A step-by-step run book. Everything runs **inside Databricks** as notebooks — no CLI, no
profile, no workspace host. Each step lists **what to run**, **what you should see**, and a
**verify** check. Estimated total: **~35–45 min** (most of it watching the 2-min stream).

**Default path — Lakebase** (run in order):

| Phase | Notebook | ~Time |
|---|---|---|
| 0 | Add repo to workspace | 3 min |
| 1 | `01_provision_lakebase` | 3 min |
| 2 | `02_create_schema` | 1 min |
| 3 | `03_register_zero_copy` | 3 min |
| 4 | `04_firmwide_reference` | 2 min |
| 5 | `05_claims_generator` | 2 min |
| 6 | `06_bronze_ingest` + `07_medallion_no_dlt` | 10 min |
| 7 | `08_dashboard` | 5 min |
| 8 | `09_genie_setup` | 5 min |
| — | `10_deploy_pipeline_and_jobs` (automates 5–7) | 3 min |
| 9 | Demo / talk track | — |
| 10 | Teardown | 3 min |

**Optional path — Azure SQL Server** (run in order; a separate set of source notebooks, and
`01` is skipped). See *Optional Phase — land in Azure SQL Server* below for the detail:

| Phase | Notebook | ~Time |
|---|---|---|
| 0 | Add repo to workspace + fill `TBD` Azure SQL config in `00_config` | 4 min |
| 2a | `02a_create_schema_azuresql` | 1 min |
| 3 | `03_register_zero_copy` | 3 min |
| 4 | `04_firmwide_reference` | 2 min |
| 5a | `05a_claims_generator_azuresql` | 2 min |
| 6a | `06a_bronze_ingest_azuresql` + `07_medallion_no_dlt` | 10 min |
| 7 | `08_dashboard` | 5 min |
| 8 | `09_genie_setup` | 5 min |
| — | `10_deploy_pipeline_and_jobs` (`source=azuresql`; automates 5a/6a/7) | 3 min |
| 9 | Demo / talk track | — |
| 10 | Teardown | 3 min |

---

## Phase 0 — Add the repo to your workspace
- **Workspace → Repos → Add repo** → `https://github.com/saswata30/allianz_hackathon.git`
  (or **Git folder**). Open the `notebooks/` folder.
- Requirements: a **serverless** workspace with **Lakebase** enabled and rights to create
  catalogs, Lakebase instances, and jobs.
- Optionally edit `notebooks/00_config` if you want different names (defaults are fine).

---

## Phase 1 — Provision Lakebase · `01_provision_lakebase`
Run all cells. **You should see** the instance poll to `AVAILABLE` and print its read/write DNS.

**Verify:** the last cell prints `state=...AVAILABLE...`.
⏳ First creation takes 1–3 min. ⚠️ If the SDK call isn't available, create the instance once
via **Compute → Database instances → Create** (name `allianz-hackathon-db`, capacity `CU_1`)
and continue.

## Phase 2 — Postgres schema · `02_create_schema`
Run all cells (it `%pip install`s `psycopg2-binary`, restarts Python, then creates the DB/table).
**You should see** `created database 'claims_db'` and `claims.claim_transactions ready (14 columns)`.

**Verify:** re-run — it now prints `database 'claims_db' already exists` (idempotent).

## Phase 3 — Zero-copy UC catalog · `03_register_zero_copy`
Run all cells. Registers Lakebase as UC catalog **`lakebase_allianz`** and creates the
`allianz_hackathon` catalog + `bronze/silver/gold/reference` schemas.

**Verify:** the final cell displays `rows_in_lakebase = 0` — this SELECT reads **live Postgres**
with no copy. ⚠️ If SDK registration fails, use **Catalog Explorer → Create catalog →
Database (Lakebase)**, then re-run the schema + verify cells.

## Phase 4 — Firmwide reference · `04_firmwide_reference`
Run all cells. **Verify:** the final query shows 6 LOBs, 8 regions each (48 rows total).

---

## Phase 5 — Start the stream · `05_claims_generator`
- **Live demo:** set widget `mode = loop`, `cycles = 0` (forever), run the last cell — it
  prints `inserted N (total …)` every 2 minutes. Detach the notebook to stop.
- **Hands-off:** leave `mode = once`; notebook 10 schedules it every 2 minutes.

**Verify:** in a SQL cell, `SELECT count(*), max(event_ts) FROM lakebase_allianz.claims.claim_transactions;`
climbs each cycle.
💡 Seed history fast: run `05` a few times with `mode=once`.

## Phase 6 — Medallion · `06_bronze_ingest` then `07_medallion_no_dlt`
- Run **06** once now → appends new rows to `bronze.claims_raw`.
  **Verify:** `SELECT count(*) FROM allianz_hackathon.bronze.claims_raw;` > 0.
- Run **07** — plain PySpark that rebuilds `silver.claims` + `gold.*` from bronze (idempotent;
  no `dlt` module needed). Schedule it via **notebook 10** (below), or run it by hand.
  **Verify:** it prints the drop/warn DQ counts as it rebuilds silver; then
  `SELECT * FROM allianz_hackathon.gold.gold_loss_ratio ORDER BY observed_loss_ratio DESC;`

## One-shot automation · `10_deploy_pipeline_and_jobs`
Run all cells to schedule the **generator**, **bronze ingest**, and **medallion**
(`07_medallion_no_dlt`) as three 2-minute jobs. One widget:
- **`source`** — `lakebase` (default) or `azuresql` (the optional path below).

**Verify:** it prints three `job_id`s incl. `allianz_hackathon_medallion_batch_2min`, and
silver/gold populate within ~2 min.

---

## Optional Phase — land in Azure SQL Server instead of Lakebase
A bring-your-own-Azure-SQL alternative to Lakebase. The synthetic generator writes the same
4–5 claims every 2 minutes into an Azure SQL Database; ingest lands them in the same
`bronze.claims_raw`, so **silver/gold/dashboard/Genie are identical** to the default path.

**Prereqs**
- An Azure SQL Server + Database reachable from the workspace (SQL firewall: *Allow Azure
  services*, or add the workspace egress IPs). Zero-copy federation is Lakebase-only, so this
  path reads over JDBC (the SQL Server driver ships with the runtime — no `%pip`).
- In `notebooks/00_config`, replace the `TBD`s: `AZ_SQL_SERVER`, `AZ_SQL_DATABASE`,
  `AZ_SQL_SECRET_SCOPE`. Store the login in that secret scope:
  ```
  databricks secrets create-scope <your-scope>
  databricks secrets put-secret  <your-scope> azuresql_user
  databricks secrets put-secret  <your-scope> azuresql_password
  ```

**Run**
1. `02a_create_schema_azuresql` — creates `claims.claim_transactions` in Azure SQL.
   **Verify:** prints `claims.claim_transactions ready` and `columns: 14`.
2. `03_register_zero_copy` — still run this for the `allianz_hackathon` catalog + medallion
   schemas (the Lakebase zero-copy catalog it also registers is simply unused here).
3. `04_firmwide_reference` — same as the default path.
4. Stream: **live demo** → `05a_claims_generator_azuresql` with `mode = loop`, then run
   `06a_bronze_ingest_azuresql`. **Hands-off** → run `10_deploy_pipeline_and_jobs` with the
   `source` widget = **`azuresql`** (schedules 05a + 06a every 2 min; jobs are suffixed
   `_azuresql`).
   **Verify:** `SELECT count(*) FROM allianz_hackathon.bronze.claims_raw;` climbs each cycle.

⚠️ Pick **one** source per `bronze.claims_raw`. Don't run both `06` and `06a` against the same
bronze table — the `claim_txn_id` watermarks come from two different databases and will collide.

**Teardown adds:** Workflows → delete `..._generator_2min_azuresql` and
`..._bronze_ingest_2min_azuresql`; drop `claims.claim_transactions` on the Azure SQL server.

---

## Phase 7 — AI/BI dashboard · `08_dashboard`
Run the notebook to validate each query, then **Dashboards → Create**, add a dataset per query
block (KPIs, incurred by LOB, **loss ratio vs target**, region heatmap, daily trend, freshness),
add widgets, **publish**, set auto-refresh to ~2 min.
**Verify:** the freshness counter (`seconds_since_latest`) stays under ~150s while data flows.

## Phase 8 — Genie space · `09_genie_setup`
Run the notebook (it checks the tables have data), then **Genie → New space**, add the five
tables, paste the instructions, add the sample questions. Ask:
*"Which lines of business are running over their target loss ratio right now?"*

---

## Phase 9 — Demo / talk track (2 min)
1. **Source** — `05` inserting rows into Lakebase (OLTP).
2. **Zero-copy** — `SELECT count(*) FROM lakebase_allianz.claims.claim_transactions` — live, no copy.
3. **Quality** — `07_medallion_no_dlt` output → silver DQ counts (dropped bad rows).
4. **Business value** — dashboard `gold_loss_ratio`: streaming claims vs firmwide premium, every 2 min.
5. **Self-service** — ask Genie in plain English.

## Phase 10 — Teardown
- Workflows → delete `allianz_hackathon_generator_2min`, `allianz_hackathon_bronze_ingest_2min`,
  and `allianz_hackathon_medallion_batch_2min`.
- Compute → Database instances → delete `allianz-hackathon-db` (removes all OLTP data).
- Catalog Explorer → delete `allianz_hackathon` and `lakebase_allianz`.

---

## Quick failure map
| Symptom | Fix |
|---|---|
| Lakebase SDK call fails | create the instance/catalog in the UI (Phase 1 / 3 notes) |
| instance stuck creating | wait 1–3 min, re-run notebook 01 |
| `permission denied for schema public` | expected — we create and use `claims_db` |
| zero-copy SELECT errors | finish notebook 03 (or register the catalog via Catalog Explorer) |
| bronze count stuck at 0 | is the generator running? does the zero-copy SELECT return rows? |
| silver empty | run `06_bronze_ingest` first; `07_medallion_no_dlt` reads *from* bronze |
| `%pip`/import errors | re-run the `%pip` + `dbutils.library.restartPython()` cells at the top |
