# Execution Guide — Allianz Claims Streaming Workshop

A step-by-step run book. Everything runs **inside Databricks** as notebooks — no CLI, no
profile, no workspace host. Each step lists **what to run**, **what you should see**, and a
**verify** check. Estimated total: **~35–45 min** (most of it watching the 2-min stream).

| Phase | Notebook | ~Time |
|---|---|---|
| 0 | Add repo to workspace | 3 min |
| 1 | `01_provision_lakebase` | 3 min |
| 2 | `02_create_schema` | 1 min |
| 3 | `03_register_zero_copy` | 3 min |
| 4 | `04_firmwide_reference` | 2 min |
| 5 | `05_claims_generator` | 2 min |
| 6 | `06_bronze_ingest` + `07_medallion_dlt` | 10 min |
| 7 | `08_dashboard` | 5 min |
| 8 | `09_genie_setup` | 5 min |
| — | `10_deploy_pipeline_and_jobs` (automates 5–7) | 3 min |
| 9 | Demo / talk track | — |
| 10 | Teardown | 3 min |

---

## Phase 0 — Add the repo to your workspace
- **Workspace → Repos → Add repo** → `https://github.com/saswata30/allianz_hackathon.git`
  (or **Git folder**). Open the `notebooks/` folder.
- Requirements: a **serverless** workspace with **Lakebase** enabled and rights to create
  catalogs, Lakebase instances, pipelines, and jobs.
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

## Phase 6 — Medallion · `06_bronze_ingest` then `07_medallion_dlt`
- Run **06** once now → appends new rows to `bronze.claims_raw`.
  **Verify:** `SELECT count(*) FROM allianz_hackathon.bronze.claims_raw;` > 0.
- Create/start the DLT pipeline for **07** — easiest via **notebook 10** (below), or manually:
  **Pipelines → Create**, attach `notebooks/07_medallion_dlt`, target catalog
  `allianz_hackathon` / schema `silver`, serverless, then **Start**.
  **Verify:** watch DQ metrics on the `claims` node; then
  `SELECT * FROM allianz_hackathon.gold.gold_loss_ratio ORDER BY observed_loss_ratio DESC;`

## One-shot automation · `10_deploy_pipeline_and_jobs`
Run all cells to create the DLT pipeline, schedule the **generator** and **bronze** jobs every
2 minutes, and start the pipeline. **Verify:** it prints a `pipeline_id` and two `job_id`s and
`started pipeline update`.

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
3. **Quality** — DLT graph → silver DQ metrics (dropped bad rows).
4. **Business value** — dashboard `gold_loss_ratio`: streaming claims vs firmwide premium, every 2 min.
5. **Self-service** — ask Genie in plain English.

## Phase 10 — Teardown
- Pipelines → delete `allianz_hackathon_medallion`.
- Workflows → delete `allianz_hackathon_generator_2min` and `allianz_hackathon_bronze_ingest_2min`.
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
| DLT silver empty | run `06_bronze_ingest` first; DLT streams *from* bronze |
| `%pip`/import errors | re-run the `%pip` + `dbutils.library.restartPython()` cells at the top |
