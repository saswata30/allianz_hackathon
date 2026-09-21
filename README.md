# Allianz Hackathon — Real-time Claims Streaming on Databricks

A hands-on workshop that streams synthetic P&C **claims into Lakebase** (managed
Postgres), ingests them into the lakehouse **zero-copy via Unity Catalog**, refines
them through a **medallion architecture with data-quality checks** (Lakeflow
Declarative Pipelines), **correlates** them against a firmwide book of business, and
serves the result through an **AI/BI dashboard** and a **Genie** natural-language space.

New claims land every **2 minutes** (4–5 rows) and flow end-to-end automatically.

---

## Architecture

```mermaid
flowchart LR
  subgraph SRC[Synthetic Source]
    GEN["claims_generator.py<br/>4-5 rows / 2 min"]
  end
  subgraph LB["Lakebase — Managed Postgres (OLTP)"]
    PG[("claims_db<br/>claims.claim_transactions")]
  end
  subgraph UCat["Unity Catalog — Governance"]
    ZC[["lakebase_allianz<br/>ZERO-COPY federation"]]
    subgraph MED["Medallion (Delta Lake)"]
      BR["bronze.claims_raw<br/>incremental append"]
      SI["silver.claims<br/>DQ expectations"]
      GO["gold.*<br/>aggregates + correlation"]
    end
    FW[("reference.firmwide_exposure<br/>firmwide Delta")]
  end
  subgraph CONS["Consumption"]
    DASH["AI/BI Dashboard"]
    GENIE["Genie Space (NL Q&A)"]
  end
  GEN -->|INSERT| PG
  PG -.->|zero copy| ZC
  ZC -->|watermark read| BR
  BR -->|stream| SI
  SI -->|aggregate| GO
  FW -->|join / loss ratio| GO
  GO --> DASH
  GO --> GENIE
  SI --> GENIE
```

Full write-up: [`docs/architecture.md`](docs/architecture.md).

---

## Repository layout

```
allianz_hackathon/
├── config/settings.env            # ONE place for all names (catalog, lakebase, etc.)
├── lib/lakebase.py                # shared Lakebase connection helper (OAuth auto-refresh)
├── setup/
│   ├── 00_provision_lakebase.sh   # create the Lakebase project/branch/endpoint
│   ├── 01_create_postgres_schema.py  # create claims_db + claims.claim_transactions
│   └── 02_register_uc_catalog.sh  # ZERO-COPY: register Lakebase into UC + medallion schemas
├── reference/firmwide_reference.sql  # firmwide book-of-business Delta table (correlation)
├── streaming/claims_generator.py  # synthetic claims -> Lakebase, 4-5 rows / 2 min
├── pipelines/
│   ├── 01_bronze_ingest.py        # zero-copy incremental read -> bronze Delta (2-min job)
│   ├── medallion_dlt.py           # DLT: silver (DQ) + gold (aggregates + correlation)
│   ├── pipeline.json              # DLT pipeline spec
│   └── bronze_job.json            # bronze ingest job (cron every 2 min)
├── dashboard/queries.sql          # AI/BI dashboard queries (gold layer)
├── genie/genie_space_setup.md     # Genie space tables, instructions, sample questions
├── scripts/run_end_to_end.sh      # one-shot orchestrator
└── docs/architecture.(md|png|svg)
```

---

## Prerequisites

- **Databricks CLI ≥ 0.285.0** (`databricks --version`) authenticated to a **serverless**
  FE-VM workspace (Lakebase requires serverless).
  ```bash
  databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile>
  ```
- **Python 3.10+** with the local deps: `pip install -r requirements.txt`
- **jq** (`brew install jq`)
- Permissions to create catalogs, Lakebase projects, pipelines, and jobs.

> Edit **`config/settings.env`** once — every script reads it. Set `DATABRICKS_PROFILE`,
> `DATABRICKS_HOST`, `UC_CATALOG`, and (after step 4) `SQL_WAREHOUSE_ID`.

---

## Quick start (automated)

```bash
git clone https://github.com/saswata30/allianz_hackathon.git
cd allianz_hackathon
pip install -r requirements.txt
# edit config/settings.env for your workspace, then:
source config/settings.env
bash scripts/run_end_to_end.sh          # steps 1-7
python3 streaming/claims_generator.py   # start the 2-min stream (leave running)
```

Prefer to understand each step? Follow the manual walk-through below.

---

## Step-by-step

### 1 · Provision Lakebase (OLTP Postgres)
```bash
source config/settings.env
bash setup/00_provision_lakebase.sh
```
Creates the `allianz-hackathon` project (auto-creates a `production` branch + `primary`
read-write endpoint), waits until it is ACTIVE, and scales it to 0.5–2 CU.

### 2 · Create the Postgres schema
```bash
python3 setup/01_create_postgres_schema.py
```
Creates `claims_db`, schema `claims`, and table `claim_transactions` (with a monotonic
`claim_txn_id` used as the ingest watermark) plus helpful indexes.

### 3 · Register the zero-copy Unity Catalog catalog
```bash
bash setup/02_register_uc_catalog.sh
```
Registers Lakebase into UC as **`lakebase_allianz`** — you can now query the live OLTP
table from Databricks with **no copy**:
```sql
SELECT count(*) FROM lakebase_allianz.claims.claim_transactions;
```
It also creates the medallion catalog `allianz_hackathon` with `bronze` / `silver` /
`gold` / `reference` schemas.
> If the CLI registration isn't available on your workspace, create the catalog from
> **Catalog Explorer → Create catalog → Database (Lakebase)** and pick the project/database.

### 4 · Load the firmwide reference (correlation denominator)
Run [`reference/firmwide_reference.sql`](reference/firmwide_reference.sql) in a SQL editor
(or set `SQL_WAREHOUSE_ID` and let `run_end_to_end.sh` execute it). Builds
`reference.firmwide_exposure` — premium & exposure by LOB × region.

### 5 · Start the claims stream
```bash
python3 streaming/claims_generator.py          # loops: 4-5 rows every 2 min
# one batch only (for a scheduled job):  python3 streaming/claims_generator.py --once
```
Leave this running. Confirm rows are landing:
```sql
SELECT count(*), max(event_ts) FROM lakebase_allianz.claims.claim_transactions;
```

### 6 · Run the medallion (bronze job + DLT pipeline)
- **Bronze** — import `pipelines/01_bronze_ingest.py` to the workspace and create the job
  from `pipelines/bronze_job.json` (runs every 2 min, appends new rows zero-copy).
- **Silver + Gold** — create the Lakeflow Declarative Pipeline from `pipelines/pipeline.json`
  (attaches `pipelines/medallion_dlt.py`) and start it. `run_end_to_end.sh` does both; or:
  ```bash
  databricks pipelines start-update <pipeline_id> -p "$DATABRICKS_PROFILE"
  ```
  Watch the **data-quality metrics** on the pipeline graph — dropped rows appear on the
  `silver.claims` node.

### 7 · Build the AI/BI dashboard
Open **Dashboards → Create**, add datasets from [`dashboard/queries.sql`](dashboard/queries.sql)
(KPIs, incurred by LOB, **loss ratio vs target**, region heatmap, daily trend, freshness),
and publish. Set it to auto-refresh (e.g. every 2 min) to watch the stream move.

### 8 · Create the Genie space
Follow [`genie/genie_space_setup.md`](genie/genie_space_setup.md): add the five tables,
paste the instructions, add the sample questions. Then ask
*"Which lines of business are running over their target loss ratio right now?"*

---

## Data quality (medallion expectations)

Defined in `pipelines/medallion_dlt.py` on `silver.claims`:

| Rule | Type | Expectation |
|---|---|---|
| `valid_claim_id` / `valid_policy_id` | drop | keys not null |
| `positive_amount` | drop | `claim_amount > 0` |
| `valid_lob` | drop | LOB in the allowed set |
| `valid_currency` | drop | currency ∈ {EUR, GBP, CHF, USD} |
| `dates_consistent` | warn | `reported_date >= incident_date` |
| `amount_not_extreme` | warn | `claim_amount < 5,000,000` |

Dropped/flagged counts are tracked automatically in the pipeline's DQ dashboard.

---

## Teardown
```bash
source config/settings.env
# stop the generator (Ctrl-C), then:
databricks pipelines delete <pipeline_id> -p "$DATABRICKS_PROFILE"
databricks jobs delete <job_id> -p "$DATABRICKS_PROFILE"
databricks postgres delete-project projects/allianz-hackathon -p "$DATABRICKS_PROFILE"  # deletes ALL data
databricks catalogs delete allianz_hackathon --force -p "$DATABRICKS_PROFILE"
databricks catalogs delete lakebase_allianz --force -p "$DATABRICKS_PROFILE"
```

## Troubleshooting
- **`unknown command postgres`** — CLI < 0.285.0; upgrade (`brew upgrade databricks`).
- **`refresh token is invalid`** — re-run `databricks auth login`.
- **Endpoint not ACTIVE** — Lakebase takes 1–2 min to spin up; re-run step 1.
- **`permission denied for schema public`** — expected; we create and use `claims_db`, not `postgres`.
- **Bronze sees no rows** — confirm the generator is running and the zero-copy `SELECT` in step 3 returns rows.
- **psql missing** — not required; the scripts use Python `psycopg2`.
