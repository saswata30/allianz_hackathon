# Allianz Hackathon — Real-time Claims Streaming on Databricks

A hands-on, **notebook-driven** workshop that streams synthetic P&C **claims into Lakebase**
(managed Postgres), ingests them into the lakehouse **zero-copy via Unity Catalog**, refines
them through a **medallion architecture with data-quality checks** (Lakeflow Declarative
Pipelines), **correlates** them against a firmwide book of business, and serves the result
through an **AI/BI dashboard** and a **Genie** natural-language space.

Everything runs **inside Databricks** — the notebooks authenticate automatically as the
running user, so there is no CLI, no profile, and no workspace host to configure.
New claims land every **2 minutes** (4–5 rows) and flow end-to-end automatically.

---

## Architecture

```mermaid
flowchart LR
  subgraph SRC[Synthetic Source]
    GEN["05_claims_generator<br/>4-5 rows / 2 min"]
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

Full write-up: [`docs/architecture.md`](docs/architecture.md). Detailed run book:
[`EXECUTION_GUIDE.md`](EXECUTION_GUIDE.md).

---

## Notebooks (run in order)

| # | Notebook | What it does |
|---|---|---|
| 00 | `notebooks/00_config` | Shared names (catalog, Lakebase, schemas). `%run` by the others. |
| 01 | `notebooks/01_provision_lakebase` | Create the Lakebase (Postgres) instance via the SDK. |
| 02 | `notebooks/02_create_schema` | Create `claims_db.claims.claim_transactions` (OLTP source). |
| 03 | `notebooks/03_register_zero_copy` | **Zero-copy**: register Lakebase into UC + medallion schemas. |
| 04 | `notebooks/04_firmwide_reference` | Firmwide book-of-business Delta table (correlation). |
| 05 | `notebooks/05_claims_generator` | Synthetic claims → Lakebase (4–5 rows/batch; `once` or `loop`). |
| 06 | `notebooks/06_bronze_ingest` | Zero-copy incremental read → `bronze.claims_raw`. |
| 07 | `notebooks/07_medallion_dlt` | DLT: `silver.claims` (DQ expectations) + `gold.*` (aggregates + correlation). |
| 08 | `notebooks/08_dashboard` | Gold-layer queries for the AI/BI dashboard. |
| 09 | `notebooks/09_genie_setup` | Genie space tables, instructions, sample questions. |
| 10 | `notebooks/10_deploy_pipeline_and_jobs` | Create the DLT pipeline + the two 2-minute jobs (SDK). |

---

## Quick start

1. **Add this repo to your workspace**: *Workspace → Repos → Add repo →*
   `https://github.com/saswata30/allianz_hackathon.git` (or *Git folder*).
2. Open **`notebooks/01_provision_lakebase`** and run it; wait for `AVAILABLE`.
3. Run **02 → 03 → 04** in order (each prints the next step).
4. Run **`notebooks/10_deploy_pipeline_and_jobs`** — this schedules the generator and bronze
   ingest every 2 minutes and starts the DLT pipeline. *(For a manual/live demo instead, run
   05 with `mode=loop`, then 06, then start the pipeline from 07.)*
5. Build the **dashboard** from **08** and the **Genie space** from **09**.

> Prerequisites: a **serverless** Databricks workspace with **Lakebase** enabled, and
> permission to create catalogs, Lakebase instances, pipelines, and jobs. The workshop uses
> only the built-in runtime + SDK; `%pip` installs `psycopg2-binary`/`Faker` where needed.

---

## Data quality (medallion expectations)

Defined on `silver.claims` in `notebooks/07_medallion_dlt`:

| Rule | Type | Expectation |
|---|---|---|
| `valid_claim_id` / `valid_policy_id` | drop | keys not null |
| `positive_amount` | drop | `claim_amount > 0` |
| `valid_lob` | drop | LOB in the allowed set |
| `valid_currency` | drop | currency ∈ {EUR, GBP, CHF, USD} |
| `dates_consistent` | warn | `reported_date >= incident_date` |
| `amount_not_extreme` | warn | `claim_amount < 5,000,000` |

Dropped/flagged counts show automatically on the pipeline's `silver.claims` node.

---

## Zero-copy, in one query

After notebook 03, the live Postgres rows are queryable from the lakehouse with **no copy**:
```sql
SELECT count(*) FROM lakebase_allianz.claims.claim_transactions;
```

## Correlation, in one table
`gold.gold_loss_ratio` joins streaming incurred claims to firmwide premium →
`observed_loss_ratio` vs `target_loss_ratio` (`OVER`/`UNDER`), updated every 2 minutes.

---

## Teardown
Stop the two jobs and the pipeline, then delete the Lakebase instance and catalogs
(Compute → Database instances → delete; Catalog Explorer → delete `allianz_hackathon` and
`lakebase_allianz`). Deleting the Lakebase instance removes all OLTP data.
