# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Create the Postgres database, schema & table
# MAGIC Connects to Lakebase with a short-lived OAuth credential from the SDK and creates
# MAGIC `claims_db.claims.claim_transactions`. Idempotent (`IF NOT EXISTS`).

# COMMAND ----------

# MAGIC %pip install psycopg2-binary -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import uuid

import psycopg2
from databricks.sdk import WorkspaceClient


def lakebase_connect(database: str, autocommit: bool = True):
    """Open a psycopg2 connection to the Lakebase instance as the current user."""
    w = WorkspaceClient()
    inst = w.database.get_database_instance(name=LB_INSTANCE)
    host = inst.read_write_dns
    user = w.current_user.me().user_name
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()), instance_names=[LB_INSTANCE]
    )
    conn = psycopg2.connect(
        host=host, port=5432, dbname=database, user=user,
        password=cred.token, sslmode="require", connect_timeout=30,
    )
    conn.autocommit = autocommit
    return conn


# The Lakebase bootstrap database (default logical DB on a new instance)
BOOTSTRAP_DB = "databricks_postgres"

# COMMAND ----------

# Create the claims_db logical database (CREATE DATABASE can't run in a txn)
conn = lakebase_connect(BOOTSTRAP_DB, autocommit=True)
try:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (LB_DATABASE,))
        if cur.fetchone():
            print(f"database '{LB_DATABASE}' already exists")
        else:
            cur.execute(f'CREATE DATABASE "{LB_DATABASE}"')
            print(f"created database '{LB_DATABASE}'")
finally:
    conn.close()

# COMMAND ----------

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {LB_SCHEMA};

CREATE TABLE IF NOT EXISTS {LB_SCHEMA}.{LB_TABLE} (
    claim_txn_id      BIGSERIAL PRIMARY KEY,          -- monotonic watermark for incremental ingest
    claim_id          VARCHAR(20)   NOT NULL,
    policy_id         VARCHAR(20)   NOT NULL,
    lob               VARCHAR(40)   NOT NULL,
    region            VARCHAR(40)   NOT NULL,
    peril             VARCHAR(60),
    claim_status      VARCHAR(30)   NOT NULL,
    transaction_type  VARCHAR(30)   NOT NULL,
    claim_amount      NUMERIC(14,2) NOT NULL,
    currency          VARCHAR(3)    NOT NULL,
    incident_date     DATE,
    reported_date     DATE,
    event_ts          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_{LB_TABLE}_event_ts ON {LB_SCHEMA}.{LB_TABLE} (event_ts);
CREATE INDEX IF NOT EXISTS idx_{LB_TABLE}_lob_region ON {LB_SCHEMA}.{LB_TABLE} (lob, region);
"""

conn = lakebase_connect(LB_DATABASE, autocommit=True)
try:
    with conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
            (LB_SCHEMA, LB_TABLE),
        )
        print(f"{LB_SCHEMA}.{LB_TABLE} ready ({cur.fetchone()[0]} columns)")
finally:
    conn.close()

print("\nNext: run 03_register_zero_copy")
