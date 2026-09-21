#!/usr/bin/env python3
"""Create the claims database, schema, and table in Lakebase (Postgres).

Idempotent: uses CREATE DATABASE / SCHEMA / TABLE IF NOT EXISTS semantics.
    source config/settings.env && python setup/01_create_postgres_schema.py
"""
from __future__ import annotations

import os
import pathlib
import sys

import psycopg2

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from lib.lakebase import LakebaseClient  # noqa: E402

DATABASE = os.environ.get("LB_DATABASE", "claims_db")
SCHEMA = os.environ.get("LB_SCHEMA", "claims")
TABLE = os.environ.get("LB_TABLE", "claim_transactions")

TABLE_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA};

CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
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

CREATE INDEX IF NOT EXISTS idx_{TABLE}_event_ts ON {SCHEMA}.{TABLE} (event_ts);
CREATE INDEX IF NOT EXISTS idx_{TABLE}_lob_region ON {SCHEMA}.{TABLE} (lob, region);
"""


def ensure_database(client: LakebaseClient) -> None:
    """CREATE DATABASE cannot run inside a transaction, so use the default db + autocommit."""
    conn = client.connect("postgres", autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE,))
            if cur.fetchone():
                print(f">> database '{DATABASE}' already exists")
            else:
                cur.execute(f'CREATE DATABASE "{DATABASE}"')
                print(f">> created database '{DATABASE}'")
    finally:
        conn.close()


def create_objects(client: LakebaseClient) -> None:
    conn = client.connect(DATABASE, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(TABLE_DDL)
            cur.execute(
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_schema=%s AND table_name=%s",
                (SCHEMA, TABLE),
            )
            ncols = cur.fetchone()[0]
        print(f">> {SCHEMA}.{TABLE} ready ({ncols} columns)")
    finally:
        conn.close()


if __name__ == "__main__":
    client = LakebaseClient()
    print(f">> connecting as {client.email()} @ {client.host()}")
    ensure_database(client)
    create_objects(client)
    print("\n>> Postgres schema ready.")
    print("   Next: bash setup/02_register_uc_catalog.sh   (zero-copy into Unity Catalog)")
