#!/usr/bin/env python3
"""Synthetic Allianz claims generator -> Lakebase (Postgres OLTP).

Inserts a small batch of realistic P&C claim transactions on an interval, to
simulate a live claims system. Runs two ways:

    # Continuous loop (workshop demo): 4-5 rows every 2 minutes
    python streaming/claims_generator.py

    # Single batch (e.g. a Databricks/cron job that fires every 2 min)
    python streaming/claims_generator.py --once

Robustness:
  * refreshes the Lakebase OAuth token automatically (tokens expire ~1h)
  * reconnects on transient connection/query errors with backoff
  * clean shutdown on Ctrl-C
  * deterministic-ish but varied data; amounts correlate with line of business
"""
from __future__ import annotations

import argparse
import os
import pathlib
import random
import signal
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from lib.lakebase import LakebaseClient  # noqa: E402

# --- domain values (kept in sync with firmwide reference for correlation) -----
LOBS = ["Property", "Motor", "Liability", "Marine", "Health", "Life"]
REGIONS = ["Germany", "France", "Italy", "UK", "Spain", "Switzerland", "Austria", "Benelux"]
PERILS = {
    "Property": ["Fire", "Flood", "Storm", "Theft", "Water Damage", "Subsidence"],
    "Motor": ["Collision", "Theft", "Third-Party", "Windscreen", "Fire"],
    "Liability": ["Bodily Injury", "Property Damage", "Professional Indemnity"],
    "Marine": ["Cargo Loss", "Hull Damage", "General Average"],
    "Health": ["Inpatient", "Outpatient", "Dental", "Optical"],
    "Life": ["Death Benefit", "Critical Illness", "Disability"],
}
STATUSES = ["Open", "In Review", "Approved", "Paid", "Denied", "Reopened"]
TXN_TYPES = ["Reserve", "Payment", "Recovery", "Adjustment"]
CURRENCY_BY_REGION = {
    "UK": "GBP", "Switzerland": "CHF", "Germany": "EUR", "France": "EUR",
    "Italy": "EUR", "Spain": "EUR", "Austria": "EUR", "Benelux": "EUR",
}
# rough severity band per LOB so gold-layer loss ratios look realistic
SEVERITY = {
    "Property": (2_000, 250_000), "Motor": (500, 40_000), "Liability": (5_000, 500_000),
    "Marine": (10_000, 750_000), "Health": (100, 25_000), "Life": (25_000, 1_000_000),
}

_RUNNING = True


def _stop(*_):
    global _RUNNING
    _RUNNING = False
    print("\n[generator] shutdown requested — finishing current batch...")


def make_rows(n: int) -> list[tuple]:
    rows = []
    today = date.today()
    for _ in range(n):
        lob = random.choice(LOBS)
        region = random.choice(REGIONS)
        lo, hi = SEVERITY[lob]
        # log-ish skew: most claims small, a few large
        amount = round(random.uniform(lo, lo + (hi - lo) * random.random() ** 2), 2)
        incident = today - timedelta(days=random.randint(0, 45))
        reported = incident + timedelta(days=random.randint(0, 10))
        rows.append(
            (
                f"CLM-{random.randint(100000, 999999)}",           # claim_id
                f"POL-{random.randint(1000000, 9999999)}",         # policy_id
                lob,
                region,
                random.choice(PERILS[lob]),                        # peril
                random.choice(STATUSES),                           # claim_status
                random.choice(TXN_TYPES),                          # transaction_type
                amount,                                            # claim_amount
                CURRENCY_BY_REGION.get(region, "EUR"),             # currency
                incident,                                          # incident_date
                reported,                                          # reported_date
            )
        )
    return rows


INSERT_SQL = """
INSERT INTO {schema}.{table}
    (claim_id, policy_id, lob, region, peril, claim_status, transaction_type,
     claim_amount, currency, incident_date, reported_date)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def insert_batch(client: LakebaseClient, database: str, schema: str, table: str,
                 min_rows: int, max_rows: int) -> int:
    n = random.randint(min_rows, max_rows)
    rows = make_rows(n)
    sql = INSERT_SQL.format(schema=schema, table=table)
    conn = client.connect(database)
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        return n
    finally:
        conn.close()


def run():
    parser = argparse.ArgumentParser(description="Synthetic claims -> Lakebase")
    parser.add_argument("--once", action="store_true", help="insert a single batch and exit")
    parser.add_argument("--interval", type=int,
                        default=int(os.environ.get("GEN_INTERVAL_SECONDS", "120")))
    parser.add_argument("--min-rows", type=int, default=int(os.environ.get("GEN_MIN_ROWS", "4")))
    parser.add_argument("--max-rows", type=int, default=int(os.environ.get("GEN_MAX_ROWS", "5")))
    args = parser.parse_args()

    database = os.environ.get("LB_DATABASE", "claims_db")
    schema = os.environ.get("LB_SCHEMA", "claims")
    table = os.environ.get("LB_TABLE", "claim_transactions")

    client = LakebaseClient()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"[generator] target={database}.{schema}.{table} "
          f"interval={args.interval}s rows={args.min_rows}-{args.max_rows}")

    total = 0
    while _RUNNING:
        try:
            n = insert_batch(client, database, schema, table, args.min_rows, args.max_rows)
            total += n
            print(f"[generator] {time.strftime('%H:%M:%S')} inserted {n} rows (total {total})")
        except Exception as e:  # noqa: BLE001 — keep the demo alive through transient errors
            print(f"[generator] ERROR: {e}\n[generator] retrying in 15s...")
            time.sleep(15)
            continue
        if args.once:
            break
        # sleep in 1s ticks so Ctrl-C is responsive
        for _ in range(args.interval):
            if not _RUNNING:
                break
            time.sleep(1)

    print(f"[generator] stopped. total rows inserted: {total}")


if __name__ == "__main__":
    run()
