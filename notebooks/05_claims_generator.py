# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Synthetic claims generator ➜ Lakebase
# MAGIC Inserts **4–5 realistic claim transactions** into Lakebase per cycle.
# MAGIC
# MAGIC * **`mode = once`** — insert a single batch and stop. Schedule this notebook as a
# MAGIC   **job every 2 minutes** (notebook 10 does this) for hands-off streaming.
# MAGIC * **`mode = loop`** — insert a batch every `interval_seconds` right here in the
# MAGIC   notebook (handy for a live demo). Detach the notebook to stop.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary Faker -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("mode", "once", ["once", "loop"], "Mode")
dbutils.widgets.text("interval_seconds", "120", "Loop interval (s)")
dbutils.widgets.text("cycles", "10", "Loop cycles (0 = forever)")
MODE = dbutils.widgets.get("mode")
INTERVAL = int(dbutils.widgets.get("interval_seconds"))
CYCLES = int(dbutils.widgets.get("cycles"))

# COMMAND ----------

import random
import time
import uuid
from datetime import date, timedelta

import psycopg2
from databricks.sdk import WorkspaceClient

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
CCY = {"UK": "GBP", "Switzerland": "CHF"}
SEVERITY = {
    "Property": (2000, 250000), "Motor": (500, 40000), "Liability": (5000, 500000),
    "Marine": (10000, 750000), "Health": (100, 25000), "Life": (25000, 1000000),
}

INSERT_SQL = f"""
INSERT INTO {LB_SCHEMA}.{LB_TABLE}
  (claim_id, policy_id, lob, region, peril, claim_status, transaction_type,
   claim_amount, currency, incident_date, reported_date)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


def connect():
    w = WorkspaceClient()
    inst = w.database.get_database_instance(name=LB_INSTANCE)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()), instance_names=[LB_INSTANCE]
    )
    c = psycopg2.connect(
        host=inst.read_write_dns, port=5432, dbname=LB_DATABASE,
        user=w.current_user.me().user_name, password=cred.token,
        sslmode="require", connect_timeout=30,
    )
    c.autocommit = True
    return c


def make_rows(n):
    today = date.today()
    rows = []
    for _ in range(n):
        lob = random.choice(LOBS)
        region = random.choice(REGIONS)
        lo, hi = SEVERITY[lob]
        amount = round(random.uniform(lo, lo + (hi - lo) * random.random() ** 2), 2)
        inc = today - timedelta(days=random.randint(0, 45))
        rows.append((
            f"CLM-{random.randint(100000,999999)}", f"POL-{random.randint(1000000,9999999)}",
            lob, region, random.choice(PERILS[lob]), random.choice(STATUSES),
            random.choice(TXN_TYPES), amount, CCY.get(region, "EUR"),
            inc, inc + timedelta(days=random.randint(0, 10)),
        ))
    return rows


def insert_batch():
    n = random.randint(GEN_MIN_ROWS, GEN_MAX_ROWS)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, make_rows(n))
    finally:
        conn.close()
    return n

# COMMAND ----------

if MODE == "once":
    n = insert_batch()
    print(f"inserted {n} rows into {LB_DATABASE}.{LB_SCHEMA}.{LB_TABLE}")
else:
    total, i = 0, 0
    while CYCLES == 0 or i < CYCLES:
        try:
            n = insert_batch()
            total += n
            print(f"{time.strftime('%H:%M:%S')} inserted {n} (total {total})")
        except Exception as e:
            print(f"ERROR: {e} — retrying in 15s")
            time.sleep(15)
            continue
        i += 1
        if CYCLES == 0 or i < CYCLES:
            time.sleep(INTERVAL)
    print(f"done — {total} rows over {i} cycles")
