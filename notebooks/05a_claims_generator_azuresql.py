# Databricks notebook source
# MAGIC %md
# MAGIC # 05a · (OPTIONAL) Synthetic claims generator ➜ Azure SQL Server
# MAGIC The **Azure SQL** twin of `05_claims_generator`. Inserts **4–5 realistic claim
# MAGIC transactions** into your Azure SQL Database per cycle instead of Lakebase.
# MAGIC
# MAGIC * **`mode = once`** — insert a single batch and stop. Schedule this notebook as a
# MAGIC   **job every 2 minutes** (notebook 10 with `source=azuresql`) for hands-off streaming.
# MAGIC * **`mode = loop`** — insert a batch every `interval_seconds` right here in the
# MAGIC   notebook (handy for a live demo). Detach the notebook to stop.
# MAGIC
# MAGIC Prereqs: run `02a_create_schema_azuresql` first, and fill in the Azure SQL config
# MAGIC (server/database/secret scope) in `00_config`.

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
from datetime import date, timedelta

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

# claim_txn_id / event_ts / created_at are set by the Azure SQL IDENTITY + DEFAULTs,
# so we only insert the business columns.
INSERT_SQL = f"""
INSERT INTO {AZ_SQL_FQ_TABLE}
  (claim_id, policy_id, lob, region, peril, claim_status, transaction_type,
   claim_amount, currency, incident_date, reported_date)
VALUES (?,?,?,?,?,?,?,?,?,?,?)
"""


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


# COMMAND ----------

# Insert row-by-row through the driver-JVM JDBC connection (parameterised PreparedStatement).
# Small batches (4-5 rows / 2 min) — no Spark job needed and IDENTITY stays server-side.
jvm = spark._sc._jvm
driver_mgr = jvm.java.sql.DriverManager


def insert_batch():
    props = azure_sql_conn_properties()
    n = random.randint(GEN_MIN_ROWS, GEN_MAX_ROWS)
    conn = driver_mgr.getConnection(azure_sql_jdbc_url(), props["user"], props["password"])
    try:
        conn.setAutoCommit(False)
        ps = conn.prepareStatement(INSERT_SQL)
        for r in make_rows(n):
            claim_id, policy_id, lob, region, peril, status, txn, amount, ccy, inc, rep = r
            ps.setString(1, claim_id)
            ps.setString(2, policy_id)
            ps.setString(3, lob)
            ps.setString(4, region)
            ps.setString(5, peril)
            ps.setString(6, status)
            ps.setString(7, txn)
            ps.setBigDecimal(8, jvm.java.math.BigDecimal(str(amount)))
            ps.setString(9, ccy)
            ps.setDate(10, jvm.java.sql.Date.valueOf(inc.isoformat()))
            ps.setDate(11, jvm.java.sql.Date.valueOf(rep.isoformat()))
            ps.addBatch()
        ps.executeBatch()
        conn.commit()
        ps.close()
    finally:
        conn.close()
    return n

# COMMAND ----------

if MODE == "once":
    n = insert_batch()
    print(f"inserted {n} rows into {AZ_SQL_DATABASE}.{AZ_SQL_FQ_TABLE}")
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
