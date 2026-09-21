# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingest — zero-copy Lakebase ➜ Delta (incremental)
# MAGIC
# MAGIC Reads only the **new** claim rows from the zero-copy Unity Catalog federation of
# MAGIC Lakebase (`lakebase_allianz.claims.claim_transactions`) using a high-watermark on the
# MAGIC monotonic `claim_txn_id`, and appends them to `allianz_hackathon.bronze.claims_raw`.
# MAGIC
# MAGIC Schedule this notebook as a **Databricks Job every 2 minutes** (matching the generator).
# MAGIC It is idempotent — the watermark guarantees each source row is appended exactly once.

# COMMAND ----------

dbutils.widgets.text("catalog", "allianz_hackathon", "UC catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("lakebase_catalog", "lakebase_allianz", "Lakebase UC catalog (zero-copy)")
dbutils.widgets.text("lb_schema", "claims", "Lakebase schema")
dbutils.widgets.text("lb_table", "claim_transactions", "Lakebase table")

CATALOG = dbutils.widgets.get("catalog")
BRONZE = dbutils.widgets.get("bronze_schema")
LB_CAT = dbutils.widgets.get("lakebase_catalog")
LB_SCHEMA = dbutils.widgets.get("lb_schema")
LB_TABLE = dbutils.widgets.get("lb_table")

SOURCE = f"{LB_CAT}.{LB_SCHEMA}.{LB_TABLE}"          # zero-copy federated view of Lakebase
TARGET = f"{CATALOG}.{BRONZE}.claims_raw"

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE}")

# current watermark (0 on first run)
if spark.catalog.tableExists(TARGET):
    watermark = spark.table(TARGET).agg(F.max("claim_txn_id")).first()[0] or 0
else:
    watermark = 0
print(f"watermark (max claim_txn_id already in bronze) = {watermark}")

# COMMAND ----------

# Zero-copy read: this SELECT executes against Lakebase Postgres live — no data copy.
new_rows = (
    spark.read.table(SOURCE)
    .where(F.col("claim_txn_id") > F.lit(watermark))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit(SOURCE))
)

n = new_rows.count()
print(f"new rows to ingest: {n}")

if n > 0:
    (new_rows.write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET))
    print(f"appended {n} rows -> {TARGET}")
else:
    print("nothing new — bronze is up to date")

# COMMAND ----------

total = spark.table(TARGET).count() if spark.catalog.tableExists(TARGET) else 0
print(f"bronze total rows: {total}")
display(spark.table(TARGET).orderBy(F.col("claim_txn_id").desc()).limit(10))
