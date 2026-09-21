# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Bronze ingest — zero-copy Lakebase ➜ Delta (incremental)
# MAGIC Reads only the **new** rows from the zero-copy UC catalog (`lakebase_allianz`) using a
# MAGIC high-watermark on the monotonic `claim_txn_id`, and appends to `bronze.claims_raw`.
# MAGIC Idempotent. Schedule as a **job every 2 minutes** (notebook 10 does this).

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

if spark.catalog.tableExists(BRONZE_TABLE):
    watermark = spark.table(BRONZE_TABLE).agg(F.max("claim_txn_id")).first()[0] or 0
else:
    watermark = 0
print(f"watermark (max claim_txn_id already in bronze) = {watermark}")

# COMMAND ----------

# Zero-copy read: executes live against Lakebase Postgres — no data copy.
new_rows = (
    spark.read.table(ZERO_COPY_TABLE)
    .where(F.col("claim_txn_id") > F.lit(watermark))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit(ZERO_COPY_TABLE))
)

n = new_rows.count()
print(f"new rows to ingest: {n}")

if n > 0:
    (new_rows.write.format("delta").mode("append")
        .option("mergeSchema", "true").saveAsTable(BRONZE_TABLE))
    print(f"appended {n} rows -> {BRONZE_TABLE}")
else:
    print("nothing new — bronze is up to date")

# COMMAND ----------

total = spark.table(BRONZE_TABLE).count() if spark.catalog.tableExists(BRONZE_TABLE) else 0
print(f"bronze total rows: {total}")
if total:
    display(spark.table(BRONZE_TABLE).orderBy(F.col("claim_txn_id").desc()).limit(10))
