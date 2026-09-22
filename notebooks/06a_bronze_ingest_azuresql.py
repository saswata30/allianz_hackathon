# Databricks notebook source
# MAGIC %md
# MAGIC # 06a · (OPTIONAL) Bronze ingest — Azure SQL ➜ Delta (incremental)
# MAGIC The **Azure SQL** twin of `06_bronze_ingest`. Reads only the **new** rows from the
# MAGIC Azure SQL `claim_transactions` table over a JDBC connection, using a high-watermark on
# MAGIC the monotonic `claim_txn_id`, and appends to the same `bronze.claims_raw` Delta table.
# MAGIC Everything downstream (silver/gold/DLT/dashboard/Genie) is therefore identical to the
# MAGIC Lakebase path. Idempotent. Schedule as a **job every 2 minutes** (notebook 10 with
# MAGIC `source=azuresql`).
# MAGIC
# MAGIC > Azure SQL has no Databricks zero-copy federation, so this reads over JDBC (a real
# MAGIC > connector) rather than the Lakebase zero-copy catalog. Pick **one** source per bronze
# MAGIC > table — don't run both `06` and `06a` against the same `bronze.claims_raw`, or the
# MAGIC > `claim_txn_id` watermarks will collide.

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

# Incremental JDBC read: push the watermark predicate into Azure SQL so only new rows
# come across the wire. event_ts/created_at are DATETIMEOFFSET (stored in UTC); cast them
# to DATETIME2 server-side so JDBC hands Spark real timestamps — matching the Lakebase path
# and the timestamp cast the downstream silver step expects.
props = azure_sql_conn_properties()
pushdown_query = f"""(
    SELECT
        claim_txn_id, claim_id, policy_id, lob, region, peril, claim_status,
        transaction_type, claim_amount, currency, incident_date, reported_date,
        CAST(event_ts   AS DATETIME2) AS event_ts,
        CAST(created_at AS DATETIME2) AS created_at
    FROM {AZ_SQL_FQ_TABLE}
    WHERE claim_txn_id > {watermark}
) AS q"""

new_rows = (
    spark.read.format("jdbc")
    .option("url", azure_sql_jdbc_url())
    .option("dbtable", pushdown_query)
    .option("user", props["user"])
    .option("password", props["password"])
    .option("driver", AZ_SQL_DRIVER)
    .load()
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit(f"azuresql://{AZ_SQL_SERVER}/{AZ_SQL_FQ_TABLE}"))
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
