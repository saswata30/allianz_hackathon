# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Medallion SILVER + GOLD in plain PySpark
# MAGIC Builds the medallion with plain Spark batch writes — no `dlt` module required:
# MAGIC
# MAGIC * `silver.claims` — cleansed, typed, **quality-checked** claims (bad rows dropped, warn
# MAGIC   rules reported), rebuilt from `bronze.claims_raw`.
# MAGIC * `gold.gold_claims_by_lob_region`, `gold.gold_loss_ratio`, `gold.gold_claims_daily` —
# MAGIC   aggregates + firmwide correlation.
# MAGIC
# MAGIC Idempotent: each run **fully rebuilds** silver + gold from the current bronze. Run after
# MAGIC `06`/`06a`, or schedule it every 2 minutes as a job (see notebook 10). Works for **both**
# MAGIC the Lakebase and Azure SQL source paths — it only reads `bronze.claims_raw`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

GOLD_BY_LOB_REGION = f"{CATALOG}.{GOLD_SCHEMA}.gold_claims_by_lob_region"
GOLD_LOSS_RATIO    = f"{CATALOG}.{GOLD_SCHEMA}.gold_loss_ratio"
GOLD_CLAIMS_DAILY  = f"{CATALOG}.{GOLD_SCHEMA}.gold_claims_daily"

VALID_LOBS = ["Property", "Motor", "Liability", "Marine", "Health", "Life"]
VALID_CCY = ["EUR", "GBP", "CHF", "USD"]

DQ_DROP = {
    "valid_claim_id": "claim_id IS NOT NULL",
    "valid_policy_id": "policy_id IS NOT NULL",
    "positive_amount": "claim_amount > 0",
    "valid_lob": f"lob IN ({', '.join(repr(x) for x in VALID_LOBS)})",
    "valid_currency": f"currency IN ({', '.join(repr(x) for x in VALID_CCY)})",
}
DQ_WARN = {
    "dates_consistent": "reported_date >= incident_date",
    "amount_not_extreme": "claim_amount < 5000000",
}

# Ensure the target schemas exist (bronze is created by the ingest notebook).
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in (SILVER_SCHEMA, GOLD_SCHEMA):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")

# COMMAND ----------

# MAGIC %md ## SILVER — cleanse, type, and apply data-quality rules

# COMMAND ----------

if not spark.catalog.tableExists(BRONZE_TABLE):
    raise ValueError(f"{BRONZE_TABLE} does not exist yet — run 06 / 06a (bronze ingest) first")

typed = (
    spark.read.table(BRONZE_TABLE)
    .select(
        F.col("claim_txn_id").cast("long"),
        F.trim("claim_id").alias("claim_id"),
        F.trim("policy_id").alias("policy_id"),
        F.initcap(F.trim("lob")).alias("lob"),
        F.initcap(F.trim("region")).alias("region"),
        F.trim("peril").alias("peril"),
        F.trim("claim_status").alias("claim_status"),
        F.trim("transaction_type").alias("transaction_type"),
        F.col("claim_amount").cast("decimal(14,2)"),
        F.upper(F.trim("currency")).alias("currency"),
        F.col("incident_date").cast("date"),
        F.col("reported_date").cast("date"),
        F.col("event_ts").cast("timestamp"),
        F.col("_ingested_at").cast("timestamp"),
    )
    .withColumn("claim_month", F.date_trunc("month", F.col("event_ts")))
    .withColumn("is_large_loss", F.col("claim_amount") >= F.lit(250000))
    .withColumn("report_lag_days", F.datediff("reported_date", "incident_date"))
)

bronze_count = typed.count()

# DROP rules: keep only rows that pass every drop expectation (report how many fail).
keep_expr = " AND ".join(f"({cond})" for cond in DQ_DROP.values())
silver = typed.where(keep_expr)
silver_count = silver.count()
print(f"data quality (drop): {bronze_count} bronze -> {silver_count} silver "
      f"({bronze_count - silver_count} rows dropped)")

# WARN rules: keep the rows, just report violations.
for name, cond in DQ_WARN.items():
    warn_violations = silver.where(f"NOT ({cond})").count()
    print(f"  warn '{name}': {warn_violations} rows violate `{cond}`")

# COMMAND ----------

(silver.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE))
print(f"wrote {silver_count} rows -> {SILVER_TABLE}")

# Read silver back so gold aggregates off the persisted table.
silver = spark.table(SILVER_TABLE)

# COMMAND ----------

# MAGIC %md ## GOLD — aggregates + firmwide correlation

# COMMAND ----------

# gold_claims_by_lob_region — counts & incurred amounts by LOB and region.
gold_by_lob_region = (
    silver.groupBy("lob", "region", "currency")
    .agg(
        F.count("*").alias("claim_txn_count"),
        F.countDistinct("claim_id").alias("distinct_claims"),
        F.sum("claim_amount").alias("incurred_amount"),
        F.avg("claim_amount").alias("avg_claim_amount"),
        F.max("claim_amount").alias("max_claim_amount"),
        F.sum(F.when(F.col("is_large_loss"), 1).otherwise(0)).alias("large_loss_count"),
        F.max("event_ts").alias("last_event_ts"),
    )
)
(gold_by_lob_region.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(GOLD_BY_LOB_REGION))
print(f"wrote {GOLD_BY_LOB_REGION}")

# COMMAND ----------

# gold_loss_ratio — CORRELATION: streaming incurred claims vs firmwide written premium.
claims_agg = (
    silver.groupBy("lob", "region")
    .agg(F.sum("claim_amount").alias("incurred_amount"),
         F.count("*").alias("claim_txn_count"))
)
firmwide = spark.read.table(FIRMWIDE_TABLE)
gold_loss_ratio = (
    claims_agg.join(firmwide, ["lob", "region"], "right")
    .select(
        "lob", "region",
        F.coalesce("incurred_amount", F.lit(0)).cast("decimal(18,2)").alias("incurred_amount"),
        F.coalesce("claim_txn_count", F.lit(0)).alias("claim_txn_count"),
        "gross_written_premium", "target_loss_ratio",
        F.round(F.coalesce("incurred_amount", F.lit(0)) / F.col("gross_written_premium"), 4)
            .alias("observed_loss_ratio"),
    )
    .withColumn("vs_target",
                F.when(F.col("observed_loss_ratio") > F.col("target_loss_ratio"),
                       F.lit("OVER")).otherwise(F.lit("UNDER")))
)
(gold_loss_ratio.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(GOLD_LOSS_RATIO))
print(f"wrote {GOLD_LOSS_RATIO}")

# COMMAND ----------

# gold_claims_daily — daily volume & incurred trend for the AI/BI dashboard.
gold_claims_daily = (
    silver.withColumn("event_date", F.to_date("event_ts"))
    .groupBy("event_date", "lob")
    .agg(F.count("*").alias("claim_txn_count"),
         F.sum("claim_amount").alias("incurred_amount"),
         F.avg("report_lag_days").alias("avg_report_lag_days"))
)
(gold_claims_daily.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(GOLD_CLAIMS_DAILY))
print(f"wrote {GOLD_CLAIMS_DAILY}")

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

print(f"silver.claims                     : {spark.table(SILVER_TABLE).count()} rows")
print(f"gold.gold_claims_by_lob_region    : {spark.table(GOLD_BY_LOB_REGION).count()} rows")
print(f"gold.gold_loss_ratio              : {spark.table(GOLD_LOSS_RATIO).count()} rows")
print(f"gold.gold_claims_daily            : {spark.table(GOLD_CLAIMS_DAILY).count()} rows")
display(spark.table(GOLD_LOSS_RATIO).orderBy(F.col("observed_loss_ratio").desc()))
