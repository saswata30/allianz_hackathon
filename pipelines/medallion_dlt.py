# Databricks notebook source
# =============================================================================
# Medallion (Lakeflow Declarative Pipelines / DLT) — SILVER + GOLD
#   bronze.claims_raw  (written by 01_bronze_ingest, zero-copy from Lakebase)
#      -> silver.claims          streaming table + DATA QUALITY expectations
#      -> gold.claims_by_lob_region
#      -> gold.loss_ratio        (correlation with firmwide reference Delta)
#      -> gold.claims_daily
#
# Attach this file to a Lakeflow Declarative Pipeline (see pipelines/pipeline.json).
# Pipeline configuration keys (Advanced > Configuration):
#   hackathon.catalog          = allianz_hackathon
#   hackathon.bronze_schema    = bronze
#   hackathon.reference_schema = reference
# Pipeline target: catalog=allianz_hackathon, schema=silver  (gold tables are
# written with explicit gold.* names).
# =============================================================================
import dlt
from pyspark.sql import functions as F

CATALOG = spark.conf.get("hackathon.catalog", "allianz_hackathon")
BRONZE = spark.conf.get("hackathon.bronze_schema", "bronze")
REFERENCE = spark.conf.get("hackathon.reference_schema", "reference")

BRONZE_TABLE = f"{CATALOG}.{BRONZE}.claims_raw"
FIRMWIDE_TABLE = f"{CATALOG}.{REFERENCE}.firmwide_exposure"

VALID_LOBS = ["Property", "Motor", "Liability", "Marine", "Health", "Life"]
VALID_CCY = ["EUR", "GBP", "CHF", "USD"]

# --- Data quality rules -------------------------------------------------------
DQ_DROP = {  # violating rows are DROPPED and counted in the pipeline's DQ metrics
    "valid_claim_id": "claim_id IS NOT NULL",
    "valid_policy_id": "policy_id IS NOT NULL",
    "positive_amount": "claim_amount > 0",
    "valid_lob": f"lob IN ({', '.join(repr(x) for x in VALID_LOBS)})",
    "valid_currency": f"currency IN ({', '.join(repr(x) for x in VALID_CCY)})",
}
DQ_WARN = {  # violating rows are KEPT but flagged in metrics
    "dates_consistent": "reported_date >= incident_date",
    "amount_not_extreme": "claim_amount < 5000000",
}


# ============================== SILVER =======================================
@dlt.table(
    name="claims",
    comment="Cleansed, typed, quality-checked claim transactions (streaming from bronze).",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
@dlt.expect_all_or_drop(DQ_DROP)
@dlt.expect_all(DQ_WARN)
def silver_claims():
    return (
        spark.readStream.table(BRONZE_TABLE)
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


# ============================== GOLD =========================================
@dlt.table(
    name="gold_claims_by_lob_region",
    comment="Claim counts & incurred amounts by line of business and region.",
    table_properties={"quality": "gold"},
)
def gold_claims_by_lob_region():
    return (
        dlt.read("claims")
        .groupBy("lob", "region", "currency")
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


@dlt.table(
    name="gold_loss_ratio",
    comment="CORRELATION: streaming incurred claims vs firmwide written premium => loss ratio.",
    table_properties={"quality": "gold"},
)
def gold_loss_ratio():
    claims = (
        dlt.read("claims")
        .groupBy("lob", "region")
        .agg(
            F.sum("claim_amount").alias("incurred_amount"),
            F.count("*").alias("claim_txn_count"),
        )
    )
    firmwide = spark.read.table(FIRMWIDE_TABLE)
    return (
        claims.join(firmwide, ["lob", "region"], "right")
        .select(
            "lob",
            "region",
            F.coalesce("incurred_amount", F.lit(0)).cast("decimal(18,2)").alias("incurred_amount"),
            F.coalesce("claim_txn_count", F.lit(0)).alias("claim_txn_count"),
            "gross_written_premium",
            "target_loss_ratio",
            F.round(F.coalesce("incurred_amount", F.lit(0)) / F.col("gross_written_premium"), 4).alias("observed_loss_ratio"),
        )
        .withColumn(
            "vs_target",
            F.when(F.col("observed_loss_ratio") > F.col("target_loss_ratio"), F.lit("OVER"))
            .otherwise(F.lit("UNDER")),
        )
    )


@dlt.table(
    name="gold_claims_daily",
    comment="Daily claim volume & incurred trend for the AI/BI dashboard.",
    table_properties={"quality": "gold"},
)
def gold_claims_daily():
    return (
        dlt.read("claims")
        .withColumn("event_date", F.to_date("event_ts"))
        .groupBy("event_date", "lob")
        .agg(
            F.count("*").alias("claim_txn_count"),
            F.sum("claim_amount").alias("incurred_amount"),
            F.avg("report_lag_days").alias("avg_report_lag_days"),
        )
    )
