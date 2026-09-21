# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Shared configuration
# MAGIC `%run ./00_config` from every step notebook so all names live in one place.
# MAGIC Nothing here is workspace-specific — notebooks authenticate automatically as the
# MAGIC running user (no host, token, or profile required).

# COMMAND ----------

# --- Medallion (Unity Catalog) ------------------------------------------------
CATALOG = "allianz_hackathon"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
REFERENCE_SCHEMA = "reference"

# --- Lakebase (managed Postgres — OLTP landing) -------------------------------
LB_INSTANCE = "allianz-hackathon-db"   # Lakebase database instance name
LB_CAPACITY = "CU_1"                    # smallest capacity — fine for the workshop
LB_DATABASE = "claims_db"               # logical database created inside the instance
LB_SCHEMA = "claims"
LB_TABLE = "claim_transactions"

# --- Zero-copy: Lakebase registered into Unity Catalog ------------------------
LAKEBASE_UC_CATALOG = "lakebase_allianz"   # SELECT * FROM lakebase_allianz.claims.claim_transactions

# --- Generator ----------------------------------------------------------------
GEN_MIN_ROWS = 4
GEN_MAX_ROWS = 5

# Fully-qualified helpers
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.claims_raw"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.claims"
FIRMWIDE_TABLE = f"{CATALOG}.{REFERENCE_SCHEMA}.firmwide_exposure"
ZERO_COPY_TABLE = f"{LAKEBASE_UC_CATALOG}.{LB_SCHEMA}.{LB_TABLE}"

print(f"catalog            = {CATALOG}")
print(f"lakebase instance  = {LB_INSTANCE}  ({LB_DATABASE}.{LB_SCHEMA}.{LB_TABLE})")
print(f"zero-copy catalog  = {LAKEBASE_UC_CATALOG}  -> {ZERO_COPY_TABLE}")
