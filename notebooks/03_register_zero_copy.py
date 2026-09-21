# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Register Lakebase into Unity Catalog — ZERO-COPY
# MAGIC Registers the Lakebase database as a UC **database catalog**. After this you can
# MAGIC query the live Postgres rows from Databricks with **no ETL and no data movement**.
# MAGIC Also creates the medallion catalog + bronze/silver/gold/reference schemas.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseCatalog

w = WorkspaceClient()

# --- zero-copy registration ---------------------------------------------------
try:
    existing = w.catalogs.get(LAKEBASE_UC_CATALOG)
    print(f"UC catalog '{LAKEBASE_UC_CATALOG}' already exists")
except Exception:
    print(f"registering Lakebase '{LB_INSTANCE}/{LB_DATABASE}' as UC catalog '{LAKEBASE_UC_CATALOG}' ...")
    w.database.create_database_catalog(
        DatabaseCatalog(
            name=LAKEBASE_UC_CATALOG,
            database_instance_name=LB_INSTANCE,
            database_name=LB_DATABASE,
            create_database_if_not_exists=False,
        )
    )
    print("registered.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the medallion catalog + schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for s in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, REFERENCE_SCHEMA):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{s}")
    print(f"  + {CATALOG}.{s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the zero-copy read (reads live Postgres — no copy)

# COMMAND ----------

df = spark.sql(f"SELECT count(*) AS rows_in_lakebase FROM {ZERO_COPY_TABLE}")
display(df)
print("Next: run 04_firmwide_reference, then start the generator (05).")

# COMMAND ----------

# MAGIC %md
# MAGIC > If the SDK registration isn't available on your workspace, create it once via
# MAGIC > **Catalog Explorer → Create catalog → Database (Lakebase)**, pick the
# MAGIC > `allianz-hackathon-db` instance + `claims_db`, name it `lakebase_allianz`, then re-run
# MAGIC > the medallion-schema and verify cells above.
