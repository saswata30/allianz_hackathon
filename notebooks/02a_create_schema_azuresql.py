# Databricks notebook source
# MAGIC %md
# MAGIC # 02a · (OPTIONAL) Create the Azure SQL schema & table
# MAGIC Alternative OLTP landing zone: **Azure SQL Server** instead of Lakebase. Run this
# MAGIC *instead of* `02_create_schema` when you want claims to land in Azure SQL.
# MAGIC
# MAGIC Creates `claims.claim_transactions` in your Azure SQL Database. The table mirrors the
# MAGIC Lakebase schema exactly (same columns, a monotonic `claim_txn_id` watermark) so the
# MAGIC downstream medallion / DLT / dashboard / Genie all work unchanged.
# MAGIC
# MAGIC **Prerequisites** (bring your own Azure SQL — a notebook can't provision Azure infra):
# MAGIC * An Azure SQL Server + Database reachable from the workspace (firewall: *Allow Azure
# MAGIC   services*, or add the workspace egress IPs).
# MAGIC * `AZ_SQL_SERVER` / `AZ_SQL_DATABASE` set in `00_config`, and the login stored in a
# MAGIC   secret scope (`AZ_SQL_SECRET_SCOPE`). See the comments in `00_config`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the schema + table via JDBC
# MAGIC The SQL Server JDBC driver ships with the Databricks runtime, so no `%pip` install is
# MAGIC needed. We reuse the SDK-managed Spark JVM connection to run idempotent DDL.

# COMMAND ----------

DDL_STATEMENTS = [
    # schema
    f"""
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{AZ_SQL_SCHEMA}')
        EXEC('CREATE SCHEMA {AZ_SQL_SCHEMA}')
    """,
    # table — claim_txn_id IDENTITY is the monotonic watermark for incremental ingest
    f"""
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '{AZ_SQL_SCHEMA}' AND t.name = '{AZ_SQL_TABLE}'
    )
    CREATE TABLE {AZ_SQL_SCHEMA}.{AZ_SQL_TABLE} (
        claim_txn_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
        claim_id          VARCHAR(20)    NOT NULL,
        policy_id         VARCHAR(20)    NOT NULL,
        lob               VARCHAR(40)    NOT NULL,
        region            VARCHAR(40)    NOT NULL,
        peril             VARCHAR(60)    NULL,
        claim_status      VARCHAR(30)    NOT NULL,
        transaction_type  VARCHAR(30)    NOT NULL,
        claim_amount      DECIMAL(14,2)  NOT NULL,
        currency          VARCHAR(3)     NOT NULL,
        incident_date     DATE           NULL,
        reported_date     DATE           NULL,
        event_ts          DATETIMEOFFSET NOT NULL DEFAULT SYSUTCDATETIME(),
        created_at        DATETIMEOFFSET NOT NULL DEFAULT SYSUTCDATETIME()
    )
    """,
    # helpful indexes (idempotent)
    f"""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_{AZ_SQL_TABLE}_event_ts')
        CREATE INDEX idx_{AZ_SQL_TABLE}_event_ts ON {AZ_SQL_SCHEMA}.{AZ_SQL_TABLE} (event_ts)
    """,
    f"""
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_{AZ_SQL_TABLE}_lob_region')
        CREATE INDEX idx_{AZ_SQL_TABLE}_lob_region ON {AZ_SQL_SCHEMA}.{AZ_SQL_TABLE} (lob, region)
    """,
]

# COMMAND ----------

# Run the DDL through the JVM JDBC connection (Statement.execute per statement).
props = azure_sql_conn_properties()
jvm = spark._sc._jvm  # py4j gateway to the driver JVM
driver_mgr = jvm.java.sql.DriverManager

conn = driver_mgr.getConnection(azure_sql_jdbc_url(), props["user"], props["password"])
try:
    stmt = conn.createStatement()
    for sql in DDL_STATEMENTS:
        stmt.execute(sql)
    stmt.close()
    print(f"{AZ_SQL_FQ_TABLE} ready in {AZ_SQL_DATABASE}")
finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the table is readable from Spark

# COMMAND ----------

df = (
    spark.read.format("jdbc")
    .option("url", azure_sql_jdbc_url())
    .option("dbtable", AZ_SQL_FQ_TABLE)
    .option("user", props["user"])
    .option("password", props["password"])
    .option("driver", AZ_SQL_DRIVER)
    .load()
)
print(f"columns: {len(df.columns)}  |  rows so far: {df.count()}")
print("\nNext: run 03_register_zero_copy (medallion catalog/schemas), then 05a to stream.")
