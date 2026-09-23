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

# --- Azure SQL (OPTIONAL alternative OLTP landing) ----------------------------
# Target: an Azure SQL Managed Instance reached over its PUBLIC endpoint (port 3342).
# The workshop can generate claims into Azure SQL instead of Lakebase (02a / 05a / 06a).
#
# SECRETS — the SQL login is NEVER stored in this (public) repo. Before running the
# Azure SQL path, store the login once in a Databricks secret scope:
#   databricks secrets create-scope allianz_hackathon
#   databricks secrets put-secret  allianz_hackathon azuresql_user      # value: databricks_svc
#   databricks secrets put-secret  allianz_hackathon azuresql_password  # value: <the SQL password>
# (or set them in the workspace UI: Compute → Secrets, or the Secrets REST API).
#
# MI public-endpoint prereqs: the managed instance must have the public endpoint
# ENABLED, and its NSG must allow inbound TCP 3342 from the Databricks/serverless egress.
AZ_SQL_SERVER       = "hacking-sql-mi.public.bddadad65355.database.windows.net"
AZ_SQL_PORT         = 3342     # SQL MI public endpoint (private endpoint uses 1433)
AZ_SQL_DATABASE     = "Hacking_SQL_Test"
AZ_SQL_SCHEMA       = "claims"
AZ_SQL_TABLE        = "claim_transactions"
AZ_SQL_DRIVER       = "com.microsoft.sqlserver.jdbc.SQLServerDriver"  # bundled in DBR
AZ_SQL_SECRET_SCOPE = "allianz_hackathon"   # secret scope holding the SQL login
AZ_SQL_USER_KEY     = "azuresql_user"       # -> databricks_svc
AZ_SQL_PASSWORD_KEY = "azuresql_password"

AZ_SQL_FQ_TABLE = f"{AZ_SQL_SCHEMA}.{AZ_SQL_TABLE}"


def azure_sql_jdbc_url():
    """JDBC URL for Azure SQL (Managed Instance public endpoint), encrypted + cert-verified.

    hostNameInCertificate=*.database.windows.net matches the MI TLS cert. If the TLS
    handshake fails on the public endpoint, a demo fallback is to drop
    hostNameInCertificate and set trustServerCertificate=true (skips cert validation —
    less secure, so prefer fixing the cert/DNS path).
    """
    return (
        f"jdbc:sqlserver://{AZ_SQL_SERVER}:{AZ_SQL_PORT};"
        f"database={AZ_SQL_DATABASE};encrypt=true;trustServerCertificate=false;"
        f"hostNameInCertificate=*.database.windows.net;loginTimeout=30"
    )


def require_azure_sql_config():
    """Fail fast with a clear message if the developer hasn't filled in the TBDs."""
    missing = [
        name for name, val in (
            ("AZ_SQL_SERVER", AZ_SQL_SERVER),
            ("AZ_SQL_DATABASE", AZ_SQL_DATABASE),
            ("AZ_SQL_SECRET_SCOPE", AZ_SQL_SECRET_SCOPE),
        )
        if not val or val == "TBD"
    ]
    if missing:
        raise ValueError(
            "Azure SQL config not set — edit 00_config and replace TBD for: "
            + ", ".join(missing)
        )


def azure_sql_conn_properties():
    """JDBC connection properties, with the login read from the secret scope."""
    require_azure_sql_config()
    return {
        "user": dbutils.secrets.get(AZ_SQL_SECRET_SCOPE, AZ_SQL_USER_KEY),        # noqa: F821
        "password": dbutils.secrets.get(AZ_SQL_SECRET_SCOPE, AZ_SQL_PASSWORD_KEY),  # noqa: F821
        "driver": AZ_SQL_DRIVER,
    }


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
print(f"azure sql (opt.)   = {AZ_SQL_SERVER}  ({AZ_SQL_DATABASE}.{AZ_SQL_FQ_TABLE})")
