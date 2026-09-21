# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Provision Lakebase (managed Postgres)
# MAGIC Creates the Lakebase **database instance** that lands claims data, using the
# MAGIC Databricks SDK (already installed on the cluster). Idempotent — re-running just
# MAGIC reports the existing instance. Wait until state is **AVAILABLE** before continuing.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

w = WorkspaceClient()

try:
    inst = w.database.get_database_instance(name=LB_INSTANCE)
    print(f"instance '{LB_INSTANCE}' already exists — state={inst.state}")
except Exception:
    print(f"creating Lakebase instance '{LB_INSTANCE}' ({LB_CAPACITY}) ...")
    inst = w.database.create_database_instance(
        DatabaseInstance(name=LB_INSTANCE, capacity=LB_CAPACITY)
    )

# COMMAND ----------

# Poll until the instance is AVAILABLE (usually 1-3 minutes)
for i in range(60):
    inst = w.database.get_database_instance(name=LB_INSTANCE)
    state = str(inst.state)
    print(f"[{i}] state={state}")
    if "AVAILABLE" in state:
        break
    time.sleep(10)

print("\nLakebase instance ready:")
print(f"  name           : {inst.name}")
print(f"  state          : {inst.state}")
print(f"  read/write DNS : {getattr(inst, 'read_write_dns', 'n/a')}")
print("\nNext: run 02_create_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC > If your workspace exposes Lakebase only in the UI, create it once via
# MAGIC > **Compute → Database instances → Create** (name = `allianz-hackathon-db`,
# MAGIC > capacity = CU_1), then continue with notebook 02.
