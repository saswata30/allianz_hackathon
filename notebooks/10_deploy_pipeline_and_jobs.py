# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Deploy the medallion + 2-minute jobs (SDK, no CLI)
# MAGIC Creates, all from this notebook:
# MAGIC * the **silver + gold** medallion as a plain PySpark batch job (`07_medallion_no_dlt`)
# MAGIC   that rebuilds silver/gold every 2 minutes
# MAGIC * a **generator job** every 2 minutes (mode=once)
# MAGIC * a **bronze ingest job** every 2 minutes
# MAGIC
# MAGIC Pick the OLTP source with the **`source`** widget:
# MAGIC * **`lakebase`** (default) → schedules `05_claims_generator` + `06_bronze_ingest`
# MAGIC * **`azuresql`** (optional) → schedules `05a_claims_generator_azuresql` +
# MAGIC   `06a_bronze_ingest_azuresql` (requires the Azure SQL config + `02a` run first)
# MAGIC
# MAGIC The medallion reads from `bronze.claims_raw` regardless of source.
# MAGIC Assumes the relevant notebooks already live in this same workspace folder.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, CronSchedule, PauseStatus

w = WorkspaceClient()

dbutils.widgets.dropdown("source", "lakebase", ["lakebase", "azuresql"], "OLTP source")
SOURCE = dbutils.widgets.get("source")

# notebooks to schedule for the chosen source (both feed the same bronze.claims_raw)
GEN_NB, BRONZE_NB = {
    "lakebase": ("05_claims_generator", "06_bronze_ingest"),
    "azuresql": ("05a_claims_generator_azuresql", "06a_bronze_ingest_azuresql"),
}[SOURCE]
SUFFIX = "" if SOURCE == "lakebase" else "_azuresql"
print(f"source = {SOURCE}  ->  generator={GEN_NB}, bronze={BRONZE_NB}")
print("medallion = batch job on 07_medallion_no_dlt")

# folder that holds these notebooks (so job paths are correct)
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
NB_DIR = "/".join(nb_path.split("/")[:-1])
print(f"notebook dir: {NB_DIR}")

EVERY_2_MIN = "0 0/2 * * * ?"

# COMMAND ----------

# MAGIC %md ## Jobs — generator + bronze ingest + medallion, every 2 minutes (serverless)

# COMMAND ----------

def upsert_job(name: str, notebook: str, params: dict):
    for j in w.jobs.list(name=name):
        print(f"job '{name}' already exists: {j.job_id}")
        return j.job_id
    created = w.jobs.create(
        name=name,
        tasks=[Task(task_key="run", notebook_task=NotebookTask(
            notebook_path=f"{NB_DIR}/{notebook}", base_parameters=params))],
        schedule=CronSchedule(quartz_cron_expression=EVERY_2_MIN,
                              timezone_id="UTC", pause_status=PauseStatus.UNPAUSED),
        max_concurrent_runs=1,
        tags={"project": "allianz_hackathon"},
    )
    print(f"created job '{name}': {created.job_id}")
    return created.job_id


gen_job = upsert_job(f"allianz_hackathon_generator_2min{SUFFIX}", GEN_NB, {"mode": "once"})
bronze_job = upsert_job(f"allianz_hackathon_bronze_ingest_2min{SUFFIX}", BRONZE_NB, {})

# Rebuild silver+gold with a plain PySpark job every 2 min.
medallion_job = upsert_job("allianz_hackathon_medallion_batch_2min", "07_medallion_no_dlt", {})

# COMMAND ----------

print("\nDeployed. Generator + bronze + 07_medallion_no_dlt run every 2 min; "
      "the batch job rebuilds silver/gold each cycle.")
print("Next: build the dashboard (08) and Genie space (09).")
