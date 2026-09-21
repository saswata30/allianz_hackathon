# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Deploy the DLT pipeline + 2-minute jobs (SDK, no CLI)
# MAGIC Creates, all from this notebook:
# MAGIC * the **Lakeflow Declarative Pipeline** (silver + gold) attached to `07_medallion_dlt`
# MAGIC * a **generator job** running `05_claims_generator` (mode=once) every 2 minutes
# MAGIC * a **bronze ingest job** running `06_bronze_ingest` every 2 minutes
# MAGIC
# MAGIC Assumes notebooks 00–07 already live in this same workspace folder.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.service.jobs import Task, NotebookTask, CronSchedule, PauseStatus

w = WorkspaceClient()

# folder that holds these notebooks (so job/pipeline paths are correct)
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
NB_DIR = "/".join(nb_path.split("/")[:-1])
print(f"notebook dir: {NB_DIR}")

EVERY_2_MIN = "0 0/2 * * * ?"

# COMMAND ----------

# MAGIC %md ## DLT pipeline (silver + gold)

# COMMAND ----------

existing = [p for p in w.pipelines.list_pipelines() if p.name == "allianz_hackathon_medallion"]
if existing:
    pipeline_id = existing[0].pipeline_id
    print(f"pipeline already exists: {pipeline_id}")
else:
    created = w.pipelines.create(
        name="allianz_hackathon_medallion",
        catalog=CATALOG,
        target=SILVER_SCHEMA,
        serverless=True,
        development=True,
        continuous=False,
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path=f"{NB_DIR}/07_medallion_dlt"))],
        configuration={
            "hackathon.catalog": CATALOG,
            "hackathon.bronze_schema": BRONZE_SCHEMA,
            "hackathon.reference_schema": REFERENCE_SCHEMA,
        },
    )
    pipeline_id = created.pipeline_id
    print(f"created pipeline: {pipeline_id}")

# COMMAND ----------

# MAGIC %md ## Jobs — generator + bronze ingest, every 2 minutes (serverless)

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


gen_job = upsert_job("allianz_hackathon_generator_2min", "05_claims_generator", {"mode": "once"})
bronze_job = upsert_job("allianz_hackathon_bronze_ingest_2min", "06_bronze_ingest", {})

# COMMAND ----------

# MAGIC %md ## Start the pipeline

# COMMAND ----------

w.pipelines.start_update(pipeline_id=pipeline_id)
print(f"started pipeline update: {pipeline_id}")
print("\nDeployed. Generator + bronze run every 2 min; DLT refreshes silver/gold.")
print("Next: build the dashboard (08) and Genie space (09).")
