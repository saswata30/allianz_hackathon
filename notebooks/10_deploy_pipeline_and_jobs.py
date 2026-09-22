# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Deploy the medallion + 2-minute jobs (SDK, no CLI)
# MAGIC Creates, all from this notebook:
# MAGIC * the **silver + gold** medallion — either a Lakeflow Declarative Pipeline *or* a plain
# MAGIC   PySpark batch job (see the `engine` widget)
# MAGIC * a **generator job** every 2 minutes (mode=once)
# MAGIC * a **bronze ingest job** every 2 minutes
# MAGIC
# MAGIC Pick the OLTP source with the **`source`** widget:
# MAGIC * **`lakebase`** (default) → schedules `05_claims_generator` + `06_bronze_ingest`
# MAGIC * **`azuresql`** (optional) → schedules `05a_claims_generator_azuresql` +
# MAGIC   `06a_bronze_ingest_azuresql` (requires the Azure SQL config + `02a` run first)
# MAGIC
# MAGIC Pick the medallion engine with the **`engine`** widget:
# MAGIC * **`dlt`** (default) → creates + starts the Lakeflow Declarative Pipeline on `07_medallion_dlt`
# MAGIC * **`batch`** → schedules `07_medallion_no_dlt` as a 2-minute job (use this when the `dlt`
# MAGIC   module isn't available in your workspace)
# MAGIC
# MAGIC The medallion reads from `bronze.claims_raw` regardless of source or engine.
# MAGIC Assumes the relevant notebooks already live in this same workspace folder.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.service.jobs import Task, NotebookTask, CronSchedule, PauseStatus

w = WorkspaceClient()

dbutils.widgets.dropdown("source", "lakebase", ["lakebase", "azuresql"], "OLTP source")
dbutils.widgets.dropdown("engine", "dlt", ["dlt", "batch"], "Medallion engine")
SOURCE = dbutils.widgets.get("source")
ENGINE = dbutils.widgets.get("engine")

# notebooks to schedule for the chosen source (both feed the same bronze.claims_raw)
GEN_NB, BRONZE_NB = {
    "lakebase": ("05_claims_generator", "06_bronze_ingest"),
    "azuresql": ("05a_claims_generator_azuresql", "06a_bronze_ingest_azuresql"),
}[SOURCE]
SUFFIX = "" if SOURCE == "lakebase" else "_azuresql"
print(f"source = {SOURCE}  ->  generator={GEN_NB}, bronze={BRONZE_NB}")
print(f"engine = {ENGINE}   ->  {'DLT pipeline on 07_medallion_dlt' if ENGINE == 'dlt' else 'batch job on 07_medallion_no_dlt'}")

# folder that holds these notebooks (so job/pipeline paths are correct)
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
NB_DIR = "/".join(nb_path.split("/")[:-1])
print(f"notebook dir: {NB_DIR}")

EVERY_2_MIN = "0 0/2 * * * ?"

# COMMAND ----------

# MAGIC %md ## Medallion — DLT pipeline (engine=dlt only)

# COMMAND ----------

pipeline_id = None
if ENGINE == "dlt":
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
else:
    print("engine=batch — skipping the DLT pipeline; 07_medallion_no_dlt is scheduled as a job below")

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


gen_job = upsert_job(f"allianz_hackathon_generator_2min{SUFFIX}", GEN_NB, {"mode": "once"})
bronze_job = upsert_job(f"allianz_hackathon_bronze_ingest_2min{SUFFIX}", BRONZE_NB, {})

# engine=batch: no DLT pipeline, so rebuild silver+gold with a plain PySpark job every 2 min.
if ENGINE == "batch":
    medallion_job = upsert_job("allianz_hackathon_medallion_batch_2min", "07_medallion_no_dlt", {})

# COMMAND ----------

# MAGIC %md ## Start the pipeline (engine=dlt only)

# COMMAND ----------

if ENGINE == "dlt":
    w.pipelines.start_update(pipeline_id=pipeline_id)
    print(f"started pipeline update: {pipeline_id}")
    print("\nDeployed. Generator + bronze run every 2 min; DLT refreshes silver/gold.")
else:
    print("\nDeployed. Generator + bronze + 07_medallion_no_dlt run every 2 min; "
          "the batch job rebuilds silver/gold each cycle.")
print("Next: build the dashboard (08) and Genie space (09).")
