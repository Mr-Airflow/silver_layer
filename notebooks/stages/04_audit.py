# Databricks notebook source
# notebooks/stages/04_audit.py — Stage 4: Audit

# Writes a pipeline run audit record for every row in the config Delta table
# where run_audit=true (default true — opt-out).
#
# Audit records are written to:
#   cdl_silver.logging.pipeline_run_log  (configured via parameter.yml)


# COMMAND ----------

import sys
import uuid
import time
import logging

# ---------------------------------------------------------------------------
# Configuration — sourced from parameter.yml / silver_config.csv
# ---------------------------------------------------------------------------

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
CONFIG_BASE_PATH = "/Workspace" + _nb_path.split("/notebooks")[0]

PARAMETER_YML    = "config/parameter.yml"
FILTER_IDS       = None   # None = run all enabled rows

run_id = dbutils.jobs.taskValues.get(
    taskKey="validate", key="run_id", debugValue=str(uuid.uuid4())
)
start_time_epoch = dbutils.jobs.taskValues.get(
    taskKey="validate", key="start_time_epoch", debugValue=int(time.time())
)

sys.path.insert(0, CONFIG_BASE_PATH)

# COMMAND ----------

from utils.transform_engine import BulkTransformEngine

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

print(f"[AUDIT] run_id={run_id}")

engine = BulkTransformEngine(
    spark,
    config_base_path = CONFIG_BASE_PATH,
    parameter_yml    = PARAMETER_YML,
)

# COMMAND ----------
#  Write audit records for all applicable rows

engine.run_audit_all(
    filter_ids=FILTER_IDS,
    run_id=run_id,
    start_time_epoch=start_time_epoch,
)

# COMMAND ----------
#  Mark processed rows as is_processed=True in the config Delta table

engine.mark_processed(filter_ids=FILTER_IDS)

print(f"\n[AUDIT]  Audit stage complete")
