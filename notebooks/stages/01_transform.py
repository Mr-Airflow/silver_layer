# Databricks notebook source
# notebooks/stages/01_transform.py — Stage 1: Transform

# Reads every enabled row from the config Delta table and runs the transform
# pipeline for each:
#   Bronze read → incremental filter (per-row load_type) → SQL → write Silver
#   → update watermark → update control table → write transformation log
#
# All configuration is sourced from:
#   config/parameter.yml     — catalog/schema/table locations
#   config/silver_config.csv — per-table transform definitions (loaded as Delta by Stage 0)


# COMMAND ----------

import sys
import uuid
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

sys.path.insert(0, CONFIG_BASE_PATH)

# COMMAND ----------

from utils.transform_engine import BulkTransformEngine

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# Engine derives watermark table (in logging schema) and config_table from
# parameter.yml automatically. Transforms are read from the config Delta table
# written by the validate stage.

engine = BulkTransformEngine(
    spark,
    config_base_path = CONFIG_BASE_PATH,
    parameter_yml    = PARAMETER_YML,
)

# COMMAND ----------
#  Run transform stage for all enabled rows

results = engine.transform_all(filter_ids=FILTER_IDS, stop_on_error=False, run_id=run_id)

# COMMAND ----------
#  Publish task values

success_count      = sum(1 for r in results if r.status == "SUCCESS")
skipped_count      = sum(1 for r in results if r.status == "SKIPPED")
failed_count       = sum(1 for r in results if r.status == "FAILED")
total_rows_written = sum(r.rows_loaded for r in results if r.status == "SUCCESS")

dbutils.jobs.taskValues.set(key="total_rows_written", value=total_rows_written)
dbutils.jobs.taskValues.set(key="success_count",      value=success_count)
dbutils.jobs.taskValues.set(key="skipped_count",      value=skipped_count)
dbutils.jobs.taskValues.set(key="failed_count",       value=failed_count)

print(
    f"\n[TRANSFORM]  Complete — {success_count}/{len(results)} succeeded  "
    f"{skipped_count} skipped (is_processed=true)  "
    f"{total_rows_written:,} rows in Silver"
)
