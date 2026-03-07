# Databricks notebook source

# notebooks/stages/05_optimize.py — Stage 5: Optimize

# Runs OPTIMIZE + VACUUM for every row in the config Delta table where
# run_optimize=true (default true — opt-out).
#
# Optimize settings (zorder_columns, vacuum_retain_hours) are sourced from
# silver_config.csv per row.


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

print(f"[OPTIMIZE] run_id={run_id}")

engine = BulkTransformEngine(
    spark,
    config_base_path = CONFIG_BASE_PATH,
    parameter_yml    = PARAMETER_YML,
)

# COMMAND ----------
#  Optimize all applicable tables

optimized_count = engine.run_optimize_all(filter_ids=FILTER_IDS)

print(f"\n[OPTIMIZE]  Optimize stage complete — {optimized_count} table(s) optimized")
