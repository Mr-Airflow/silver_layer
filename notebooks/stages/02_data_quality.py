# Databricks notebook source

# notebooks/stages/02_data_quality.py — Stage 2: Data Quality

# Runs a DQX post-write scan for every row in the config Delta table where
# run_data_quality=true and dq_config_path is set.
#
# DQ rules are sourced from:
#   config/data_quality.yml  — DQX rule definitions per table


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

print(f"[DQ] run_id={run_id}")

engine = BulkTransformEngine(
    spark,
    config_base_path = CONFIG_BASE_PATH,
    parameter_yml    = PARAMETER_YML,
)

# COMMAND ----------
#  Run DQ for all applicable rows

dq_stats = engine.run_dq_all(filter_ids=FILTER_IDS)

# COMMAND ----------
#  Publish task values

dbutils.jobs.taskValues.set(key="dq_pass_count",     value=dq_stats["pass_rows"])
dbutils.jobs.taskValues.set(key="dq_fail_count",     value=dq_stats["fail_rows"])
dbutils.jobs.taskValues.set(key="dq_tables_checked", value=dq_stats["tables_checked"])

print(
    f"\n[DQ]  Data quality stage complete — "
    f"pass={dq_stats['pass_rows']:,}  fail={dq_stats['fail_rows']:,}  "
    f"tables={dq_stats['tables_checked']}"
)
