# Databricks notebook source

# notebooks/stages/02_data_quality.py — Stage 2: Data Quality
#
# Runs ONLY for tables where run_data_quality=true in silver_config.csv.
# Stage 1 (transform) defers these tables entirely — no Silver write happens
# in Stage 1 for DQ-enabled tables.
#
# This stage runs the full pipeline for each DQ-enabled table:
#   Bronze read → incremental filter → SQL transform
#   → DQ filter (quarantine bad rows) → write valid rows to Silver
#   → watermark update → control table update → migration log
#
# Data is only written to Silver AFTER DQ checks pass — ensuring a single,
# clean DQ gate with no duplicate processing.
#
# DQ rules are sourced per-table from the dq_config_path column in
# silver_config.csv (e.g. config/dq/cust_001.yml).


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

print(f"[DQ] run_id={run_id}  (post-write Silver validation)")

engine = BulkTransformEngine(
    spark,
    config_base_path = CONFIG_BASE_PATH,
    parameter_yml    = PARAMETER_YML,
)

# COMMAND ----------
#  Run post-write DQ scan for all applicable rows

dq_stats = engine.run_dq_all(filter_ids=FILTER_IDS, run_id=run_id)

# COMMAND ----------
#  Publish task values

dbutils.jobs.taskValues.set(key="dq_pass_count",     value=dq_stats["pass_rows"])
dbutils.jobs.taskValues.set(key="dq_fail_count",     value=dq_stats["fail_rows"])
dbutils.jobs.taskValues.set(key="dq_tables_checked", value=dq_stats["tables_checked"])

print(
    f"\n[DQ]  Post-write validation complete — "
    f"pass={dq_stats['pass_rows']:,}  fail={dq_stats['fail_rows']:,}  "
    f"tables={dq_stats['tables_checked']}"
)
