# Databricks notebook source
# notebooks/stages/00_validate.py — Stage 0: Validate
#
# Responsibilities:
#   1. Read parameter.yml and CREATE IF NOT EXISTS all infrastructure
#      catalogs, schemas, and tables (watermark, audit, logging, DQ, governance)
#   2. Write silver_config.csv as a queryable Delta table in Unity Catalog
#   3. Ensure every Silver target catalog + schema exists
#   4. Verify every source table is accessible (read-test)
#   5. Bootstrap watermark rows for first-time entries
#   6. Publish task values for downstream stages
#
# All configuration is sourced from:
#   config/parameter.yml     — catalog/schema/table locations
#   config/silver_config.csv — per-table transform definitions


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

run_id           = str(uuid.uuid4())
start_time_epoch = int(time.time())

sys.path.insert(0, CONFIG_BASE_PATH)

# COMMAND ----------

from utils.transform_engine import BulkTransformEngine

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

# BulkTransformEngine reads parameter.yml automatically and derives
# self.control_table (watermark, in logging schema) and self.config_table.

engine = BulkTransformEngine(
    spark,
    config_base_path = CONFIG_BASE_PATH,
    parameter_yml    = PARAMETER_YML,
)

# COMMAND ----------
#  Run validation (infra setup + CSV → Delta + source checks)

validation    = engine.validate_all(filter_ids=FILTER_IDS)
table_results = validation["table_results"]
table_count   = len(table_results)

# COMMAND ----------
#  Publish task values for downstream stages

dbutils.jobs.taskValues.set(key="run_id",           value=run_id)
dbutils.jobs.taskValues.set(key="start_time_epoch", value=start_time_epoch)
dbutils.jobs.taskValues.set(key="table_count",      value=table_count)

dbutils.jobs.taskValues.set(key="any_run_dq",
    value=str(validation["any_run_dq"]).lower())
dbutils.jobs.taskValues.set(key="any_run_governance",
    value=str(validation["any_run_governance"]).lower())
dbutils.jobs.taskValues.set(key="any_run_optimize",
    value=str(validation["any_run_optimize"]).lower())

print(
    f"\n[VALIDATE]  Pre-flight complete — {table_count} table(s) ready  run_id={run_id}\n"
    f"  Stages enabled →  DQ={validation['any_run_dq']}  "
    f"Gov={validation['any_run_governance']}  "
    f"Optimize={validation['any_run_optimize']}  "
    f"Audit=yes (always)"
)
