# Databricks notebook source
# notebooks/stages/03_governance.py — Stage 3: Governance

# Applies Unity Catalog governance for every row in the config Delta table
# where run_governance=true and governance_config_path is set.
#
# Governance rules are sourced from:
#   config/data_governance.yml — column masking, tags, RLS, grants


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

print(f"[GOVERNANCE] run_id={run_id}")

engine = BulkTransformEngine(
    spark,
    config_base_path = CONFIG_BASE_PATH,
    parameter_yml    = PARAMETER_YML,
)

# COMMAND ----------
#  Apply governance for all applicable rows

applied_count = engine.run_governance_all(filter_ids=FILTER_IDS)

print(f"\n[GOVERNANCE]  Governance stage complete — {applied_count} table(s) processed")
