# Databricks notebook source

# notebooks/run_bulk_transforms.py — Bulk Transform Orchestrator
#
# Single-notebook entry point that runs the full Silver pipeline in one shot.
# Internally delegates to the same staged methods used by notebooks/stages/00–05,
# so both execution paths share one code base and one set of behaviours:
#
#   run_all()  →  validate_all()       Stage 0: infra + source checks
#              →  transform_all()      Stage 1: Bronze → DQ pre-write → Silver
#              →  run_governance_all() Stage 2 (governance)
#              →  run_optimize_all()   Stage 3
#              →  run_audit_all()      Stage 4
#              →  mark_processed()     Stage 5
#
# All configuration is driven by:
#   config/parameter.yml     — catalog/schema/table locations
#   config/silver_config.csv — per-table transform definitions

# COMMAND ----------

import sys
import logging

# ---------------------------------------------------------------------------
# Configuration — all values sourced from parameter.yml / silver_config.csv
# ---------------------------------------------------------------------------

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
CONFIG_BASE_PATH    = "/Workspace" + _nb_path.split("/notebooks")[0]

PARAMETER_YML       = "config/parameter.yml"
BULK_TRANSFORMS_CSV = "config/silver_config.csv"
FILTER_IDS          = None   # None = run all enabled rows
STOP_ON_ERROR       = False
DRY_RUN             = False

sys.path.insert(0, CONFIG_BASE_PATH)

# COMMAND ----------

from utils.transform_engine import BulkTransformEngine

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

print("=" * 65)
print("  Bulk Transform Orchestrator")
print("=" * 65)
print(f"  config_base_path : {CONFIG_BASE_PATH}")
print(f"  csv              : {BULK_TRANSFORMS_CSV}")
print(f"  parameter_yml    : {PARAMETER_YML}")
print(f"  filter_ids       : {FILTER_IDS or '(all enabled)'}")
print(f"  stop_on_error    : {STOP_ON_ERROR}")
print(f"  dry_run          : {DRY_RUN}")
print("=" * 65)

# COMMAND ----------

#  Initialise engine — reads parameter.yml automatically

engine = BulkTransformEngine(
    spark,
    config_base_path = CONFIG_BASE_PATH,
    csv_path         = BULK_TRANSFORMS_CSV,
    parameter_yml    = PARAMETER_YML,
)

# COMMAND ----------

#  Dry-run: load and display the plan without executing
#  (reads from CSV directly for plan display — Delta table is written by run_all → validate_all)

transforms = engine.load_transforms(filter_ids=FILTER_IDS)

print(f"\n  {len(transforms)} transform(s) {'would be' if DRY_RUN else 'will be'} executed:\n")

header = f"  {'ID':<30} {'STRATEGY':<14} {'LOAD_TYPE':<14} {'SOURCE':<40} {'TARGET':<40} {'WM_COL':<20}"
print(header)
print("  " + "-" * (len(header) - 2))

for t in transforms:
    full_source = (
        f"{t.get('source_catalog','')}.{t.get('source_schema','')}.{t.get('source_table','')}"
        if t.get("source_table")
        else "(from sql_file)"
    )
    full_target = f"{t['target_catalog']}.{t['target_schema']}.{t['target_table']}"
    wm = t.get("watermark_column") or "(none)"
    print(
        f"  {t['id']:<30} {t.get('load_strategy','merge'):<14} "
        f"{t.get('load_type','incremental'):<14} "
        f"{full_source:<40} {full_target:<40} {wm:<20}"
    )

if DRY_RUN:
    print("\n  [DRY RUN] No transforms executed.")
    dbutils.notebook.exit("DRY_RUN")

# COMMAND ----------

#  Execute all transforms via run_all() which calls validate_all() first,
#  then runs every stage in order using the same code path as notebooks/stages/.

results = engine.run_all(
    filter_ids    = FILTER_IDS,
    stop_on_error = STOP_ON_ERROR,
)

# COMMAND ----------

#  Publish outcome to task values (for downstream audit/monitoring)

total      = len(results)
success    = sum(1 for r in results if r.status == "SUCCESS")
skipped    = sum(1 for r in results if r.status == "SKIPPED")
failed     = sum(1 for r in results if r.status == "FAILED")
total_rows = sum(r.rows_loaded for r in results if r.status == "SUCCESS")

dbutils.jobs.taskValues.set(key="bulk_transform_total",   value=total)
dbutils.jobs.taskValues.set(key="bulk_transform_success", value=success)
dbutils.jobs.taskValues.set(key="bulk_transform_skipped", value=skipped)
dbutils.jobs.taskValues.set(key="bulk_transform_failed",  value=failed)
dbutils.jobs.taskValues.set(key="bulk_transform_rows",    value=total_rows)

print(
    f"[BULK TRANSFORMS] Complete — {success}/{total} succeeded  "
    f"{skipped} skipped  {total_rows:,} rows in Silver"
)

if failed > 0 and STOP_ON_ERROR:
    raise RuntimeError(
        f"[BULK TRANSFORMS] {failed} transform(s) failed. "
        "Check logs above for details."
    )
