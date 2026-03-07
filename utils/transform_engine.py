# utils/transform_engine.py

# Unified transformation layer — two engines in one module:
#
# TransformationEngine  — YAML-driven chain (snake_case → rename → SQL chain
#                         → column_transforms → derived_columns → drop_columns)
#                         Used by the single-table staged notebooks (01_transform).
#
# BulkTransformEngine   — CSV-driven full pipeline (read Bronze → incremental
#                         filter → SQL → Silver write → DQ → Governance →
#                         Optimize → Audit → watermark).
#                         Reads bulk_transforms.csv as the single config source
#                         replacing pipeline_config.yml + transformation.yml.

import csv
import logging
import os
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from utils.logger import AUDIT_SCHEMA, DEFAULT_AUDIT_TABLE, MIGRATION_LOG_SCHEMA, PipelineLogger

import yaml
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# TransformationEngine — YAML-driven chain

_DTYPE_MAP = {
    "STRING": StringType(),
    "VARCHAR": StringType(),
    "INT": IntegerType(),
    "INTEGER": IntegerType(),
    "LONG": LongType(),
    "BIGINT": LongType(),
    "DOUBLE": DoubleType(),
    "FLOAT": FloatType(),
    "BOOLEAN": BooleanType(),
    "BOOL": BooleanType(),
    "DATE": DateType(),
    "TIMESTAMP": TimestampType(),
}


def to_snake_case(name: str) -> str:
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    name = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", name)
    name = re.sub(r"[\s\-\.]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_").lower()


def apply_snake_case(df: DataFrame) -> DataFrame:
    renamed = {}
    for col in df.columns:
        snake = to_snake_case(col)
        if snake != col:
            df = df.withColumnRenamed(col, snake)
            renamed[col] = snake
    if renamed:
        logger.info("snake_case: renamed %d columns", len(renamed))
    return df


def apply_rename_columns(df: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
        else:
            logger.warning("rename_columns: '%s' not found — skipping", old)
    return df


def apply_single_column_transform(df: DataFrame, t: Dict[str, Any]) -> DataFrame:
    col = t.get("column")
    act = t.get("action", "").lower()
    p = t.get("params", {})

    if not col or not act:
        return df
    if col not in df.columns:
        logger.warning("column_transform: '%s' not in DataFrame — skipping '%s'", col, act)
        return df

    try:
        if act == "cast":
            key = p.get("dtype", "STRING").upper()
            dtype = _DTYPE_MAP.get(key)
            if dtype is None:
                m = re.match(r"DECIMAL\((\d+),\s*(\d+)\)", key)
                dtype = DecimalType(int(m.group(1)), int(m.group(2))) if m else None
            if dtype:
                df = df.withColumn(col, F.col(col).cast(dtype))
        elif act == "trim":
            df = df.withColumn(col, F.trim(F.col(col)))
        elif act == "upper":
            df = df.withColumn(col, F.upper(F.col(col)))
        elif act == "lower":
            df = df.withColumn(col, F.lower(F.col(col)))
        elif act == "date_format":
            df = df.withColumn(
                col,
                F.date_format(
                    F.to_date(F.col(col), p.get("input_format", "yyyy-MM-dd")),
                    p.get("output_format", "yyyy-MM-dd"),
                ),
            )
        elif act in ("coalesce", "fill_null"):
            df = df.withColumn(col, F.coalesce(F.col(col), F.lit(p.get("default_value"))))
        elif act == "regex_replace":
            df = df.withColumn(
                col,
                F.regexp_replace(F.col(col), p.get("pattern", ""), p.get("replacement", "")),
            )
        else:
            logger.warning("Unknown column_transform action '%s' — skipping", act)
    except Exception as exc:
        logger.exception("column_transform '%s' on '%s': %s", act, col, exc)
    return df


def apply_derived_columns(df: DataFrame, derived: List[Dict]) -> DataFrame:
    for entry in derived:
        name = entry.get("name")
        expr = entry.get("expression")
        if name and expr:
            try:
                df = df.withColumn(name, F.expr(expr))
            except Exception as exc:
                logger.exception("derived_column '%s' expr='%s': %s", name, expr, exc)
    return df


def apply_drop_columns(df: DataFrame, drops: List[str]) -> DataFrame:
    existing = [c for c in drops if c in df.columns]
    if existing:
        df = df.drop(*existing)
    return df


class TransformationEngine:
    """
    Runs the YAML-driven transformation chain from transformation.yml.

    Steps: snake_case → rename → SQL chain → column_transforms
           → derived_columns → drop_columns

    Usage (single source):
        engine = TransformationEngine(spark, cfg.transform)
        silver_df = engine.run(bronze_df)

    Usage (multi-source join):
        silver_df = engine.run(primary_df, extra_views={"customers": df1, "branches": df2})
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config

    def run(
        self,
        df: DataFrame,
        extra_views: Optional[Dict[str, DataFrame]] = None,
    ) -> DataFrame:
        logger.info("Starting transformation pipeline")
        initial_cols = len(df.columns)

        if extra_views:
            for alias, view_df in extra_views.items():
                view_df.createOrReplaceTempView(alias)
                logger.info("Registered extra view: '%s'", alias)

        if self.config.get("snake_case_columns", False):
            logger.info("Step 1: snake_case all columns")
            df = apply_snake_case(df)

        rename_map = self.config.get("rename_columns", {})
        if rename_map:
            logger.info("Step 2: %d explicit renames", len(rename_map))
            df = apply_rename_columns(df, rename_map)

        sql_transforms = self.config.get("sql_transforms", [])
        if sql_transforms:
            logger.info("Step 3: %d SQL transform(s)", len(sql_transforms))
            df = self._run_sql_chain(df, sql_transforms)
        else:
            logger.info("Step 3: No sql_transforms — skipping")

        col_transforms = self.config.get("column_transforms", [])
        if col_transforms:
            logger.info("Step 4: %d column transform(s)", len(col_transforms))
            for t in col_transforms:
                df = apply_single_column_transform(df, t)

        derived = self.config.get("derived_columns", [])
        if derived:
            logger.info("Step 5: %d derived column(s)", len(derived))
            df = apply_derived_columns(df, derived)

        drops = self.config.get("drop_columns", [])
        if drops:
            logger.info("Step 6: dropping %d column(s)", len(drops))
            df = apply_drop_columns(df, drops)

        logger.info("Transformation complete: %d → %d columns", initial_cols, len(df.columns))
        return df

    def _run_sql_chain(self, df: DataFrame, steps: List[Dict]) -> DataFrame:
        view = "silver_transform_source"
        df.createOrReplaceTempView(view)

        for i, step in enumerate(steps, 1):
            name = step.get("name", f"step_{i}")
            desc = step.get("description", "")
            sql = step.get("sql", "").strip()

            if not sql:
                logger.warning("SQL transform '%s' has no SQL — skipping", name)
                continue

            rendered = sql.replace("{source}", view)
            logger.info("SQL [%d/%d] '%s': %s", i, len(steps), name, desc)

            try:
                df = self.spark.sql(rendered)
            except Exception as exc:
                raise RuntimeError(
                    f"SQL transform '{name}' failed.\nRendered SQL:\n{rendered}\nError: {exc}"
                ) from exc

            view = f"silver_transform_{i}_{name}"
            df.createOrReplaceTempView(view)
            logger.info("  → %d rows, %d cols after '%s'", df.count(), len(df.columns), name)

        return df


# BulkTransformEngine — CSV-driven unified pipeline


class BulkTransformResult:
    """Holds the outcome of a single transform execution."""

    __slots__ = ("id", "status", "rows_loaded", "error_message", "duration_seconds")

    def __init__(
        self,
        transform_id: str,
        status: str,
        rows_loaded: int = 0,
        error_message: str = "",
        duration_seconds: float = 0.0,
    ):
        self.id = transform_id
        self.status = status
        self.rows_loaded = rows_loaded
        self.error_message = error_message
        self.duration_seconds = duration_seconds

    def __repr__(self) -> str:
        return (
            f"BulkTransformResult(id={self.id!r}, status={self.status!r}, "
            f"rows={self.rows_loaded:,}, secs={self.duration_seconds:.1f})"
        )


def _safe_create_catalog(spark, catalog: str, *_args, **_kwargs) -> None:
    """
    Verify that a catalog exists — never attempts to create it.
    Catalog creation is admin/DBA responsibility (Databricks UI or Terraform).
    Raises RuntimeError if the catalog is not found.
    """
    existing = {row[0].lower() for row in spark.sql("SHOW CATALOGS").collect()}
    if catalog.lower() not in existing:
        raise RuntimeError(
            f"[INFRA] Catalog '{catalog}' does not exist. "
            f"Please create it in the Databricks UI (Data → Create Catalog) and re-run the pipeline."
        )


class BulkTransformEngine:
    """
    CSV-driven unified pipeline — replaces pipeline_config.yml + transformation.yml.

    Reads silver_config.csv (written as a Delta table by the validate stage) and
    runs the full Silver pipeline for every enabled row:
    Bronze read → incremental filter → SQL → Silver write
    → DQ → Governance → Optimize/Vacuum → Audit → watermark update.

    Stage flags (per CSV row, default shown):
      run_data_quality      false  opt-in  (needs dq_config_path)
      run_governance        false  opt-in  (needs governance_config_path)
      run_audit             true   opt-out
      run_optimize          true   opt-out

    Multi-value CSV fields use "|" as the separator (pk_columns, zorder_columns).
    SQL in the "query" column references Spark temp view "source" (the Bronze DF).
    Complex SQL can live in a .sql file referenced via the sql_file column.

    Infrastructure locations (control table, config table, audit table, etc.) are
    read automatically from parameter.yml — callers do not need to pass them.
    """

    _CONTROL_SCHEMA = (
        "id               STRING  NOT NULL, "
        "last_load_time   TIMESTAMP, "
        "last_run_status  STRING, "
        "last_run_ts      TIMESTAMP, "
        "rows_loaded      LONG, "
        "error_message    STRING"
    )

    # Audit schema and default table are defined in utils.logger (single source
    # of truth).  All audit + log records go to the `logging` schema.
    _AUDIT_SCHEMA = AUDIT_SCHEMA
    _DEFAULT_AUDIT_TABLE = DEFAULT_AUDIT_TABLE

    def __init__(
        self,
        spark: SparkSession,
        config_base_path: str,
        csv_path: str = "config/silver_config.csv",
        parameter_yml: str = "config/parameter.yml",
    ):
        self.spark = spark
        self.config_base_path = config_base_path
        self.csv_path = csv_path
        self.parameter_yml = parameter_yml

        #  Load parameter.yml once; all methods share self._params
        self._params: Dict[str, Any] = self._load_parameter_yml()

        #  Derive watermark/control table from logging catalog + schema
        #  (watermark state lives in the single `logging` schema, no separate schema needed)
        self.control_table = "cdl_silver.logging.watermark_state"
        if self._params:
            log_cat = self._params.get("logging_catalog", "")
            log_sch = self._params.get("logging_schema", "")
            wm_tbl  = self._params.get("watermark_table", "watermark_state")
            if log_cat and log_sch:
                self.control_table = f"{log_cat}.{log_sch}.{wm_tbl}"

        #  Derive config Delta table location from parameter.yml ─
        self.config_table: str = ""
        if self._params:
            cfg_cat = self._params.get("config_catalog", "")
            cfg_sch = self._params.get("config_schema", "")
            cfg_tbl = self._params.get("config_table", "")
            if cfg_cat and cfg_sch and cfg_tbl:
                self.config_table = f"{cfg_cat}.{cfg_sch}.{cfg_tbl}"

        #  Derive migration log table from parameter.yml ─
        self.migration_log_table: str = ""
        if self._params:
            ml_cat = self._params.get("logging_catalog", "")
            ml_sch = self._params.get("logging_schema", "")
            ml_tbl = self._params.get("log_table", "")
            if ml_cat and ml_sch and ml_tbl:
                self.migration_log_table = f"{ml_cat}.{ml_sch}.{ml_tbl}"

        self._ensure_control_table()

    #  Public API 

    def load_transforms(
        self,
        filter_ids: Optional[List[str]] = None,
        include_disabled: bool = False,
    ) -> List[Dict[str, Any]]:
        """Load and parse bulk_transforms.csv, normalising all fields."""
        full_path = os.path.join(self.config_base_path, self.csv_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"[BulkTransform] CSV not found: {full_path}")

        rows: List[Dict[str, Any]] = []
        with open(full_path, encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for raw in reader:
                row = {k.strip(): (v.strip() if v else "") for k, v in raw.items()}

                row["enabled"] = row.get("enabled", "true").lower() != "false"
                if not row["enabled"] and not include_disabled:
                    continue
                if filter_ids and row["id"] not in filter_ids:
                    continue

                for flag in ("run_data_quality", "run_governance"):
                    row[flag] = row.get(flag, "").lower() == "true"
                for flag in ("run_audit", "run_optimize"):
                    row[flag] = row.get(flag, "").lower() != "false"

                row["pk_columns"] = _split_pipe(row.get("pk_columns", ""))
                row["zorder_columns"] = _split_pipe(row.get("zorder_columns", ""))

                vh = row.get("vacuum_retain_hours", "")
                row["vacuum_retain_hours"] = int(vh) if vh else 168

                lt = row.get("load_type", "").strip().lower()
                row["load_type"] = lt if lt in ("incremental", "full_refresh") else "incremental"

                rows.append(row)

        logger.info("[BulkTransform] Loaded %d enabled transform(s) from %s", len(rows), full_path)
        return rows

    def load_transforms_from_table(
        self,
        filter_ids: Optional[List[str]] = None,
        include_disabled: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Load and parse transforms from the config Delta table written by validate_all().
        Falls back to load_transforms() (CSV) when the table is unavailable.

        The Delta table is located at self.config_table (derived from parameter.yml).
        """
        if not self.config_table:
            logger.warning(
                "[BulkTransform] config_table not set in parameter.yml — falling back to CSV"
            )
            return self.load_transforms(filter_ids=filter_ids, include_disabled=include_disabled)

        try:
            df = self.spark.table(self.config_table)
        except Exception as exc:
            logger.warning(
                "[BulkTransform] Cannot read config table %s (%s) — falling back to CSV",
                self.config_table, exc,
            )
            return self.load_transforms(filter_ids=filter_ids, include_disabled=include_disabled)

        result: List[Dict[str, Any]] = []
        for raw in df.collect():
            row: Dict[str, Any] = {
                k: (v.strip() if isinstance(v, str) else ("" if v is None else v))
                for k, v in raw.asDict().items()
            }

            row["enabled"] = str(row.get("enabled", "true")).lower() not in ("false", "0")
            if not row["enabled"] and not include_disabled:
                continue
            if filter_ids and row.get("id") not in filter_ids:
                continue

            for flag in ("run_data_quality", "run_governance"):
                row[flag] = str(row.get(flag, "")).lower() == "true"
            for flag in ("run_audit", "run_optimize"):
                row[flag] = str(row.get(flag, "")).lower() not in ("false", "0")

            row["pk_columns"]     = _split_pipe(str(row.get("pk_columns", "")))
            row["zorder_columns"] = _split_pipe(str(row.get("zorder_columns", "")))

            vh = row.get("vacuum_retain_hours", "")
            row["vacuum_retain_hours"] = int(vh) if vh else 168

            lt = str(row.get("load_type", "")).strip().lower()
            row["load_type"] = lt if lt in ("incremental", "full_refresh") else "incremental"

            result.append(row)

        logger.info(
            "[BulkTransform] Loaded %d enabled transform(s) from config table %s",
            len(result), self.config_table,
        )
        return result

    def mark_processed(self, filter_ids: Optional[List[str]] = None) -> None:
        """
        Update is_processed = 'True' in the config Delta table for all rows
        that were part of this pipeline run.

        If filter_ids is provided, only those IDs are updated; otherwise every
        enabled row is marked processed.
        """
        if not self.config_table:
            logger.warning(
                "[BulkTransform] config_table not set — cannot mark is_processed"
            )
            return

        if filter_ids:
            escaped = "', '".join(filter_ids)
            where_clause = f"id IN ('{escaped}')"
        else:
            where_clause = "enabled NOT IN ('false', 'False', '0')"

        try:
            self.spark.sql(
                f"UPDATE {self.config_table} SET is_processed = 'True' WHERE {where_clause}"
            )
            target = ", ".join(filter_ids) if filter_ids else "all enabled rows"
            logger.info("[BulkTransform] Marked is_processed=True for: %s", target)
            print(f"[AUDIT]  Marked is_processed=True in {self.config_table} for: {target}")
        except Exception as exc:
            logger.error("[BulkTransform] Failed to mark is_processed: %s", exc, exc_info=True)
            print(f"[AUDIT] ✗ Could not update is_processed: {exc}")

    def run_all(
        self,
        filter_ids: Optional[List[str]] = None,
        stop_on_error: bool = False,
    ) -> List[BulkTransformResult]:
        """Execute all enabled transforms (optionally filtered by ID list)."""
        transforms = self.load_transforms(filter_ids=filter_ids)
        if not transforms:
            logger.warning("[BulkTransform] No enabled transforms found to run.")
            return []

        results: List[BulkTransformResult] = []
        total = len(transforms)

        logger.info("=" * 65)
        logger.info("  BulkTransformEngine — starting %d transform(s)", total)
        logger.info("  load_type: per-row (incremental or full_refresh)")
        logger.info("=" * 65)

        for idx, transform in enumerate(transforms, start=1):
            tid = transform["id"]
            logger.info(
                "[%d/%d] Starting id=%r  desc=%r",
                idx, total, tid, transform.get("description", ""),
            )
            try:
                result = self.run_one(transform)
            except Exception as exc:
                logger.error("[%d/%d] FAILED id=%r : %s", idx, total, tid, exc, exc_info=True)
                result = BulkTransformResult(
                    transform_id=tid, status="FAILED", error_message=str(exc),
                )
                if stop_on_error:
                    results.append(result)
                    raise
            results.append(result)

        self._print_summary(results)
        return results

    def run_one(self, transform: Dict[str, Any]) -> BulkTransformResult:
        """
        Full pipeline for one CSV row.

        1.  Resolve fields + generate run_id
        2.  Ensure Silver catalog/schema
        3.  Bootstrap control-table row
        4.  Read Bronze source → temp view "source"
        5.  Incremental filter (watermark_column, incremental mode)
        6.  Execute SQL → transformed DataFrame
        7.  Write to Silver (merge / append / full_refresh)
        8.  Data Quality   if run_data_quality=true + dq_config_path set
        9.  Governance     if run_governance=true  + governance_config_path set
        10. OPTIMIZE+VACUUM if run_optimize=true
        11. Update watermark
        12. Audit log      if run_audit=true
        13. Update control table
        """
        t_start = datetime.utcnow()
        run_id = str(uuid.uuid4())
        tid = transform["id"]

        source_catalog = transform.get("source_catalog", "")
        source_schema  = transform.get("source_schema", "")
        source_table   = transform.get("source_table", "")
        target_catalog = transform["target_catalog"]
        target_schema  = transform["target_schema"]
        target_table   = transform["target_table"]
        strategy       = transform.get("load_strategy", "merge").lower()
        pk_cols        = transform.get("pk_columns", [])
        watermark_col  = transform.get("watermark_column", "")
        zorder_cols    = transform.get("zorder_columns", [])
        vacuum_hours   = transform.get("vacuum_retain_hours", 168)

        full_source = (
            f"{source_catalog}.{source_schema}.{source_table}"
            if source_catalog and source_schema and source_table else ""
        )
        full_target = f"{target_catalog}.{target_schema}.{target_table}"

        source_count = 0
        rows_loaded  = 0
        dq_summary: Dict[str, Any] = {}

        logger.info(
            "  source=%s  target=%s  strategy=%s  wm_col=%r",
            full_source or "(none)", full_target, strategy, watermark_col or "(none)",
        )
        logger.info(
            "  stages → DQ=%s  Gov=%s  Audit=%s  Optimize=%s",
            transform.get("run_data_quality"), transform.get("run_governance"),
            transform.get("run_audit"), transform.get("run_optimize"),
        )

        try:
            if strategy in ("merge", "scd2") and not pk_cols:
                raise ValueError(f"[{tid}] load_strategy='{strategy}' requires pk_columns.")

            self._ensure_catalog_schema(target_catalog, target_schema)
            self._bootstrap_control_row(tid, transform.get("last_load_time", ""))

            if full_source:
                source_df = self.spark.table(full_source)
                source_count = source_df.count()
                logger.info("  Source rows: %s", f"{source_count:,}")
            else:
                source_df = None
                logger.info("  No source_table — SQL must reference tables directly.")

            load_type = transform.get("load_type", "incremental")
            if source_df is not None and watermark_col and load_type == "incremental":
                last_wm = self._get_last_watermark(tid)
                if last_wm is not None:
                    before = source_df.count()
                    source_df = source_df.filter(F.col(watermark_col) > F.lit(last_wm))
                    logger.info(
                        "  Incremental filter %r > %s : %s → %s rows",
                        watermark_col, last_wm, f"{before:,}", f"{source_df.count():,}",
                    )
                else:
                    logger.info("  No previous watermark — initial full load for id=%r", tid)

            if source_df is not None:
                source_df.createOrReplaceTempView("source")

            result_df = self.spark.sql(self._get_query(transform))
            transform_count = result_df.count()
            logger.info("  Transformed rows: %s", f"{transform_count:,}")

            if transform_count == 0:
                logger.info("  0 rows — skipping write for id=%r", tid)
                rows_loaded = (
                    self.spark.table(full_target).count()
                    if self.spark.catalog.tableExists(full_target) else 0
                )
                self._update_control(tid, "SUCCESS", rows_loaded, "")
                if transform.get("run_audit", True):
                    self._write_audit_log(
                        transform, run_id, full_target,
                        source_count, rows_loaded, dq_summary, t_start, "SUCCESS",
                    )
                self._write_migration_log(
                    transform, run_id, full_source, full_target,
                    rows_loaded, t_start, "SUCCESS", "transform", "0 rows transformed — skipped write",
                )
                duration = (datetime.utcnow() - t_start).total_seconds()
                return BulkTransformResult(
                    transform_id=tid, status="SUCCESS",
                    rows_loaded=rows_loaded, duration_seconds=duration,
                )

            self._write(result_df, full_target, strategy, pk_cols)
            rows_loaded = self.spark.table(full_target).count()
            logger.info("  Silver rows after write: %s", f"{rows_loaded:,}")

            dq_config_path = transform.get("dq_config_path", "").strip()
            if transform.get("run_data_quality") and dq_config_path:
                dq_summary = self._run_data_quality(tid, full_target, dq_config_path, run_id)

            gov_config_path = transform.get("governance_config_path", "").strip()
            if transform.get("run_governance") and gov_config_path:
                self._run_governance(transform, gov_config_path)

            if transform.get("run_optimize", True):
                self._optimize_and_vacuum(full_target, zorder_cols, vacuum_hours)

            if watermark_col and watermark_col in result_df.columns:
                new_wm = result_df.agg(F.max(watermark_col)).first()[0]
                if new_wm is not None:
                    self._update_watermark(tid, new_wm)
                    logger.info("  Watermark updated → %s", new_wm)

            if transform.get("run_audit", True):
                self._write_audit_log(
                    transform, run_id, full_target,
                    source_count, rows_loaded, dq_summary, t_start, "SUCCESS",
                )

            self._update_control(tid, "SUCCESS", rows_loaded, "")
            self._write_migration_log(
                transform, run_id, full_source, full_target,
                rows_loaded, t_start, "SUCCESS", "transform",
            )

            duration = (datetime.utcnow() - t_start).total_seconds()
            logger.info("  [%s]  Complete — %s rows  (%.1fs)", tid, f"{rows_loaded:,}", duration)
            return BulkTransformResult(
                transform_id=tid, status="SUCCESS",
                rows_loaded=rows_loaded, duration_seconds=duration,
            )

        except Exception as exc:
            err_msg = str(exc)[:2000]
            if transform.get("run_audit", True):
                try:
                    self._write_audit_log(
                        transform, run_id, full_target,
                        source_count, rows_loaded, dq_summary, t_start, "FAILED", err_msg,
                    )
                except Exception:
                    pass
            try:
                self._update_control(tid, "FAILED", rows_loaded, err_msg)
            except Exception:
                pass
            try:
                self._write_migration_log(
                    transform, run_id, full_source, full_target,
                    rows_loaded, t_start, "FAILED", "transform", err_msg,
                )
            except Exception:
                pass
            raise

    #  Stage-specific public API (used by 6-stage pipeline notebooks) 

    def validate_all(self, filter_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Stage 0: Validate all enabled CSV rows.

        Steps:
          1. Read parameter.yml and ensure all infrastructure catalogs/schemas/tables.
          2. Write silver_config.csv as a queryable Delta table in Unity Catalog.
          3. Ensure each row's target catalog/schema exists.
          4. Verify every source table is accessible.

        Raises AssertionError listing all inaccessible sources.

        Returns dict:
          {
            "table_results": [{id, source_ok, source_count}, ...],
            "any_run_dq":         bool,
            "any_run_governance":  bool,
            "any_run_audit":       bool,
            "any_run_optimize":    bool,
          }
        """
        #  Step 1: infrastructure setup from parameter.yml 
        params = self._load_parameter_yml()
        if params:
            self._ensure_infrastructure(params)
            self._load_config_as_table(params)

        transforms = self.load_transforms(filter_ids=filter_ids)
        if not transforms:
            logger.warning("[VALIDATE] No enabled transforms found.")
            return {
                "table_results": [],
                "any_run_dq": False, "any_run_governance": False,
                "any_run_audit": False, "any_run_optimize": False,
            }

        print(f"\n{'='*65}")
        print(f"  [VALIDATE] Bulk Silver Pipeline — Pre-flight Check")
        print(f"  tables   : {len(transforms)}")
        print(f"{'='*65}\n")

        issues: List[str] = []
        results: List[Dict[str, Any]] = []

        for t in transforms:
            tid = t["id"]
            sc  = t.get("source_catalog", "")
            ss  = t.get("source_schema", "")
            st  = t.get("source_table", "")

            self._ensure_catalog_schema(t["target_catalog"], t["target_schema"])

            source_ok = True
            source_count = -1
            if sc and ss and st:
                full_source = f"{sc}.{ss}.{st}"
                try:
                    source_count = self.spark.table(full_source).count()
                    print(f"[VALIDATE]  [{tid}] source '{full_source}' — {source_count:,} rows")
                except Exception as e:
                    msg = f"[{tid}] Cannot access '{full_source}': {e}"
                    issues.append(msg)
                    source_ok = False
                    print(f"[VALIDATE] ✗ {msg}")
            else:
                print(f"[VALIDATE] ○ [{tid}] No source_table — SQL references tables directly")

            results.append({"id": tid, "source_ok": source_ok, "source_count": source_count})

        print(f"\n{'─'*65}")
        if issues:
            raise AssertionError(
                f"[VALIDATE] {len(issues)} source(s) not accessible:\n" + "\n".join(issues)
            )
        print(f"[VALIDATE]  All {len(transforms)} table(s) validated successfully")

        return {
            "table_results":      results,
            "any_run_dq":         any(t.get("run_data_quality", False) for t in transforms),
            "any_run_governance":  any(t.get("run_governance", False) for t in transforms),
            "any_run_audit":       any(t.get("run_audit", True) for t in transforms),
            "any_run_optimize":    any(t.get("run_optimize", True) for t in transforms),
        }

    def transform_all(
        self,
        filter_ids: Optional[List[str]] = None,
        stop_on_error: bool = False,
        run_id: str = "",
    ) -> List["BulkTransformResult"]:
        """
        Stage 1: Transform + write for all enabled rows.
        Does NOT run DQ, governance, optimize, or audit — those are separate stages.
        Returns list of BulkTransformResult.
        """
        transforms = self.load_transforms_from_table(filter_ids=filter_ids)
        if not transforms:
            logger.warning("[TRANSFORM] No enabled transforms found.")
            return []

        run_id = run_id or str(uuid.uuid4())
        results: List[BulkTransformResult] = []
        total = len(transforms)

        print(f"\n{'='*65}")
        print(f"  [TRANSFORM] Running {total} transform(s)  run_id={run_id}")
        print(f"{'='*65}")

        for idx, t in enumerate(transforms, 1):
            tid = t["id"]
            print(f"\n[{idx}/{total}] Starting id={tid!r}  desc={t.get('description', '')!r}")
            try:
                result = self._run_transform_step(t, run_id=run_id)
                print(f"   {tid} — {result.rows_loaded:,} rows  ({result.duration_seconds:.1f}s)")
            except Exception as exc:
                logger.error("[TRANSFORM] FAILED id=%r: %s", tid, exc, exc_info=True)
                result = BulkTransformResult(
                    transform_id=tid, status="FAILED", error_message=str(exc)
                )
                print(f"  ✗ {tid} — FAILED: {str(exc)[:200]}")
                if stop_on_error:
                    results.append(result)
                    raise
            results.append(result)

        self._print_summary(results)
        return results

    def run_dq_all(self, filter_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Stage 2: Run DQ scan for all rows with run_data_quality=true.
        Returns aggregate stats: {pass_rows, fail_rows, tables_checked}.
        """
        transforms = self.load_transforms_from_table(filter_ids=filter_ids)
        total_pass = 0
        total_fail = 0
        applied = 0
        run_id = str(uuid.uuid4())

        print(f"\n{'='*65}")
        print(f"  [DQ] Running data quality checks")
        print(f"{'='*65}")

        for t in transforms:
            if not t.get("run_data_quality") or not t.get("dq_config_path", "").strip():
                print(f"[DQ] [{t['id']}] Skipped (run_data_quality=false or no dq_config_path)")
                continue

            full_target = f"{t['target_catalog']}.{t['target_schema']}.{t['target_table']}"
            try:
                summary = self._run_data_quality(
                    t["id"], full_target, t["dq_config_path"], run_id
                )
                total_pass += summary.get("pass_rows", 0)
                total_fail += summary.get("fail_rows", 0)
                applied += 1
                print(
                    f"[DQ] [{t['id']}]  pass={summary.get('pass_rows', 0):,}  "
                    f"fail={summary.get('fail_rows', 0):,}  "
                    f"status={summary.get('status', '?')}"
                )
            except Exception as e:
                logger.error("[DQ] [%s] Failed: %s", t["id"], e, exc_info=True)
                print(f"[DQ] [{t['id']}] ✗ Failed: {e}")

        print(
            f"\n[DQ]  Complete — {applied} table(s) scanned  "
            f"pass={total_pass:,}  fail={total_fail:,}"
        )
        return {"pass_rows": total_pass, "fail_rows": total_fail, "tables_checked": applied}

    def run_governance_all(self, filter_ids: Optional[List[str]] = None) -> int:
        """
        Stage 3: Apply governance for all rows with run_governance=true.
        Returns count of tables processed.
        """
        transforms = self.load_transforms_from_table(filter_ids=filter_ids)
        applied = 0

        print(f"\n{'='*65}")
        print(f"  [GOVERNANCE] Applying Unity Catalog governance")
        print(f"{'='*65}")

        for t in transforms:
            if not t.get("run_governance") or not t.get("governance_config_path", "").strip():
                print(f"[GOVERNANCE] [{t['id']}] Skipped (run_governance=false or no config)")
                continue
            try:
                self._run_governance(t, t["governance_config_path"])
                applied += 1
                print(f"[GOVERNANCE] [{t['id']}]  Governance applied")
            except Exception as e:
                logger.error("[GOVERNANCE] [%s] Failed: %s", t["id"], e, exc_info=True)
                print(f"[GOVERNANCE] [{t['id']}] ✗ Failed: {e}")

        print(f"\n[GOVERNANCE]  Complete — {applied} table(s) processed")
        return applied

    def run_audit_all(
        self,
        filter_ids: Optional[List[str]] = None,
        run_id: str = "",
        start_time_epoch: int = 0,
    ) -> None:
        """
        Stage 4: Write audit records for all rows with run_audit=true.
        Reads run status and rows_loaded from the control table.
        """
        transforms = self.load_transforms_from_table(filter_ids=filter_ids)
        t_start = (
            datetime.utcfromtimestamp(start_time_epoch)
            if start_time_epoch
            else datetime.utcnow()
        )
        if not run_id:
            run_id = str(uuid.uuid4())

        try:
            ctrl_map = {
                row["id"]: row
                for row in self.spark.table(self.control_table).collect()
            }
        except Exception:
            ctrl_map = {}

        written = 0
        print(f"\n{'='*65}")
        print(f"  [AUDIT] Writing audit records  run_id={run_id}")
        print(f"{'='*65}")

        for t in transforms:
            if not t.get("run_audit", True):
                print(f"[AUDIT] [{t['id']}] Skipped (run_audit=false)")
                continue

            tid = t["id"]
            full_target = f"{t['target_catalog']}.{t['target_schema']}.{t['target_table']}"
            ctrl = ctrl_map.get(tid)
            status      = (ctrl["last_run_status"] or "UNKNOWN") if ctrl else "UNKNOWN"
            rows_loaded = int(ctrl["rows_loaded"] or 0) if ctrl else 0
            err_msg     = (ctrl["error_message"] or "") if ctrl else ""

            try:
                self._write_audit_log(
                    t, run_id, full_target,
                    source_count=0,
                    rows_loaded=rows_loaded,
                    dq_summary={},
                    t_start=t_start,
                    status=status,
                    error_message=err_msg,
                )
                written += 1
                print(f"[AUDIT] [{tid}]  Audit written — status={status}  rows={rows_loaded:,}")
            except Exception as e:
                logger.error("[AUDIT] [%s] Failed: %s", tid, e, exc_info=True)
                print(f"[AUDIT] [{tid}] ✗ Failed: {e}")

        print(f"\n[AUDIT]  Complete — {written} audit record(s) written")

    def run_optimize_all(self, filter_ids: Optional[List[str]] = None) -> int:
        """
        Stage 5: OPTIMIZE + VACUUM for all rows with run_optimize=true.
        Returns count of tables optimized.
        """
        transforms = self.load_transforms_from_table(filter_ids=filter_ids)
        applied = 0

        print(f"\n{'='*65}")
        print(f"  [OPTIMIZE] Running OPTIMIZE + VACUUM")
        print(f"{'='*65}")

        for t in transforms:
            if not t.get("run_optimize", True):
                print(f"[OPTIMIZE] [{t['id']}] Skipped (run_optimize=false)")
                continue

            full_target = f"{t['target_catalog']}.{t['target_schema']}.{t['target_table']}"
            zorder_cols  = t.get("zorder_columns", [])
            vacuum_hours = t.get("vacuum_retain_hours", 168)

            try:
                self._optimize_and_vacuum(full_target, zorder_cols, vacuum_hours)
                applied += 1
                print(f"[OPTIMIZE] [{t['id']}]  Optimized {full_target}")
            except Exception as e:
                logger.error("[OPTIMIZE] [%s] Failed: %s", t["id"], e, exc_info=True)
                print(f"[OPTIMIZE] [{t['id']}] ✗ Failed: {e}")

        print(f"\n[OPTIMIZE]  Complete — {applied} table(s) optimized")
        return applied

    def _run_transform_step(
        self, transform: Dict[str, Any], run_id: str = ""
    ) -> "BulkTransformResult":
        """
        Transform-only for one CSV row:
          read Bronze → incremental filter → SQL → write Silver
          → update watermark → update control table → write migration log.
        Does NOT run DQ, governance, optimize, or audit.

        Skips the table entirely when is_processed=true in the config table.
        """
        t_start = datetime.utcnow()
        tid = transform["id"]
        run_id = run_id or str(uuid.uuid4())

        # ------------------------------------------------------------------
        # is_processed guard — skip already-processed tables
        # ------------------------------------------------------------------
        if str(transform.get("is_processed", "false")).lower() == "true":
            print(
                f"\n[TRANSFORM] [{tid}] SKIPPED — table already processed "
                f"(is_processed=true). Reset is_processed to false in the "
                f"config table to re-run."
            )
            self._write_migration_log(
                transform, run_id,
                full_source="",
                full_target=f"{transform.get('target_catalog','')}.{transform.get('target_schema','')}.{transform.get('target_table','')}",
                rows_loaded=0,
                t_start=t_start,
                status="SKIPPED",
                stage="transform",
                message="is_processed=true — skipped",
            )
            return BulkTransformResult(
                transform_id=tid,
                status="SKIPPED",
                error_message="is_processed=true — skipped",
            )

        sc  = transform.get("source_catalog", "")
        ss  = transform.get("source_schema", "")
        st  = transform.get("source_table", "")
        tc  = transform["target_catalog"]
        ts  = transform["target_schema"]
        tt  = transform["target_table"]
        strategy      = transform.get("load_strategy", "merge").lower()
        pk_cols       = transform.get("pk_columns", [])
        watermark_col = transform.get("watermark_column", "")

        full_source = f"{sc}.{ss}.{st}" if sc and ss and st else ""
        full_target = f"{tc}.{ts}.{tt}"
        rows_loaded = 0

        try:
            if strategy in ("merge", "scd2") and not pk_cols:
                raise ValueError(f"[{tid}] load_strategy='{strategy}' requires pk_columns.")

            self._ensure_catalog_schema(tc, ts)
            self._bootstrap_control_row(tid, transform.get("last_load_time", ""))

            source_df = None
            if full_source:
                source_df = self.spark.table(full_source)
                logger.info("  [%s] Source rows: %s", tid, f"{source_df.count():,}")
            else:
                logger.info("  [%s] No source_table — SQL references tables directly.", tid)

            load_type = transform.get("load_type", "incremental")
            if source_df is not None and watermark_col and load_type == "incremental":
                last_wm = self._get_last_watermark(tid)
                if last_wm is not None:
                    before = source_df.count()
                    source_df = source_df.filter(F.col(watermark_col) > F.lit(last_wm))
                    logger.info(
                        "  [%s] Incremental filter %r > %s: %s → %s rows",
                        tid, watermark_col, last_wm,
                        f"{before:,}", f"{source_df.count():,}",
                    )
                else:
                    logger.info("  [%s] No previous watermark — full initial load.", tid)

            if source_df is not None:
                source_df.createOrReplaceTempView("source")

            result_df = self.spark.sql(self._get_query(transform))
            transform_count = result_df.count()
            logger.info("  [%s] Transformed rows: %s", tid, f"{transform_count:,}")

            msg = ""
            if transform_count > 0:
                self._write(result_df, full_target, strategy, pk_cols)
                rows_loaded = self.spark.table(full_target).count()
                logger.info("  [%s] Silver rows after write: %s", tid, f"{rows_loaded:,}")

                if watermark_col and watermark_col in result_df.columns:
                    new_wm = result_df.agg(F.max(watermark_col)).first()[0]
                    if new_wm is not None:
                        self._update_watermark(tid, new_wm)
                        logger.info("  [%s] Watermark updated → %s", tid, new_wm)
            else:
                msg = "0 rows transformed — skipped write"
                logger.info("  [%s] 0 rows — skipping write.", tid)
                rows_loaded = (
                    self.spark.table(full_target).count()
                    if self.spark.catalog.tableExists(full_target) else 0
                )

            self._update_control(tid, "SUCCESS", rows_loaded, "")
            self._write_migration_log(
                transform, run_id, full_source, full_target,
                rows_loaded, t_start, "SUCCESS", "transform", msg,
            )
            duration = (datetime.utcnow() - t_start).total_seconds()
            return BulkTransformResult(
                transform_id=tid, status="SUCCESS",
                rows_loaded=rows_loaded, duration_seconds=duration,
            )

        except Exception as exc:
            err_msg = str(exc)[:2000]
            try:
                self._update_control(tid, "FAILED", rows_loaded, err_msg)
            except Exception:
                pass
            try:
                self._write_migration_log(
                    transform, run_id, full_source, full_target,
                    rows_loaded, t_start, "FAILED", "transform", err_msg,
                )
            except Exception:
                pass
            raise

    #  Infrastructure setup (parameter.yml-driven) ─

    def _load_parameter_yml(self) -> Dict[str, Any]:
        """Load config/parameter.yml (flat key structure). Returns {} if not found."""
        full_path = os.path.join(self.config_base_path, self.parameter_yml)
        if not os.path.exists(full_path):
            logger.warning("[INFRA] parameter.yml not found at %s — skipping infra setup", full_path)
            return {}
        with open(full_path, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def _ensure_infrastructure(self, params: Dict[str, Any]) -> None:
        """
        Creates all catalogs, schemas, and system tables defined in parameter.yml
        using IF NOT EXISTS so it is safe to call on every pipeline run.

        Reads flat keys from parameter.yml:
          config_catalog / config_schema            → config catalog+schema
          logging_catalog / logging_schema          → watermark, audit, and pipeline log tables
          data_quality_catalog / data_quality_schema → DQ catalog+schema (DQX creates tables)
          governance_catalog / governance_schema    → governance catalog+schema
        """
        if not params:
            return

        print(f"\n{'='*65}")
        print(f"  [INFRA] Ensuring Silver Layer infrastructure from parameter.yml")
        print(f"{'='*65}\n")

        seen_catalogs: set = set()
        seen_schemas:  set = set()

        def _mkcat(cat: str) -> None:
            if cat and cat not in seen_catalogs:
                _safe_create_catalog(self.spark, cat)
                print(f"[INFRA]    Catalog : {cat}")
                seen_catalogs.add(cat)

        def _mksch(cat: str, sch: str) -> None:
            if cat and sch and (cat, sch) not in seen_schemas:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
                print(f"[INFRA]    Schema  : {cat}.{sch}")
                seen_schemas.add((cat, sch))

        #  Catalogs + schemas
        for prefix in ("config", "logging", "data_quality", "governance"):
            cat = params.get(f"{prefix}_catalog", "")
            sch = params.get(f"{prefix}_schema", "")
            _mkcat(cat)
            _mksch(cat, sch)

        #  Audit log table (logging schema — single schema for all audit + logs)
        log_cat = params.get("logging_catalog", "")
        log_sch = params.get("logging_schema", "")
        aud_tbl = params.get("audit_table", "")
        if log_cat and log_sch and aud_tbl:
            aud_full = f"{log_cat}.{log_sch}.{aud_tbl}"
            PipelineLogger(self.spark).ensure_audit_table(aud_full)
            print(f"[INFRA]    Audit table    : {aud_full}")

        #  Migration log table (one record per table per pipeline run)
        log_tbl = params.get("log_table", "")
        if log_cat and log_sch and log_tbl:
            log_full = f"{log_cat}.{log_sch}.{log_tbl}"
            PipelineLogger(self.spark).ensure_migration_log_table(log_full)
            print(f"[INFRA]    Migration log  : {log_full}")

        #  Watermark / control state table (now in logging schema)
        wm_tbl = params.get("watermark_table", "watermark_state")
        if log_cat and log_sch and wm_tbl:
            wm_full = f"`{log_cat}`.`{log_sch}`.`{wm_tbl}`"
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {wm_full} (
                    {self._CONTROL_SCHEMA}
                ) USING DELTA
                COMMENT 'Runtime watermark state for incremental loads'
                """
            )
            print(f"[INFRA]    Watermark table : {log_cat}.{log_sch}.{wm_tbl}")

        print(f"\n[INFRA]  Infrastructure ready\n")

    def _load_config_as_table(self, params: Dict[str, Any]) -> None:
        """
        Read silver_config.csv from the local workspace file path and write it
        as a Delta table in Unity Catalog so it is queryable via SQL.

        Target location from parameter.yml:
          config_catalog.config_schema.config_table
        """
        cat = params.get("config_catalog", "")
        sch = params.get("config_schema", "")
        tbl = params.get("config_table", "")
        if not (cat and sch and tbl):
            logger.warning("[CONFIG] config_catalog/schema/table missing in parameter.yml — skipping")
            return

        target = f"`{cat}`.`{sch}`.`{tbl}`"
        csv_full_path = os.path.join(self.config_base_path, self.csv_path)
        if not os.path.exists(csv_full_path):
            logger.warning("[CONFIG] CSV not found at %s — skipping config table load", csv_full_path)
            return

        # Read CSV with Python (spark.read.csv with local paths is forbidden in
        # Databricks serverless / Spark Connect — use csv.DictReader instead,
        # same approach as load_transforms(), then create the DataFrame in memory)
        rows: List[Dict[str, str]] = []
        with open(csv_full_path, encoding="utf-8", newline="") as fh:
            for raw in csv.DictReader(fh):
                rows.append({k.strip(): (v.strip() if v else "") for k, v in raw.items()})

        if not rows:
            logger.warning("[CONFIG] CSV is empty — skipping config table load")
            return

        df = self.spark.createDataFrame(rows)
        target_plain = f"{cat}.{sch}.{tbl}"

        if self.spark.catalog.tableExists(target_plain):
            # MERGE: update changed rows, insert new rows — preserves runtime state
            df.createOrReplaceTempView("_config_csv_src")
            all_cols = df.columns
            update_set = ", ".join([f"tgt.{c} = src.{c}" for c in all_cols if c != "id"])
            insert_cols = ", ".join(all_cols)
            insert_vals = ", ".join([f"src.{c}" for c in all_cols])
            self.spark.sql(
                f"""
                MERGE INTO {target} AS tgt
                USING _config_csv_src AS src ON tgt.id = src.id
                WHEN MATCHED THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
                """
            )
            logger.info("[CONFIG] silver_config.csv → MERGED into %s (%d rows)", target_plain, len(rows))
            print(f"[CONFIG]  silver_config.csv merged into Delta table: {target_plain} ({len(rows)} rows)")
        else:
            # First run — create table with initial data
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_plain)
            logger.info("[CONFIG] silver_config.csv → created %s (%d rows)", target_plain, len(rows))
            print(f"[CONFIG]  silver_config.csv loaded to Delta table: {target_plain} ({len(rows)} rows)")

    #  Control table ─

    def _ensure_control_table(self) -> None:
        cat, sch, _ = _parse_3part(self.control_table)
        if cat:
            _safe_create_catalog(self.spark, cat)
        if sch:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.control_table} (
                {self._CONTROL_SCHEMA}
            ) USING DELTA
            COMMENT 'Runtime state for BulkTransformEngine'
            """
        )

    def _bootstrap_control_row(self, transform_id: str, csv_last_load_time: str) -> None:
        exists = (
            self.spark.table(self.control_table)
            .filter(F.col("id") == transform_id)
            .count() > 0
        )
        if exists:
            return

        seed_ts = None
        if csv_last_load_time:
            try:
                seed_ts = datetime.fromisoformat(csv_last_load_time)
            except ValueError:
                logger.warning("[%s] Cannot parse last_load_time=%r", transform_id, csv_last_load_time)

        self.spark.createDataFrame(
            [(transform_id, seed_ts, "BOOTSTRAPPED", datetime.utcnow(), 0, "")],
            schema=(
                "id STRING, last_load_time TIMESTAMP, last_run_status STRING, "
                "last_run_ts TIMESTAMP, rows_loaded LONG, error_message STRING"
            ),
        ).write.format("delta").mode("append").saveAsTable(self.control_table)
        logger.info("[%s] Control table seeded — last_load_time=%s", transform_id, seed_ts)

    def _get_last_watermark(self, transform_id: str) -> Optional[datetime]:
        row = (
            self.spark.table(self.control_table)
            .filter(F.col("id") == transform_id)
            .select("last_load_time")
            .first()
        )
        return row["last_load_time"] if row else None

    def _update_watermark(self, transform_id: str, new_watermark: Any) -> None:
        self.spark.createDataFrame(
            [(transform_id, new_watermark)], schema="id STRING, last_load_time TIMESTAMP"
        ).createOrReplaceTempView("_bulk_wm_update")
        self.spark.sql(
            f"""
            MERGE INTO {self.control_table} AS tgt
            USING _bulk_wm_update AS src ON tgt.id = src.id
            WHEN MATCHED THEN UPDATE SET tgt.last_load_time = src.last_load_time
            """
        )

    def _update_control(self, transform_id: str, status: str, rows_loaded: int, error_message: str) -> None:
        self.spark.createDataFrame(
            [(transform_id, status, datetime.utcnow(), rows_loaded, error_message)],
            schema=(
                "id STRING, last_run_status STRING, last_run_ts TIMESTAMP, "
                "rows_loaded LONG, error_message STRING"
            ),
        ).createOrReplaceTempView("_bulk_ctrl_update")
        self.spark.sql(
            f"""
            MERGE INTO {self.control_table} AS tgt
            USING _bulk_ctrl_update AS src ON tgt.id = src.id
            WHEN MATCHED THEN UPDATE SET
                tgt.last_run_status = src.last_run_status,
                tgt.last_run_ts     = src.last_run_ts,
                tgt.rows_loaded     = src.rows_loaded,
                tgt.error_message   = src.error_message
            """
        )

    #  Pipeline stage helpers 

    def _ensure_catalog_schema(self, catalog: str, schema: str) -> None:
        if catalog:
            _safe_create_catalog(self.spark, catalog)
        if catalog and schema:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    def _load_yaml(self, relative_path: str) -> Dict[str, Any]:
        full_path = os.path.join(self.config_base_path, relative_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"[BulkTransform] Config not found: {full_path}")
        with open(full_path, encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def _get_query(self, transform: Dict[str, Any]) -> str:
        sql_file = transform.get("sql_file", "").strip()
        if sql_file:
            full_path = os.path.join(self.config_base_path, sql_file)
            if not os.path.exists(full_path):
                raise FileNotFoundError(f"[{transform['id']}] sql_file not found: {full_path}")
            with open(full_path, encoding="utf-8") as fh:
                return fh.read().strip()
        inline = transform.get("query", "").strip()
        if inline:
            return inline
        logger.warning("[%s] No query/sql_file — using SELECT * FROM source", transform["id"])
        return "SELECT * FROM source"

    def _run_data_quality(
        self, tid: str, full_target: str, dq_config_path: str, run_id: str
    ) -> Dict[str, Any]:
        from utils.dq_engine import DQEngine

        raw = self._load_yaml(dq_config_path)
        dq_cfg = raw.get("data_quality", raw)
        dq_engine = DQEngine(self.spark, dq_cfg)
        _valid_df, _quarantine_df, summary = dq_engine.run(
            self.spark.table(full_target), run_id=run_id
        )
        logger.info(
            "  [%s] DQ: total=%d pass=%d fail=%d status=%s",
            tid, summary["total_rows"], summary["pass_rows"],
            summary["fail_rows"], summary["status"],
        )
        return summary

    def _run_governance(self, transform: Dict[str, Any], gov_config_path: str) -> None:
        from utils.governance_engine import GovernanceEngine

        raw = self._load_yaml(gov_config_path)
        gov_cfg = raw.get("governance", raw)
        pipeline_cfg = {
            "catalog":       transform["target_catalog"],
            "target_schema": transform["target_schema"],
            "target_table":  transform["target_table"],
        }
        GovernanceEngine(self.spark, gov_cfg, pipeline_cfg=pipeline_cfg).apply_all()
        logger.info("  [%s] Governance applied", transform["id"])

    def _write_migration_log(
        self,
        transform: Dict[str, Any],
        run_id: str,
        full_source: str,
        full_target: str,
        rows_loaded: int,
        t_start: datetime,
        status: str = "SUCCESS",
        stage: str = "transform",
        message: str = "",
    ) -> None:
        if not self.migration_log_table:
            return
        duration = (datetime.utcnow() - t_start).total_seconds()
        PipelineLogger(self.spark).write_migration_log(
            log_table=self.migration_log_table,
            run_id=run_id,
            table_id=transform["id"],
            source_table=full_source,
            target_table=full_target,
            load_strategy=transform.get("load_strategy", "merge"),
            rows_loaded=rows_loaded,
            status=status,
            stage=stage,
            message=message,
            duration_seconds=duration,
            level="ERROR" if status == "FAILED" else "INFO",
        )

    def _write_audit_log(
        self,
        transform: Dict[str, Any],
        run_id: str,
        full_target: str,
        source_count: int,
        rows_loaded: int,
        dq_summary: Dict[str, Any],
        t_start: datetime,
        status: str = "SUCCESS",
        error_message: str = "",
    ) -> None:
        audit_table = transform.get("audit_table", "").strip() or self._DEFAULT_AUDIT_TABLE
        duration = int((datetime.utcnow() - t_start).total_seconds())
        parts = full_target.split(".")
        table_name = ".".join(parts[-2:]) if len(parts) >= 2 else full_target

        PipelineLogger(self.spark).write_audit_log(
            audit_table=audit_table,
            run_id=run_id,
            pipeline_name=transform.get("description", transform["id"]),
            table_name=table_name,
            run_by="bulk_transform_engine",
            source_row_count=source_count,
            target_row_count=rows_loaded,
            quarantine_row_count=dq_summary.get("fail_rows", 0),
            dq_pass_count=dq_summary.get("pass_rows", 0),
            dq_fail_count=dq_summary.get("fail_rows", 0),
            load_strategy=transform.get("load_strategy", "merge"),
            status=status,
            error_message=error_message,
            duration_seconds=duration,
        )

    #  Write strategies 

    def _write(self, df: DataFrame, full_target: str, strategy: str, pk_columns: List[str]) -> None:
        logger.info("  Writing → %s  strategy=%s", full_target, strategy)
        if strategy == "full_refresh":
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_target)
            logger.info("  full_refresh complete: %s", full_target)
        elif strategy == "append":
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_target)
            logger.info("  append complete: %s", full_target)
        elif strategy == "merge":
            self._write_merge(df, full_target, pk_columns)
        elif strategy == "scd2":
            raise NotImplementedError(
                "scd2 is not supported in BulkTransformEngine. "
                "Use pipeline_config.yml for SCD Type-2 tables."
            )
        else:
            raise ValueError(f"Unknown load_strategy='{strategy}'.")

    def _write_merge(self, df: DataFrame, full_target: str, pk_columns: List[str]) -> None:
        if not self.spark.catalog.tableExists(full_target):
            logger.info("  Target does not exist — creating: %s", full_target)
            df.limit(0).write.format("delta").saveAsTable(full_target)

        merge_condition = " AND ".join([f"tgt.{k} = src.{k}" for k in pk_columns])
        update_set = {c: f"src.{c}" for c in df.columns if c not in pk_columns}

        (
            DeltaTable.forName(self.spark, full_target)
            .alias("tgt")
            .merge(df.alias("src"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("  merge complete: %s", full_target)

    def _optimize_and_vacuum(self, full_target: str, zorder_cols: List[str], vacuum_hours: int) -> None:
        if zorder_cols:
            self.spark.sql(f"OPTIMIZE {full_target} ZORDER BY ({', '.join(zorder_cols)})")
            logger.info("  OPTIMIZE ZORDER BY (%s) complete", ", ".join(zorder_cols))
        else:
            self.spark.sql(f"OPTIMIZE {full_target}")
            logger.info("  OPTIMIZE complete")
        if vacuum_hours and vacuum_hours > 0:
            self.spark.sql(f"VACUUM {full_target} RETAIN {vacuum_hours} HOURS")
            logger.info("  VACUUM RETAIN %sh complete", vacuum_hours)

    #  Summary ─

    def _print_summary(self, results: List[BulkTransformResult]) -> None:
        width = 72
        print("\n" + "=" * width)
        print("  BulkTransformEngine — Run Summary")
        print("=" * width)
        print(f"  {'ID':<30} {'STATUS':<12} {'ROWS':>10} {'SECS':>8}")
        print("  " + "-" * (width - 2))
        for r in results:
            if r.status == "SUCCESS":
                mark = ""
            elif r.status == "SKIPPED":
                mark = "-"
            else:
                mark = "✗"
            print(f"  {mark} {r.id:<29} {r.status:<12} {r.rows_loaded:>10,} {r.duration_seconds:>7.1f}s")
            if r.error_message and r.status not in ("SKIPPED",):
                msg = r.error_message[:120] + ("…" if len(r.error_message) > 120 else "")
                print(f"    ERROR: {msg}")
        print("=" * width)
        success  = sum(1 for r in results if r.status == "SUCCESS")
        skipped  = sum(1 for r in results if r.status == "SKIPPED")
        failed   = sum(1 for r in results if r.status == "FAILED")
        print(f"  Total: {len(results)}  Success: {success}  Skipped: {skipped}  Failed: {failed}")
        print("=" * width + "\n")


# Module helpers

def _split_pipe(value: str) -> List[str]:
    """Split a pipe-delimited string into a cleaned list. Returns [] if blank."""
    return [v.strip() for v in value.split("|") if v.strip()] if value else []


def _parse_3part(full_name: str):
    """Parse 'catalog.schema.table' → (catalog, schema, table)."""
    parts = full_name.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return "", parts[0], parts[1]
    return "", "", parts[0]
