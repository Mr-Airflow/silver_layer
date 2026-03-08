# utils/transform_engine.py

# BulkTransformEngine — CSV-driven full pipeline (read Bronze → incremental
#                       filter → SQL → Silver write → DQ → Governance →
#                       Optimize → Audit → watermark).
#                       Reads silver_config.csv as the single config source.
#
# Column utility functions — pure Python helpers (no Spark required):
#   to_snake_case, apply_snake_case, apply_rename_columns, apply_drop_columns

import csv
import logging
import os
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from utils.logger import AUDIT_SCHEMA, DEFAULT_AUDIT_TABLE, MIGRATION_LOG_SCHEMA, PipelineLogger

import yaml
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column utility functions — pure Python, no Spark runtime required
# ---------------------------------------------------------------------------

def to_snake_case(name: str) -> str:
    """Convert any column name string to lower_snake_case."""
    # Replace common separators with underscore
    name = re.sub(r"[\s\-\.]", "_", name)
    # Insert underscore before uppercase letters that follow lowercase/digit
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Insert underscore before a run of uppercase that precedes a lowercase
    name = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    # Collapse multiple underscores and strip leading/trailing
    name = re.sub(r"_+", "_", name).strip("_")
    return name.lower()


def apply_snake_case(df) -> "DataFrame":
    """Rename all DataFrame columns to snake_case. No-op if already snake."""
    for col in df.columns:
        snake = to_snake_case(col)
        if snake != col:
            df = df.withColumnRenamed(col, snake)
    return df


def apply_rename_columns(df, rename_map: Dict[str, str]) -> "DataFrame":
    """Apply an explicit column rename map. Logs a warning for missing columns."""
    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
        else:
            logger.warning("apply_rename_columns: column %r not found — skipped", old)
    return df


def apply_drop_columns(df, drops: List[str]) -> "DataFrame":
    """Drop columns that exist in the DataFrame; silently skip missing ones."""
    to_drop = [c for c in drops if c in df.columns]
    if to_drop:
        df = df.drop(*to_drop)
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
    Create the catalog if it does not already exist.
    """
    existing = {row[0].lower() for row in spark.sql("SHOW CATALOGS").collect()}
    if catalog.lower() not in existing:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        logger.info("[INFRA] Created catalog: %s", catalog)
        print(f"[INFRA]    Created catalog : {catalog}")


class BulkTransformEngine:
    """
    CSV-driven unified pipeline — replaces pipeline_config.yml + transformation.yml.

    Reads silver_config.csv (written as a Delta table by the validate stage) and
    runs the full Silver pipeline for every row:
    Bronze read → incremental filter → SQL → Silver write
    → DQ → Governance → Optimize → Audit → watermark update.

    Stage flags (per CSV row, true/false, default shown):
      run_data_quality  false  opt-in  (needs dq_config_path)
      run_governance    false  opt-in  (needs governance_config_path)
      run_optimize      false  opt-in

    Audit always runs for every row (no flag needed).
    The pk column is pipe-separated for composite keys: customer_id|order_id
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
        "source_count     LONG, "
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
    ) -> List[Dict[str, Any]]:
        """Load and parse silver_config.csv, normalising all fields."""
        full_path = os.path.join(self.config_base_path, self.csv_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"[BulkTransform] CSV not found: {full_path}")

        rows: List[Dict[str, Any]] = []
        with open(full_path, encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for raw in reader:
                row = {k.strip(): (v.strip() if v else "") for k, v in raw.items()}

                if filter_ids and row["id"] not in filter_ids:
                    continue

                for flag in ("run_data_quality", "run_governance", "run_optimize"):
                    row[flag] = row.get(flag, "").lower() == "true"

                row["pk"] = _split_pipe(row.get("pk", ""))

                lt = row.get("load_type", "").strip().lower()
                row["load_type"] = lt if lt in ("incremental", "full_refresh") else "incremental"

                rows.append(row)

        logger.info("[BulkTransform] Loaded %d transform(s) from %s", len(rows), full_path)
        return rows

    def load_transforms_from_table(
        self,
        filter_ids: Optional[List[str]] = None,
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
            return self.load_transforms(filter_ids=filter_ids)

        try:
            df = self.spark.table(self.config_table)
        except Exception as exc:
            logger.warning(
                "[BulkTransform] Cannot read config table %s (%s) — falling back to CSV",
                self.config_table, exc,
            )
            return self.load_transforms(filter_ids=filter_ids)

        result: List[Dict[str, Any]] = []
        for raw in df.collect():
            row: Dict[str, Any] = {
                k: (v.strip() if isinstance(v, str) else ("" if v is None else v))
                for k, v in raw.asDict().items()
            }

            if filter_ids and row.get("id") not in filter_ids:
                continue

            for flag in ("run_data_quality", "run_governance", "run_optimize"):
                row[flag] = str(row.get(flag, "")).lower() == "true"

            row["pk"] = _split_pipe(str(row.get("pk", "")))

            lt = str(row.get("load_type", "")).strip().lower()
            row["load_type"] = lt if lt in ("incremental", "full_refresh") else "incremental"

            result.append(row)

        logger.info(
            "[BulkTransform] Loaded %d transform(s) from config table %s",
            len(result), self.config_table,
        )
        return result

    def mark_processed(self, filter_ids: Optional[List[str]] = None) -> None:
        """
        Set processed = 'true' in the config Delta table for all rows in this run.
        If filter_ids is provided, only those IDs are updated; otherwise all rows.
        """
        if not self.config_table:
            logger.warning(
                "[BulkTransform] config_table not set — cannot mark processed"
            )
            return

        if filter_ids:
            escaped = "', '".join(filter_ids)
            where_clause = f"id IN ('{escaped}')"
        else:
            where_clause = "1=1"

        try:
            self.spark.sql(
                f"UPDATE {self.config_table} SET processed = 'true' WHERE {where_clause}"
            )
            target = ", ".join(filter_ids) if filter_ids else "all rows"
            logger.info("[BulkTransform] Marked processed=true for: %s", target)
            print(f"[AUDIT]  Marked processed=true in {self.config_table} for: {target}")
        except Exception as exc:
            logger.error("[BulkTransform] Failed to mark processed: %s", exc, exc_info=True)
            print(f"[AUDIT]  Could not update processed: {exc}")

    def run_all(
        self,
        filter_ids: Optional[List[str]] = None,
        stop_on_error: bool = False,
    ) -> List[BulkTransformResult]:
        """
        Execute the full Silver pipeline in staged order using the same code
        path as the staged notebooks (00–05).  Both entry points are now
        identical — there is only one execution model.

        Order:
          0. validate_all        — infra setup + source accessibility checks
          1. transform_all       — Bronze read → DQ pre-write → Silver write
          2. run_governance_all  — Unity Catalog tags, masking, grants
          3. run_optimize_all    — OPTIMIZE
          4. run_audit_all       — write audit records
          5. mark_processed      — set processed=true in config table
        """
        run_id = str(uuid.uuid4())
        logger.info("=" * 65)
        logger.info("  BulkTransformEngine.run_all — run_id=%s", run_id)
        logger.info("=" * 65)

        self.validate_all(filter_ids=filter_ids)
        results = self.transform_all(
            filter_ids=filter_ids, stop_on_error=stop_on_error, run_id=run_id
        )
        self.run_dq_all(filter_ids=filter_ids, run_id=run_id)
        self.run_governance_all(filter_ids=filter_ids)
        self.run_optimize_all(filter_ids=filter_ids)
        self.run_audit_all(filter_ids=filter_ids, run_id=run_id)
        self.mark_processed(filter_ids=filter_ids)

        self._print_summary(results)
        return results

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
            "table_results":     [{id, source_ok, source_count}, ...],
            "any_run_dq":        bool,
            "any_run_governance": bool,
            "any_run_optimize":   bool,
          }
        """
        #  Step 1: infra setup from parameter.yml
        params = self._load_parameter_yml()
        if params:
            self._ensure_infrastructure(params)
            # Write CSV → Delta table so all subsequent stages read a single
            # consistent snapshot.  After this point every stage uses
            # load_transforms_from_table() — the CSV is never read again.
            self._load_config_as_table(params)

        # Read from the Delta config table (not the CSV) so validate_all and
        # all downstream stages share the exact same row data.
        transforms = self.load_transforms_from_table(filter_ids=filter_ids)
        if not transforms:
            logger.warning("[VALIDATE] No transforms found.")
            return {
                "table_results": [],
                "any_run_dq": False, "any_run_governance": False,
                "any_run_optimize": False,
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
                    print(f"[VALIDATE]  {msg}")
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
            "any_run_optimize":    any(t.get("run_optimize", False) for t in transforms),
        }

    def transform_all(
        self,
        filter_ids: Optional[List[str]] = None,
        stop_on_error: bool = False,
        run_id: str = "",
    ) -> List["BulkTransformResult"]:
        """
        Stage 1: Transform + write for all rows.
        Does NOT run DQ, governance, optimize, or audit — those are separate stages.
        Returns list of BulkTransformResult.
        """
        transforms = self.load_transforms_from_table(filter_ids=filter_ids)
        if not transforms:
            logger.warning("[TRANSFORM] No transforms found.")
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
                print(f"   {tid} — FAILED: {str(exc)[:200]}")
                if stop_on_error:
                    results.append(result)
                    raise
            results.append(result)

        self._print_summary(results)
        return results

    def run_dq_all(
        self,
        filter_ids: Optional[List[str]] = None,
        run_id: str = "",
    ) -> Dict[str, Any]:
        """
        Stage 2: For all rows with run_data_quality=true, execute the full pipeline:
          Bronze read → incremental filter → SQL transform → DQ filter → Silver write
          → watermark update → control table update → migration log.

        Data is only written to Silver AFTER DQ checks pass.
        Rows that fail DQ are diverted to the per-table quarantine table by DQEngine.

        Returns aggregate stats: {pass_rows, fail_rows, tables_checked}.
        """
        transforms = self.load_transforms_from_table(filter_ids=filter_ids)
        total_pass = 0
        total_fail = 0
        applied = 0
        run_id = run_id or str(uuid.uuid4())

        print(f"\n{'='*65}")
        print(f"  [DQ] Running transform + DQ + Silver write  run_id={run_id}")
        print(f"{'='*65}")

        for t in transforms:
            tid = t["id"]
            if not t.get("run_data_quality") or not t.get("dq_config_path", "").strip():
                print(f"[DQ] [{tid}] Skipped (run_data_quality=false or no dq_config_path)")
                continue

            t_start = datetime.utcnow()
            sc  = t.get("source_catalog", "")
            ss  = t.get("source_schema", "")
            st  = t.get("source_table", "")
            tc  = t["target_catalog"]
            ts  = t["target_schema"]
            tt  = t["target_table"]
            strategy      = t.get("load_strategy", "merge").lower()
            pk_cols       = t.get("pk", [])
            watermark_col = t.get("watermark_column", "")
            full_source   = f"{sc}.{ss}.{st}" if sc and ss and st else ""
            full_target   = f"{tc}.{ts}.{tt}"
            source_count  = 0
            rows_loaded   = 0

            try:
                self._ensure_catalog_schema(tc, ts)
                self._bootstrap_control_row(tid, t.get("last_load_time", ""))

                # ── Read source ───────────────────────────────────────────────
                source_df = None
                if full_source:
                    source_df = self.spark.table(full_source)
                    source_count = source_df.count()

                # ── Incremental watermark filter ──────────────────────────────
                load_type = t.get("load_type", "incremental")
                if source_df is not None and watermark_col and load_type == "incremental":
                    last_wm = self._get_last_watermark(tid)
                    if last_wm is not None:
                        source_df = source_df.filter(F.col(watermark_col) > F.lit(last_wm))

                if source_df is not None:
                    source_df.createOrReplaceTempView("source")

                # ── SQL transform ─────────────────────────────────────────────
                result_df = self.spark.sql(self._get_query(t))
                transform_count = result_df.count()

                if transform_count == 0:
                    self._update_control(tid, "SUCCESS", 0, "", source_count)
                    self._write_migration_log(
                        t, run_id, full_source, full_target,
                        0, t_start, "SUCCESS", "data_quality", "0 rows transformed — skipped write",
                    )
                    print(f"[DQ] [{tid}] 0 rows after transform — skipped")
                    continue

                # ── DQ filter — only valid rows reach Silver ──────────────────
                write_df, _quarantine_df, dq_summary = self._run_dq_prewrite(
                    tid, result_df, t["dq_config_path"], run_id
                )
                rows_loaded = dq_summary.get("pass_rows", 0)
                total_pass += rows_loaded
                total_fail += dq_summary.get("fail_rows", 0)
                applied += 1

                print(
                    f"[DQ] [{tid}]  pass={rows_loaded:,}  "
                    f"fail={dq_summary.get('fail_rows', 0):,}  "
                    f"status={dq_summary.get('status', 'PASS')}"
                )

                if rows_loaded == 0:
                    self._update_control(tid, "SUCCESS", 0, "", source_count)
                    self._write_migration_log(
                        t, run_id, full_source, full_target,
                        0, t_start, "SUCCESS", "data_quality",
                        "All rows failed DQ checks — Silver write skipped",
                    )
                    continue

                # ── Write valid rows to Silver ────────────────────────────────
                self._write(write_df, full_target, strategy, pk_cols)

                # ── Watermark update ──────────────────────────────────────────
                if watermark_col and watermark_col in write_df.columns:
                    new_wm = write_df.agg(F.max(watermark_col)).first()[0]
                    if new_wm is not None:
                        self._update_watermark(tid, new_wm)

                self._update_control(tid, "SUCCESS", rows_loaded, "", source_count)
                self._write_migration_log(
                    t, run_id, full_source, full_target,
                    rows_loaded, t_start, "SUCCESS", "data_quality",
                )

            except Exception as e:
                logger.error("[DQ] [%s] Failed: %s", tid, e, exc_info=True)
                print(f"[DQ] [{tid}]  Failed: {e}")
                try:
                    self._update_control(tid, "FAILED", rows_loaded, str(e)[:2000], source_count)
                    self._write_migration_log(
                        t, run_id, full_source, full_target,
                        rows_loaded, t_start, "FAILED", "data_quality", str(e)[:2000],
                    )
                except Exception:
                    pass

        print(
            f"\n[DQ]  Complete — {applied} table(s) processed  "
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
                print(f"[GOVERNANCE] [{t['id']}]  Failed: {e}")

        print(f"\n[GOVERNANCE]  Complete — {applied} table(s) processed")
        return applied

    def run_audit_all(
        self,
        filter_ids: Optional[List[str]] = None,
        run_id: str = "",
        start_time_epoch: int = 0,
    ) -> None:
        """
        Stage 4: Write audit records for all rows (audit always runs).
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
            tid = t["id"]
            full_target = f"{t['target_catalog']}.{t['target_schema']}.{t['target_table']}"
            ctrl = ctrl_map.get(tid)
            status       = (ctrl["last_run_status"] or "UNKNOWN") if ctrl else "UNKNOWN"
            rows_loaded  = int(ctrl["rows_loaded"]   or 0)        if ctrl else 0
            # source_count is stored by _update_control during transform step
            source_count = int(ctrl["source_count"]  or 0)        if ctrl else 0
            err_msg      = (ctrl["error_message"]    or "")        if ctrl else ""

            try:
                self._write_audit_log(
                    t, run_id, full_target,
                    source_count=source_count,
                    rows_loaded=rows_loaded,
                    dq_summary={},
                    t_start=t_start,
                    status=status,
                    error_message=err_msg,
                )
                written += 1
                print(
                    f"[AUDIT] [{tid}]  Audit written — "
                    f"status={status}  source={source_count:,}  rows={rows_loaded:,}"
                )
            except Exception as e:
                logger.error("[AUDIT] [%s] Failed: %s", tid, e, exc_info=True)
                print(f"[AUDIT] [{tid}]  Failed: {e}")

        print(f"\n[AUDIT]  Complete — {written} audit record(s) written")

    def run_optimize_all(self, filter_ids: Optional[List[str]] = None) -> int:
        """
        Stage 5: OPTIMIZE for all rows with run_optimize=yes.
        Returns count of tables optimized.
        """
        transforms = self.load_transforms_from_table(filter_ids=filter_ids)
        applied = 0

        print(f"\n{'='*65}")
        print(f"  [OPTIMIZE] Running OPTIMIZE")
        print(f"{'='*65}")

        for t in transforms:
            if not t.get("run_optimize", False):
                print(f"[OPTIMIZE] [{t['id']}] Skipped (run_optimize=no)")
                continue

            full_target = f"{t['target_catalog']}.{t['target_schema']}.{t['target_table']}"

            try:
                self._optimize_and_vacuum(full_target)
                applied += 1
                print(f"[OPTIMIZE] [{t['id']}]  Optimized {full_target}")
            except Exception as e:
                logger.error("[OPTIMIZE] [%s] Failed: %s", t["id"], e, exc_info=True)
                print(f"[OPTIMIZE] [{t['id']}]  Failed: {e}")

        print(f"\n[OPTIMIZE]  Complete — {applied} table(s) optimized")
        return applied

    def _run_transform_step(
        self, transform: Dict[str, Any], run_id: str = ""
    ) -> "BulkTransformResult":
        """
        Full transform pipeline for one CSV row (used by staged pipeline):
          read Bronze → incremental filter → SQL transform
          → DQ pre-write (if enabled, filters bad rows before Silver write)
          → write valid rows to Silver → update watermark
          → update control table → write migration log.

        Skips the table when processed=true in the config table.
        DQ-enabled tables (run_data_quality=true) are deferred to the DQ stage,
        which handles: Bronze read → transform → DQ filter → Silver write.
        """
        t_start = datetime.utcnow()
        tid = transform["id"]
        run_id = run_id or str(uuid.uuid4())

        if str(transform.get("processed", "false")).lower() == "true":
            print(
                f"\n[TRANSFORM] [{tid}] SKIPPED — processed=true. "
                f"Reset processed to 'false' in the config table to re-run."
            )
            full_target = (
                f"{transform.get('target_catalog','')}"
                f".{transform.get('target_schema','')}"
                f".{transform.get('target_table','')}"
            )
            self._write_migration_log(
                transform, run_id, full_source="", full_target=full_target,
                rows_loaded=0, t_start=t_start, status="SKIPPED",
                stage="transform", message="processed=true — skipped",
            )
            return BulkTransformResult(
                transform_id=tid, status="SKIPPED",
                error_message="processed=true — skipped",
            )

        # Defer DQ-enabled tables to the DQ stage (Stage 2).
        # Stage 2 handles: Bronze read → transform → DQ filter → Silver write.
        # This ensures data is only written to Silver AFTER DQ checks pass.
        if transform.get("run_data_quality") and transform.get("dq_config_path", "").strip():
            full_target = (
                f"{transform.get('target_catalog','')}"
                f".{transform.get('target_schema','')}"
                f".{transform.get('target_table','')}"
            )
            print(
                f"\n[TRANSFORM] [{tid}] DEFERRED — run_data_quality=true. "
                f"Transform + DQ + write will run in the DQ stage."
            )
            self._write_migration_log(
                transform, run_id, full_source="", full_target=full_target,
                rows_loaded=0, t_start=t_start, status="SKIPPED",
                stage="transform", message="run_data_quality=true — deferred to DQ stage",
            )
            return BulkTransformResult(
                transform_id=tid, status="SKIPPED",
                error_message="run_data_quality=true — deferred to DQ stage",
            )

        sc  = transform.get("source_catalog", "")
        ss  = transform.get("source_schema", "")
        st  = transform.get("source_table", "")
        tc  = transform["target_catalog"]
        ts  = transform["target_schema"]
        tt  = transform["target_table"]
        strategy      = transform.get("load_strategy", "merge").lower()
        pk_cols       = transform.get("pk", [])
        watermark_col = transform.get("watermark_column", "")

        full_source  = f"{sc}.{ss}.{st}" if sc and ss and st else ""
        full_target  = f"{tc}.{ts}.{tt}"
        source_count = 0
        rows_loaded  = 0
        dq_summary: Dict[str, Any] = {}

        try:
            if strategy in ("merge", "scd2") and not pk_cols:
                raise ValueError(f"[{tid}] load_strategy='{strategy}' requires pk.")

            self._ensure_catalog_schema(tc, ts)
            self._bootstrap_control_row(tid, transform.get("last_load_time", ""))

            # ── Read source ──────────────────────────────────────────────────
            source_df = None
            if full_source:
                source_df = self.spark.table(full_source)
                source_count = source_df.count()
                logger.info("  [%s] Source rows: %s", tid, f"{source_count:,}")
            else:
                logger.info("  [%s] No source_table — SQL references tables directly.", tid)

            # ── Incremental watermark filter ──────────────────────────────────
            load_type = transform.get("load_type", "incremental")
            if source_df is not None and watermark_col and load_type == "incremental":
                last_wm = self._get_last_watermark(tid)
                if last_wm is not None:
                    source_df = source_df.filter(F.col(watermark_col) > F.lit(last_wm))
                    logger.info(
                        "  [%s] Incremental filter: %r > %s applied", tid, watermark_col, last_wm
                    )
                else:
                    logger.info("  [%s] No previous watermark — initial full load.", tid)

            if source_df is not None:
                source_df.createOrReplaceTempView("source")

            # ── SQL transform ────────────────────────────────────────────────
            result_df = self.spark.sql(self._get_query(transform))
            transform_count = result_df.count()
            logger.info("  [%s] Transformed rows: %s", tid, f"{transform_count:,}")

            if transform_count == 0:
                logger.info("  [%s] 0 rows — skipping Silver write.", tid)
                self._update_control(tid, "SUCCESS", 0, "", source_count)
                self._write_migration_log(
                    transform, run_id, full_source, full_target,
                    0, t_start, "SUCCESS", "transform", "0 rows transformed — skipped write",
                )
                duration = (datetime.utcnow() - t_start).total_seconds()
                return BulkTransformResult(
                    transform_id=tid, status="SUCCESS",
                    rows_loaded=0, duration_seconds=duration,
                )

            # DQ-enabled tables are deferred to the DQ stage before this point,
            # so all tables reaching here have run_data_quality=false.
            write_df = result_df
            rows_loaded = transform_count

            # ── Write to Silver ───────────────────────────────────────────────
            self._write(write_df, full_target, strategy, pk_cols)
            logger.info("  [%s] Silver rows written: %s", tid, f"{rows_loaded:,}")

            # ── Watermark update ──────────────────────────────────────────────
            if watermark_col and watermark_col in write_df.columns:
                new_wm = write_df.agg(F.max(watermark_col)).first()[0]
                if new_wm is not None:
                    self._update_watermark(tid, new_wm)
                    logger.info("  [%s] Watermark updated → %s", tid, new_wm)

            self._update_control(tid, "SUCCESS", rows_loaded, "", source_count)
            self._write_migration_log(
                transform, run_id, full_source, full_target,
                rows_loaded, t_start, "SUCCESS", "transform",
            )
            duration = (datetime.utcnow() - t_start).total_seconds()
            return BulkTransformResult(
                transform_id=tid, status="SUCCESS",
                rows_loaded=rows_loaded, duration_seconds=duration,
            )

        except Exception as exc:
            err_msg = str(exc)[:2000]
            try:
                self._update_control(tid, "FAILED", rows_loaded, err_msg, source_count)
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
        csv_columns: List[str] = []
        with open(csv_full_path, encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            csv_columns = [c.strip() for c in (reader.fieldnames or [])]
            for raw in reader:
                rows.append({k.strip(): (v.strip() if v else "") for k, v in raw.items()})

        if not rows:
            logger.warning("[CONFIG] CSV is empty — skipping config table load")
            return

        df = self.spark.createDataFrame(rows)
        # Enforce column order to match silver_config.csv header exactly
        if csv_columns:
            df = df.select(csv_columns)
        target_plain = f"{cat}.{sch}.{tbl}"

        if self.spark.catalog.tableExists(target_plain):
            existing_cols = set(self.spark.table(target_plain).columns)
            new_cols = set(df.columns)
            if existing_cols != new_cols:
                # Schema changed (column added/removed/renamed) — overwrite to apply new schema.
                # This handles config evolution (e.g. pk_columns → pk, is_processed → processed).
                df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_plain)
                logger.info(
                    "[CONFIG] Schema change detected (old=%s, new=%s) — recreated %s (%d rows)",
                    sorted(existing_cols), sorted(new_cols), target_plain, len(rows),
                )
                print(f"[CONFIG]  Schema updated — silver_config.csv recreated Delta table: {target_plain} ({len(rows)} rows)")
            else:
                # Same schema — MERGE to preserve runtime state (processed flag etc.)
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
        # Migrate existing tables that pre-date the source_count column.
        try:
            self.spark.sql(
                f"ALTER TABLE {self.control_table} ADD COLUMN source_count LONG"
            )
        except Exception:
            pass  # column already exists — safe to ignore

    def _bootstrap_control_row(self, transform_id: str, csv_last_load_time: str) -> None:
        exists = (
            self.spark.table(self.control_table)
            .filter(F.col("id") == transform_id)
            .limit(1)
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
            [(transform_id, seed_ts, "BOOTSTRAPPED", datetime.utcnow(), 0, 0, "")],
            schema=(
                "id STRING, last_load_time TIMESTAMP, last_run_status STRING, "
                "last_run_ts TIMESTAMP, rows_loaded LONG, source_count LONG, error_message STRING"
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

    def _update_control(
        self,
        transform_id: str,
        status: str,
        rows_loaded: int,
        error_message: str,
        source_count: int = 0,
    ) -> None:
        self.spark.createDataFrame(
            [(transform_id, status, datetime.utcnow(), rows_loaded, source_count, error_message)],
            schema=(
                "id STRING, last_run_status STRING, last_run_ts TIMESTAMP, "
                "rows_loaded LONG, source_count LONG, error_message STRING"
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
                tgt.source_count    = src.source_count,
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

    def _run_dq_prewrite(
        self, tid: str, df: DataFrame, dq_config_path: str, run_id: str
    ) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
        """
        Run DQ checks on the transformed DataFrame BEFORE writing to Silver.
        Returns (valid_df, quarantine_df, summary).
        Only valid rows are subsequently written to the Silver table.
        """
        from utils.dq_engine import DQEngine

        raw = self._load_yaml(dq_config_path)
        dq_cfg = raw.get("data_quality", raw)
        dq_engine = DQEngine(self.spark, dq_cfg)
        valid_df, quarantine_df, summary = dq_engine.run(df, run_id=run_id)
        logger.info(
            "  [%s] DQ pre-write: total=%d  pass=%d  fail=%d  status=%s",
            tid, summary["total_rows"], summary["pass_rows"],
            summary["fail_rows"], summary["status"],
        )
        return valid_df, quarantine_df, summary

    def _run_data_quality(
        self, tid: str, full_target: str, dq_config_path: str, run_id: str
    ) -> Dict[str, Any]:
        """Post-write DQ scan on the Silver table (used by run_dq_all / Stage 2)."""
        from utils.dq_engine import DQEngine

        raw = self._load_yaml(dq_config_path)
        dq_cfg = raw.get("data_quality", raw)
        dq_engine = DQEngine(self.spark, dq_cfg)
        _valid_df, _quarantine_df, summary = dq_engine.run(
            self.spark.table(full_target), run_id=run_id
        )
        logger.info(
            "  [%s] DQ post-write: total=%d pass=%d fail=%d status=%s",
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
        audit_table = self._DEFAULT_AUDIT_TABLE
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
        if strategy in ("overwrite", "full_refresh"):
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_target)
            logger.info("  overwrite complete: %s", full_target)
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

    def _optimize_and_vacuum(self, full_target: str) -> None:
        self.spark.sql(f"OPTIMIZE {full_target}")
        logger.info("  OPTIMIZE complete: %s", full_target)

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
                mark = ""
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
