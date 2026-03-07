# utils/logger.py — Unified pipeline audit and logging for the Silver Layer
#
# Replaces the former audit.py concept.  Both pipeline run audit records and
# operational log entries are written here into the single `logging` schema in
# Databricks Unity Catalog.  No separate `audit` schema is required.

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema constants shared by transform_engine, governance_engine, and callers
# ---------------------------------------------------------------------------

MIGRATION_LOG_SCHEMA = (
    "log_id           STRING, "
    "log_ts           TIMESTAMP, "
    "run_id           STRING, "
    "level            STRING, "
    "table_id         STRING, "
    "source_table     STRING, "
    "target_table     STRING, "
    "load_strategy    STRING, "
    "rows_loaded      LONG, "
    "status           STRING, "
    "stage            STRING, "
    "message          STRING, "
    "duration_seconds LONG"
)

AUDIT_SCHEMA = (
    "run_id               STRING, "
    "pipeline_name        STRING, "
    "table_name           STRING, "
    "run_by               STRING, "
    "run_timestamp        TIMESTAMP, "
    "source_row_count     LONG, "
    "target_row_count     LONG, "
    "quarantine_row_count LONG, "
    "dq_pass_count        LONG, "
    "dq_fail_count        LONG, "
    "load_strategy        STRING, "
    "status               STRING, "
    "error_message        STRING, "
    "duration_seconds     LONG"
)

# Default audit table lives in the `logging` schema (single schema for all
# audit + log tables).  Override per-row via the audit_table CSV column.
DEFAULT_AUDIT_TABLE = "cdl_silver.logging.pipeline_run_log"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _parse_3part(full_name: str):
    """Parse 'catalog.schema.table' → (catalog, schema, table)."""
    parts = full_name.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return "", parts[0], parts[1]
    return "", "", parts[0]


# ---------------------------------------------------------------------------
# PipelineLogger
# ---------------------------------------------------------------------------

class PipelineLogger:
    """
    Unified audit and operational log writer for the Silver Layer pipeline.

    Writes both pipeline run audit records and general operational log entries
    into the single ``logging`` schema in Databricks Unity Catalog.

    Usage::

        pl = PipelineLogger(spark)
        pl.write_audit_log(
            audit_table="cdl_silver.logging.pipeline_run_log",
            run_id=run_id,
            pipeline_name="customers_silver",
            table_name="silver.customers",
            run_by="bulk_transform_engine",
            source_row_count=5000,
            target_row_count=4980,
            quarantine_row_count=20,
            dq_pass_count=4980,
            dq_fail_count=20,
            load_strategy="merge",
            status="SUCCESS",
            error_message="",
            duration_seconds=42,
        )
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ------------------------------------------------------------------
    # Audit log (pipeline_run_log)
    # ------------------------------------------------------------------

    def ensure_audit_table(self, audit_table: str) -> None:
        """Create the audit table in Unity Catalog if it does not exist."""
        cat, sch, _ = _parse_3part(audit_table)
        if cat:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cat}`")
        if cat and sch:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {audit_table} (
                {AUDIT_SCHEMA}
            ) USING DELTA
            COMMENT 'Silver layer pipeline audit and log trail'
            """
        )

    def write_audit_log(
        self,
        audit_table: str,
        run_id: str,
        pipeline_name: str,
        table_name: str,
        run_by: str,
        source_row_count: int,
        target_row_count: int,
        quarantine_row_count: int,
        dq_pass_count: int,
        dq_fail_count: int,
        load_strategy: str,
        status: str,
        error_message: str,
        duration_seconds: int,
        run_timestamp: Optional[datetime] = None,
    ) -> None:
        """
        Append one audit record to *audit_table* (defaults to DEFAULT_AUDIT_TABLE).
        The table is created automatically on first write.
        """
        audit_table = audit_table or DEFAULT_AUDIT_TABLE
        self.ensure_audit_table(audit_table)

        self.spark.createDataFrame(
            [(
                run_id or str(uuid.uuid4()),
                pipeline_name,
                table_name,
                run_by,
                run_timestamp or datetime.utcnow(),
                int(source_row_count),
                int(target_row_count),
                int(quarantine_row_count),
                int(dq_pass_count),
                int(dq_fail_count),
                load_strategy,
                status,
                (error_message or "")[:2000],
                int(duration_seconds),
            )],
            schema=AUDIT_SCHEMA,
        ).write.format("delta").mode("append").saveAsTable(audit_table)
        logger.info("Audit log written → %s", audit_table)

    # ------------------------------------------------------------------
    # Migration log (transformation_log — one row per table per run)
    # ------------------------------------------------------------------

    def ensure_migration_log_table(self, log_table: str) -> None:
        """Create the migration log table in Unity Catalog if it does not exist."""
        cat, sch, _ = _parse_3part(log_table)
        if cat:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cat}`")
        if cat and sch:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cat}`.`{sch}`")
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {log_table} (
                {MIGRATION_LOG_SCHEMA}
            ) USING DELTA
            COMMENT 'Silver layer per-table migration log — one record per table per pipeline run'
            """
        )

    def write_migration_log(
        self,
        log_table: str,
        run_id: str,
        table_id: str,
        source_table: str,
        target_table: str,
        load_strategy: str,
        rows_loaded: int,
        status: str,
        stage: str = "transform",
        message: str = "",
        duration_seconds: float = 0,
        level: str = "INFO",
    ) -> None:
        """
        Append one migration log record for a single table processed during a
        pipeline run.  Called once per config-table row after each stage
        completes (transform, dq, governance, optimize, audit).
        """
        if not log_table:
            return
        try:
            self.ensure_migration_log_table(log_table)
            self.spark.createDataFrame(
                [(
                    str(uuid.uuid4()),
                    datetime.utcnow(),
                    run_id,
                    level,
                    table_id,
                    source_table,
                    target_table,
                    load_strategy,
                    int(rows_loaded),
                    status,
                    stage,
                    (message or "")[:2000],
                    int(duration_seconds),
                )],
                schema=MIGRATION_LOG_SCHEMA,
            ).write.format("delta").mode("append").saveAsTable(log_table)
            logger.info("Migration log written → %s  [%s] %s", log_table, table_id, status)
        except Exception as exc:
            logger.warning("Could not write migration log: %s", exc)

    # ------------------------------------------------------------------
    # Governance audit log (silver_audit_log written by GovernanceEngine)
    # ------------------------------------------------------------------

    def write_governance_audit_log(
        self,
        audit_table: str,
        audit_data: Dict[str, Any],
        current_user: Optional[str] = None,
    ) -> None:
        """
        Write a governance-level audit record (called from GovernanceEngine).
        Uses the same AUDIT_SCHEMA so all audit records land in one place.
        """
        audit_table = audit_table or DEFAULT_AUDIT_TABLE
        self.ensure_audit_table(audit_table)

        run_by = audit_data.get("run_by") or current_user or "unknown"
        self.spark.createDataFrame(
            [(
                str(audit_data.get("run_id", uuid.uuid4())),
                str(audit_data.get("pipeline_name", "unknown")),
                str(audit_data.get("table_name", "unknown")),
                run_by,
                audit_data.get("run_timestamp", datetime.utcnow()),
                int(audit_data.get("source_row_count", 0)),
                int(audit_data.get("target_row_count", 0)),
                int(audit_data.get("quarantine_row_count", 0)),
                int(audit_data.get("dq_pass_count", 0)),
                int(audit_data.get("dq_fail_count", 0)),
                str(audit_data.get("load_strategy", "unknown")),
                str(audit_data.get("status", "UNKNOWN")),
                str(audit_data.get("error_message", ""))[:2000],
                int(audit_data.get("duration_seconds", 0)),
            )],
            schema=AUDIT_SCHEMA,
        ).write.format("delta").mode("append").saveAsTable(audit_table)
        logger.info("Governance audit log written → %s", audit_table)
