# utils/dq_engine.py — Data Quality engine (Databricks DQX)

import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DQPipelineException(Exception):
    """Raised when DQ failure rate exceeds the configured threshold."""


class DQEngine:
    """
    Runs all DQX data quality checks from data_quality.yml.

    Usage:
        engine = DQEngine(spark, cfg.dq)
        valid_df, quarantine_df, summary = engine.run(transformed_df, run_id)
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.quarantine_table = config.get("quarantine_table", "silver.quarantine")
        self.dq_results_table = config.get("dq_results_table", "silver.dq_results")
        self.fail_threshold = float(config.get("failure_threshold_pct", 5.0))
        self.checks = config.get("checks", [])

    def run(
        self,
        df: DataFrame,
        run_id: Optional[str] = None,
    ) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
        run_id = run_id or str(uuid.uuid4())
        logger.info("Starting DQX checks run_id=%s, checks=%d", run_id, len(self.checks))

        if not self.checks:
            logger.info("No checks defined — all rows pass through.")
            empty = self.spark.createDataFrame([], df.schema)
            summary = self._build_summary(run_id, df.count(), 0)
            return df, empty, summary

        try:
            from databricks.labs.dqx.engine import DQEngine as _DQX
            from databricks.sdk import WorkspaceClient
        except ImportError as e:
            raise ImportError(
                "DQX is not installed. Run: %pip install databricks-labs-dqx ; dbutils.library.restartPython()"
            ) from e

        dq_engine = _DQX(WorkspaceClient())

        logger.info("Validating check syntax...")
        status = dq_engine.validate_checks(self.checks)
        if status.has_errors:
            raise ValueError(
                f"[DQ] Invalid check definitions in data_quality.yml: {status.errors}"
            )
        logger.info("Check syntax valid")

        logger.info("Applying checks via DQX...")
        valid_df, quarantine_df = dq_engine.apply_checks_by_metadata_and_split(
            df,
            self.checks,
        )

        quarantine_count = quarantine_df.count()
        if quarantine_count > 0:
            quarantine_df = (
                quarantine_df.withColumn("_dq_run_id", F.lit(run_id))
                .withColumn("_dq_quarantine_ts", F.current_timestamp())
                .withColumn(
                    "_dq_source_table",
                    F.lit(self.config.get("quarantine_table", "")),
                )
            )
            self._write_quarantine(quarantine_df)

        total = df.count()
        fail_count = quarantine_count
        pass_count = total - fail_count
        fail_pct = round((fail_count / total * 100) if total > 0 else 0.0, 4)

        summary = self._build_summary(run_id, total, fail_count)
        self._write_dq_summary(summary)

        logger.info(
            "DQ complete: total=%d, pass=%d, fail=%d, fail_pct=%.2f%%, status=%s",
            total,
            pass_count,
            fail_count,
            fail_pct,
            summary["status"],
        )

        if fail_pct > self.fail_threshold:
            raise DQPipelineException(
                f"[DQ] PIPELINE HALTED: {fail_pct:.2f}% rows failed DQ checks "
                f"(threshold = {self.fail_threshold}%). "
                f"Quarantine table: {self.quarantine_table}"
            )

        return valid_df, quarantine_df, summary

    def _ensure_location(self, full_table: str) -> None:
        """Verify catalog exists and create schema for a 3-part table name if it doesn't."""
        parts = [p.strip().strip("`") for p in full_table.split(".")]
        if len(parts) >= 1 and parts[0]:
            existing = {r[0].lower() for r in self.spark.sql("SHOW CATALOGS").collect()}
            if parts[0].lower() not in existing:
                raise RuntimeError(f"Catalog `{parts[0]}` does not exist. Please create it first.")
        if len(parts) >= 2 and parts[1]:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{parts[0]}`.`{parts[1]}`")

    def _write_quarantine(self, quarantine_df: DataFrame) -> None:
        self._ensure_location(self.quarantine_table)
        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(self.quarantine_table)
        )
        logger.info("Quarantine rows written → %s", self.quarantine_table)

    def _write_dq_summary(self, summary: Dict[str, Any]) -> None:
        self._ensure_location(self.dq_results_table)
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.dq_results_table} (
                run_id      STRING,
                total_rows  LONG,
                pass_rows   LONG,
                fail_rows   LONG,
                fail_pct    DOUBLE,
                status      STRING,
                created_at  TIMESTAMP
            ) USING DELTA
            """
        )
        self.spark.createDataFrame(
            [
                (
                    summary["run_id"],
                    int(summary["total_rows"]),
                    int(summary["pass_rows"]),
                    int(summary["fail_rows"]),
                    float(summary["fail_pct"]),
                    summary["status"],
                    datetime.now(),
                )
            ],
            schema=(
                "run_id STRING, total_rows LONG, pass_rows LONG, "
                "fail_rows LONG, fail_pct DOUBLE, status STRING, created_at TIMESTAMP"
            ),
        ).write.format("delta").mode("append").saveAsTable(self.dq_results_table)

    def _build_summary(self, run_id: str, total: int, fail_count: int) -> Dict[str, Any]:
        pass_count = total - fail_count
        fail_pct = round((fail_count / total * 100) if total > 0 else 0.0, 4)
        status = (
            "FAIL"
            if fail_pct > self.fail_threshold
            else ("WARN" if fail_pct > 0 else "PASS")
        )
        return {
            "run_id": run_id,
            "total_rows": total,
            "pass_rows": pass_count,
            "fail_rows": fail_count,
            "fail_pct": fail_pct,
            "status": status,
        }
