# utils/silver_io.py

# Unified Silver I/O layer — three responsibilities in one module:
#
# WatermarkManager      — Delta-backed watermark state store.
#                         Tracks last processed timestamp per pipeline+table.
#
# IncrementalLoadEngine — Full read/write lifecycle: incremental filter,
#                         full_refresh / append / merge / scd2 write strategies,
#                         post-write OPTIMIZE/VACUUM, watermark update.
#
# MultiSourceReader     — Reads one or more Bronze tables, registers each
#                         as a Spark temp view by alias. Supports single-table
#                         and multi-table join scenarios.
#
# SilverWriter          — Thin public façade over IncrementalLoadEngine:
#                         filter_incremental(), write(), update_watermark().
#                         Used by the single-table staged notebooks.
#
# _resolve_target_table — Helper that builds the 3-part Unity Catalog
#                         table name from pipeline_config.yml fields.


import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


# =============================================================================
# WatermarkManager
# =============================================================================

class WatermarkManager:
    """Tracks and updates pipeline watermarks in a Delta table."""

    def __init__(self, spark: SparkSession, watermark_table: str):
        self.spark = spark
        self.watermark_table = watermark_table
        self._ensure_watermark_table()

    def _ensure_watermark_table(self) -> None:
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.watermark_table} (
                pipeline_name   STRING,
                table_name      STRING,
                watermark_col   STRING,
                last_watermark  TIMESTAMP,
                updated_at      TIMESTAMP
            ) USING DELTA
            """
        )

    def get_last_watermark(self, pipeline_name: str, table_name: str) -> Optional[datetime]:
        df = (
            self.spark.table(self.watermark_table)
            .filter(
                (F.col("pipeline_name") == pipeline_name)
                & (F.col("table_name") == table_name)
            )
            .select("last_watermark")
        )
        if df.count() == 0:
            return None
        return df.first()["last_watermark"]

    def update_watermark(
        self,
        pipeline_name: str,
        table_name: str,
        watermark_col: str,
        new_watermark: datetime,
    ) -> None:
        update_df = (
            self.spark.createDataFrame(
                [(pipeline_name, table_name, watermark_col, new_watermark)],
                schema=(
                    "pipeline_name STRING, table_name STRING, "
                    "watermark_col STRING, last_watermark TIMESTAMP"
                ),
            ).withColumn("updated_at", F.current_timestamp())
        )
        update_df.createOrReplaceTempView("_wm_update_staging")
        self.spark.sql(
            f"""
            MERGE INTO {self.watermark_table} AS target
            USING _wm_update_staging AS source
            ON  target.pipeline_name = source.pipeline_name
            AND target.table_name    = source.table_name
            WHEN MATCHED     THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )


# =============================================================================
# IncrementalLoadEngine
# =============================================================================

class IncrementalLoadEngine:
    """
    Writes Silver DataFrame using the strategy from pipeline_config.yml:
    full_refresh, append, merge, or scd2.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        pipeline_cfg: Dict[str, Any],
    ):
        self.spark = spark
        self.config = config
        self.pipeline_cfg = pipeline_cfg
        self.target_table = config["target_table"]
        self.strategy = config["load_strategy"]

        wm_table = pipeline_cfg.get("watermark_table", "governance.pipeline_watermarks")
        self.watermark_mgr = WatermarkManager(spark, wm_table)

    def get_source_df(self, source_df: DataFrame) -> DataFrame:
        """Apply incremental filter on source DataFrame."""
        incr = self.config.get("incremental_filter", {})
        if not incr.get("enabled", False) or self.strategy == "full_refresh":
            return source_df

        watermark_col = incr["watermark_column"]
        pipeline_name = self.pipeline_cfg.get("pipeline_name", "default")
        table_name = self.config.get("table_name", "default")

        last_wm = self.watermark_mgr.get_last_watermark(pipeline_name, table_name)
        if last_wm is None:
            logger.info("No previous watermark found — performing initial full load")
            return source_df

        lookback_days = incr.get("lookback_days", 0)
        if lookback_days:
            last_wm = last_wm - timedelta(days=lookback_days)

        logger.info("Incremental filter: %s > '%s'", watermark_col, last_wm)
        return source_df.filter(F.col(watermark_col) > F.lit(last_wm))

    def update_watermark(self, source_df: DataFrame) -> None:
        """Store the max watermark from the current batch."""
        incr = self.config.get("incremental_filter", {})
        if not incr.get("enabled", False):
            return

        watermark_col = incr["watermark_column"]
        if watermark_col not in source_df.columns:
            return

        max_wm = source_df.agg(F.max(watermark_col)).first()[0]
        if max_wm:
            self.watermark_mgr.update_watermark(
                self.pipeline_cfg.get("pipeline_name", "default"),
                self.config.get("table_name", "default"),
                watermark_col,
                max_wm,
            )
            logger.info("Watermark updated to: %s", max_wm)

    def write(self, df: DataFrame) -> None:
        """Write DataFrame to target table using configured strategy."""
        logger.info("Writing to '%s' using strategy: %s", self.target_table, self.strategy)

        if self.strategy == "full_refresh":
            self._full_refresh(df)
        elif self.strategy == "append":
            self._append(df)
        elif self.strategy == "merge":
            self._merge(df)
        elif self.strategy == "scd2":
            self._scd2(df)
        else:
            raise ValueError(f"Unknown load_strategy: '{self.strategy}'")

        self._post_write_optimize()

    def _full_refresh(self, df: DataFrame) -> None:
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.target_table)
        )
        logger.info("Full refresh complete: %s", self.target_table)

    def _append(self, df: DataFrame) -> None:
        append_cfg = self.config.get("append", {})

        if append_cfg.get("dedup_before_append", False):
            dedup_keys = append_cfg.get("dedup_keys", [])
            if dedup_keys:
                w = Window.partitionBy(*dedup_keys).orderBy(F.col(dedup_keys[-1]).desc())
                df = (
                    df.withColumn("_rn", F.row_number().over(w))
                    .filter(F.col("_rn") == 1)
                    .drop("_rn")
                )
                logger.info("Deduplication applied on keys: %s", dedup_keys)

        if append_cfg.get("partition_overwrite", False):
            self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            df.write.format("delta").mode("overwrite").saveAsTable(self.target_table)
        else:
            df.write.format("delta").mode("append").saveAsTable(self.target_table)

        logger.info("Append complete: %s", self.target_table)

    def _merge(self, df: DataFrame) -> None:
        merge_cfg = self.config["merge"]
        merge_keys = merge_cfg["merge_keys"]
        upd_cols = merge_cfg.get("update_columns", [])

        merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        if upd_cols:
            update_set = {c: f"source.{c}" for c in upd_cols}
        else:
            update_set = {c: f"source.{c}" for c in df.columns if c not in merge_keys}

        self._ensure_target_exists(df)

        delta_table = DeltaTable.forName(self.spark, self.target_table)
        merge_builder = (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
        )

        if merge_cfg.get("delete_when_not_in_source", False):
            merge_builder = merge_builder.whenNotMatchedBySourceDelete()
        elif merge_cfg.get("soft_delete_column"):
            soft_col = merge_cfg["soft_delete_column"]
            soft_val = merge_cfg.get("soft_delete_value", True)
            merge_builder = merge_builder.whenNotMatchedBySourceUpdate(
                set={soft_col: F.lit(soft_val)}
            )

        if merge_cfg.get("insert_when_not_matched", True):
            merge_builder = merge_builder.whenNotMatchedInsertAll()

        merge_builder.execute()
        logger.info("Merge complete: %s", self.target_table)

    def _scd2(self, df: DataFrame) -> None:
        scd2_cfg = self.config["scd2"]
        biz_keys = scd2_cfg["business_keys"]
        tracked_cols = scd2_cfg["tracked_columns"]
        eff_start = scd2_cfg["effective_start_col"]
        eff_end = scd2_cfg["effective_end_col"]
        curr_flag = scd2_cfg["current_flag_col"]
        sk_col = scd2_cfg.get("surrogate_key_col", "sk_id")

        df = (
            df.withColumn(eff_start, F.current_timestamp())
            .withColumn(eff_end, F.lit(None).cast("timestamp"))
            .withColumn(curr_flag, F.lit(True))
            .withColumn(sk_col, F.expr("uuid()"))
        )

        self._ensure_target_exists(df)

        delta_table = DeltaTable.forName(self.spark, self.target_table)
        change_condition = " OR ".join([f"target.{c} <> source.{c}" for c in tracked_cols])
        key_condition = " AND ".join([f"target.{k} = source.{k}" for k in biz_keys])

        joined = (
            df.alias("source")
            .join(
                delta_table.toDF().filter(F.col(curr_flag)).alias("target"),
                biz_keys,
                "left",
            )
            .withColumn(
                "_action",
                F.when(F.expr(change_condition), F.lit("UPDATE")).otherwise(F.lit("SAME")),
            )
            .filter(F.col("_action") == "UPDATE")
        )
        staged_updates = joined.select([F.col(f"source.{c}").alias(c) for c in df.columns])

        delta_table.alias("target").merge(
            staged_updates.alias("source"),
            f"{key_condition} AND target.{curr_flag} = true",
        ).whenMatchedUpdate(
            set={eff_end: "current_timestamp()", curr_flag: "false"}
        ).execute()

        staged_updates.write.format("delta").mode("append").saveAsTable(self.target_table)
        logger.info("SCD2 complete: %s", self.target_table)

    def _ensure_target_exists(self, df: DataFrame) -> None:
        if not self.spark.catalog.tableExists(self.target_table):
            logger.info("Target table '%s' does not exist — creating it", self.target_table)
            df.limit(0).write.format("delta").saveAsTable(self.target_table)

    def _post_write_optimize(self) -> None:
        write_opts = self.config.get("write_options", {})
        if write_opts.get("optimize_after_write", False):
            zorder_cols = write_opts.get("zorder_columns", [])
            if zorder_cols:
                self.spark.sql(
                    f"OPTIMIZE {self.target_table} ZORDER BY ({', '.join(zorder_cols)})"
                )
            else:
                self.spark.sql(f"OPTIMIZE {self.target_table}")
            logger.info("OPTIMIZE complete: %s", self.target_table)

        vacuum_hours = write_opts.get("vacuum_retain_hours", 168)
        if vacuum_hours:
            self.spark.sql(f"VACUUM {self.target_table} RETAIN {vacuum_hours} HOURS")
            logger.info("VACUUM complete: %s (%sh retention)", self.target_table, vacuum_hours)


# =============================================================================
# MultiSourceReader
# =============================================================================

class MultiSourceReader:
    """
    Reads one or more Bronze tables and registers them as Spark temp views.

    Single source  → source_table: "catalog.schema.table"
    Multi-source   → source_tables: [{alias, table, is_primary}, ...]

    The primary source drives watermark tracking and the audit row count.
    All sources are registered as temp views so SQL transforms can reference
    them by alias.
    """

    def __init__(self, spark: SparkSession, pipeline_cfg: Dict[str, Any]):
        self.spark = spark
        self.pipeline_cfg = pipeline_cfg
        self._sources: Optional[List[Dict[str, Any]]] = None

    def get_sources_config(self) -> List[Dict[str, Any]]:
        """Normalise source config to a unified list format."""
        if self._sources is not None:
            return self._sources

        if "source_tables" in self.pipeline_cfg:
            sources = self.pipeline_cfg["source_tables"]
            if not sources:
                raise ValueError("pipeline.source_tables is defined but empty.")
            if not any(s.get("is_primary", False) for s in sources):
                sources[0]["is_primary"] = True
            self._sources = sources
        elif "source_table" in self.pipeline_cfg:
            self._sources = [
                {"alias": "source", "table": self.pipeline_cfg["source_table"], "is_primary": True}
            ]
        else:
            raise ValueError(
                "pipeline_config must have either 'source_table' (string) "
                "or 'source_tables' (list) defined."
            )
        return self._sources

    def read_all(self) -> Dict[str, DataFrame]:
        """Read all configured Bronze source tables and register as temp views."""
        sources = self.get_sources_config()
        result: Dict[str, DataFrame] = {}

        for src in sources:
            alias = src["alias"]
            table = src["table"]
            logger.info(
                "Reading source: alias=%s, table=%s, is_primary=%s",
                alias, table, src.get("is_primary", False),
            )
            df = self.spark.table(table)
            df.createOrReplaceTempView(alias)
            result[alias] = df
            logger.info("  → registered as temp view '%s'", alias)

        return result

    def get_primary_df(self, source_dfs: Dict[str, DataFrame]) -> DataFrame:
        """Return the DataFrame marked as primary."""
        primary = next(s for s in self.get_sources_config() if s.get("is_primary", False))
        alias = primary["alias"]
        df = source_dfs.get(alias)
        if df is None:
            raise KeyError(
                f"Primary source alias '{alias}' not found. "
                f"Available: {list(source_dfs.keys())}"
            )
        return df

    def total_source_row_count(self, source_dfs: Dict[str, DataFrame]) -> int:
        return sum(df.count() for df in source_dfs.values())

    def primary_source_name(self) -> str:
        primary = next(s for s in self.get_sources_config() if s.get("is_primary", False))
        return primary["table"]

    def is_multi_source(self) -> bool:
        return len(self.get_sources_config()) > 1


# =============================================================================
# SilverWriter + _resolve_target_table
# =============================================================================

def _resolve_target_table(pipeline_cfg: Dict[str, Any]) -> str:
    """
    Build the 3-part Unity Catalog table name from pipeline_config.yml fields.
    Returns catalog.target_schema.target_table when all three are present.
    """
    target = pipeline_cfg.get("target_table", "")
    catalog = pipeline_cfg.get("catalog", "").strip()
    schema = pipeline_cfg.get("target_schema", "").strip()

    if catalog and schema and target:
        if target.count(".") >= 2:
            return target
        return f"{catalog}.{schema}.{target}"
    return target


class SilverWriter:
    """
    Public façade for Silver write operations used by single-table staged notebooks.
    Wraps IncrementalLoadEngine: filter_incremental(), write(), update_watermark().
    """

    def __init__(self, spark: SparkSession, pipeline_cfg: Dict[str, Any]):
        self.spark = spark
        self.pipeline_cfg = pipeline_cfg
        full_table = _resolve_target_table(pipeline_cfg)
        config_for_engine = {**pipeline_cfg, "target_table": full_table}
        self._engine = IncrementalLoadEngine(spark, config_for_engine, pipeline_cfg)

    def filter_incremental(self, source_df: DataFrame, run_mode: str = "incremental") -> DataFrame:
        """Apply watermark filter; returns source_df unchanged for full_refresh."""
        if run_mode == "full_refresh":
            logger.info("Run mode full_refresh — no incremental filter")
            return source_df
        return self._engine.get_source_df(source_df)

    def write(self, df: DataFrame) -> None:
        """Write DataFrame to Silver using configured load strategy."""
        self._engine.write(df)

    def update_watermark(self, df: DataFrame) -> None:
        """Persist max watermark from the written batch for next run."""
        self._engine.update_watermark(df)
