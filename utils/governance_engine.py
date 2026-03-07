# utils/governance_engine.py — Unity Catalog governance (tags, masking, RLS, audit)

import logging
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from utils.logger import PipelineLogger

logger = logging.getLogger(__name__)


def _resolve_full_table(
    governance_cfg: Dict[str, Any],
    pipeline_cfg: Optional[Dict[str, Any]],
) -> tuple:
    """
    Resolve (catalog, schema, table, full_name) from pipeline_cfg (preferred)
    or fall back to values in data_governance.yml.

    Rationale: pipeline_config.yml is the single source of truth for table
    identity. Governance config should NOT need to repeat catalog/schema/table.
    """
    if pipeline_cfg:
        catalog = pipeline_cfg.get("catalog", "main")
        schema = pipeline_cfg.get("target_schema", "silver")
        # target_table may be 3-part "main.silver.customers" or just "customers"
        raw_table = pipeline_cfg.get("target_table", "unknown")
        table = raw_table.split(".")[-1]
        full_name = f"{catalog}.{schema}.{table}"
    else:
        catalog = governance_cfg.get("catalog", "main")
        schema = governance_cfg.get("schema", "silver")
        table = governance_cfg.get("table", "unknown")
        full_name = governance_cfg.get(
            "full_table", f"{catalog}.{schema}.{table}"
        )
    return catalog, schema, table, full_name


class GovernanceEngine:
    """
    Applies Unity Catalog governance to a Silver Delta table.
    Driven by data_governance.yml.

    Pass pipeline_cfg so the engine resolves catalog/schema/table automatically
    from pipeline_config.yml — no need to duplicate them in data_governance.yml.

    Usage:
        engine = GovernanceEngine(spark, cfg.governance, pipeline_cfg=cfg.pipeline)
        engine.apply_all()
        engine.write_audit_log(audit_data)
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        pipeline_cfg: Optional[Dict[str, Any]] = None,
    ):
        self.spark = spark
        self.config = config
        self.catalog, self.schema, self.table, self.full_name = _resolve_full_table(
            config, pipeline_cfg
        )

        try:
            from databricks.sdk import WorkspaceClient
            self._wc = WorkspaceClient()
        except ImportError:
            logger.warning(
                "databricks-sdk not installed — SDK-based calls will be skipped."
            )
            self._wc = None

    def apply_table_tags(self) -> None:
        tags = self.config.get("table_tags", {})
        if not tags:
            return
        tags_clause = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
        try:
            self.spark.sql(
                f"ALTER TABLE {self.full_name} SET TAGS ({tags_clause})"
            )
            logger.info("Table tags applied → %s", self.full_name)
        except Exception as exc:
            logger.warning("Table tags failed: %s", exc)

    def apply_column_tags(self) -> None:
        all_tag_entries = []
        for pii_col in self.config.get("pii_columns", []):
            col = pii_col.get("column")
            if col:
                all_tag_entries.append({
                    "column": col,
                    "tags": {
                        pii_col.get("abac_tag_key", "pii"): pii_col.get(
                            "abac_tag_value",
                            pii_col.get("pii_type", "").lower(),
                        ),
                        "pii_type": pii_col.get("pii_type", ""),
                        "sensitivity": pii_col.get("sensitivity", "high"),
                    },
                })
        all_tag_entries.extend(self.config.get("column_tags", []))

        for entry in all_tag_entries:
            col = entry.get("column")
            tags = entry.get("tags", {})
            if not col or not tags:
                continue
            tags_clause = ", ".join(
                [f"'{k}' = '{v}'" for k, v in tags.items() if v]
            )
            if not tags_clause:
                continue
            try:
                self.spark.sql(
                    f"ALTER TABLE {self.full_name} "
                    f"ALTER COLUMN {col} SET TAGS ({tags_clause})"
                )
                logger.info("Column tags set: %s → %s", col, tags)
            except Exception as exc:
                logger.warning("Column tag failed on '%s': %s", col, exc)

    def _ensure_governance_schema(self) -> None:
        """Create governance catalog and schema if they don't exist."""
        try:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS `{self.catalog}`")
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`governance`"
            )
        except Exception as exc:
            logger.warning("Could not ensure governance schema: %s", exc)

    def apply_column_masking(self) -> None:
        self._ensure_governance_schema()

        for pii_col in self.config.get("pii_columns", []):
            col = pii_col.get("column")
            fn_name = pii_col.get("masking_function")
            masking_sql = pii_col.get("masking_sql", "").strip()

            if not col or not fn_name:
                continue

            if masking_sql:
                try:
                    self.spark.sql(masking_sql)
                    logger.info(
                        "Masking function created: %s.governance.%s",
                        self.catalog,
                        fn_name,
                    )
                except Exception as exc:
                    # Log the failure but still attempt SET MASK — the function
                    # may already exist from a previous successful run.
                    logger.warning(
                        "Could not create masking function '%s': %s",
                        fn_name,
                        exc,
                    )

            try:
                self.spark.sql(
                    f"ALTER TABLE {self.full_name} "
                    f"ALTER COLUMN {col} SET MASK {self.catalog}.governance.{fn_name}"
                )
                logger.info("Column mask applied: %s → %s", col, fn_name)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to attach column mask '{self.catalog}.governance.{fn_name}' "
                    f"to column '{col}' on table '{self.full_name}'. "
                    f"PII data would be exposed to all users without this mask. "
                    f"Underlying error: {exc}"
                ) from exc

    def apply_grants(self) -> None:
        if not self._wc:
            logger.warning("SDK not available — skipping grants")
            return
        grants_cfg = self.config.get("grants", [])
        if not grants_cfg:
            return
        try:
            from databricks.sdk.service.catalog import (
                PermissionsChange,
                Privilege,
                SecurableType,
            )
        except ImportError:
            logger.warning(
                "databricks-sdk catalog module not available — skipping grants"
            )
            return

        changes = []
        for g in grants_cfg:
            principal = g.get("principal")
            privileges = g.get("privileges", [])
            if not principal or not privileges:
                continue
            changes.append(
                PermissionsChange(
                    add=[Privilege(p) for p in privileges],
                    principal=principal,
                )
            )
        if not changes:
            return
        try:
            self._wc.grants.update(
                securable_type=SecurableType.TABLE,
                full_name=self.full_name,
                changes=changes,
            )
            logger.info(
                "Grants applied to %s: %s",
                self.full_name,
                [x.get("principal") for x in grants_cfg],
            )
        except Exception as exc:
            logger.warning("Could not apply grants: %s", exc)

    def set_table_owner(self) -> None:
        owner = self.config.get("table_owner")
        if not owner:
            return
        if self._wc:
            try:
                self._wc.tables.update(full_name=self.full_name, owner=owner)
                logger.info("Table owner set (via SDK): %s", owner)
                return
            except Exception as exc:
                logger.warning("Could not set table owner via SDK: %s — trying SQL", exc)
        try:
            self.spark.sql(
                f"ALTER TABLE {self.full_name} SET OWNER TO `{owner}`"
            )
            logger.info("Table owner set (via SQL): %s", owner)
        except Exception as exc:
            logger.warning("Could not set table owner via SQL: %s", exc)

    def apply_row_level_security(self) -> None:
        rls = self.config.get("row_level_security", {})
        if not rls.get("enabled", False):
            return
        fn_name = rls.get("rls_function_name")
        filter_col = rls.get("filter_column")
        admin_group = rls.get("admin_group", "admin")
        mapping_table = rls.get("group_mapping_table", "")

        if not fn_name or not filter_col:
            logger.warning(
                "RLS config missing rls_function_name or filter_column"
            )
            return

        full_fn = f"{self.catalog}.governance.{fn_name}"
        create_fn_sql = f"""
            CREATE OR REPLACE FUNCTION {full_fn}({filter_col} STRING)
            RETURN
                IS_ACCOUNT_GROUP_MEMBER('{admin_group}')
                OR (
                    '{mapping_table}' != ''
                    AND EXISTS (
                        SELECT 1
                        FROM   {mapping_table} m
                        WHERE  IS_ACCOUNT_GROUP_MEMBER(m.user_group)
                        AND    array_contains(m.allowed_branch_codes, {filter_col})
                    )
                )
        """
        try:
            self.spark.sql(create_fn_sql)
            logger.info("RLS function created: %s", full_fn)
        except Exception as exc:
            logger.warning("Could not create RLS function: %s", exc)
            return
        try:
            self.spark.sql(
                f"ALTER TABLE {self.full_name} "
                f"SET ROW FILTER {full_fn} ON ({filter_col})"
            )
            logger.info("Row-level security applied on column: %s", filter_col)
        except Exception as exc:
            logger.warning("Could not attach RLS row filter: %s", exc)

    def write_audit_log(self, audit_data: Dict[str, Any]) -> None:
        audit_cfg = self.config.get("audit", {})
        if not audit_cfg.get("enabled", False):
            return
        audit_table = audit_cfg.get(
            "audit_table", "cdl_silver.logging.silver_audit_log"
        )
        PipelineLogger(self.spark).write_governance_audit_log(
            audit_table=audit_table,
            audit_data={**audit_data, "table_name": f"{self.schema}.{self.table}"},
            current_user=self._current_user(),
        )

    def _current_user(self) -> str:
        try:
            if self._wc:
                return self._wc.current_user.me().user_name or "unknown"
            return self.spark.sql("SELECT current_user()").first()[0]
        except Exception:
            return "unknown"

    def apply_all(self, audit_data: Optional[Dict[str, Any]] = None) -> None:
        logger.info("Applying governance to %s", self.full_name)
        self.apply_table_tags()
        self.apply_column_tags()
        self.apply_column_masking()
        self.apply_grants()
        self.apply_row_level_security()
        self.set_table_owner()
        if audit_data:
            self.write_audit_log(audit_data)
        logger.info("Governance complete")
