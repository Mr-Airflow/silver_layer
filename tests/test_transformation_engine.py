"""Tests for TransformationEngine — no Spark required (pure-Python logic)."""
import pytest

from utils.transform_engine import (
    apply_drop_columns,
    apply_rename_columns,
    apply_snake_case,
    to_snake_case,
)


# =============================================================================
# to_snake_case
# =============================================================================

class TestToSnakeCase:
    def test_camel_case(self):
        assert to_snake_case("CustomerName") == "customer_name"

    def test_pascal_case(self):
        assert to_snake_case("CustomerAmount") == "customer_amount"

    def test_already_snake(self):
        assert to_snake_case("customer_id") == "customer_id"

    def test_consecutive_uppercase(self):
        assert to_snake_case("HTTPStatusCode") == "http_status_code"

    def test_spaces_to_underscores(self):
        assert to_snake_case("Customer Name") == "customer_name"

    def test_hyphens_to_underscores(self):
        assert to_snake_case("customer-amount") == "customer_amount"

    def test_dots_to_underscores(self):
        assert to_snake_case("customer.id") == "customer_id"

    def test_leading_trailing_underscores_stripped(self):
        assert to_snake_case("_customer_id_") == "customer_id"

    def test_multiple_underscores_collapsed(self):
        assert to_snake_case("customer__id") == "customer_id"

    def test_lowercase_unchanged(self):
        assert to_snake_case("amount") == "amount"

    def test_all_caps_word(self):
        assert to_snake_case("ID") == "id"

    def test_mixed_case_with_numbers(self):
        assert to_snake_case("customer2ID") == "customer2_id"


# =============================================================================
# apply_snake_case  (uses a lightweight mock DataFrame)
# =============================================================================

class _MockDF:
    """Minimal mock for testing apply_snake_case without a real SparkSession."""

    def __init__(self, columns):
        self.columns = list(columns)
        self._renames = {}

    def withColumnRenamed(self, old, new):
        df = _MockDF(
            [new if c == old else c for c in self.columns]
        )
        df._renames = dict(self._renames)
        df._renames[old] = new
        return df


def test_apply_snake_case_renames_columns():
    df = _MockDF(["CustomerName", "CustomerAmount", "customer_id"])
    result = apply_snake_case(df)
    assert "customer_name"   in result.columns
    assert "customer_amount" in result.columns
    assert "customer_id"     in result.columns
    assert "CustomerName"    not in result.columns


def test_apply_snake_case_no_op_when_all_snake():
    df = _MockDF(["customer_id", "customer_name", "amount"])
    result = apply_snake_case(df)
    assert result.columns == ["customer_id", "customer_name", "amount"]


# =============================================================================
# apply_rename_columns
# =============================================================================

def test_apply_rename_columns_renames_existing():
    df = _MockDF(["cust_id", "int_rate", "other"])
    rename_map = {"cust_id": "customer_id", "int_rate": "interest_rate"}
    result = apply_rename_columns(df, rename_map)
    assert "customer_id"   in result.columns
    assert "interest_rate" in result.columns
    assert "cust_id"       not in result.columns


def test_apply_rename_columns_skips_missing(caplog):
    """Renaming a column that doesn't exist should log a warning, not raise."""
    import logging
    df = _MockDF(["col_a"])
    with caplog.at_level(logging.WARNING, logger="utils.transform_engine"):
        result = apply_rename_columns(df, {"nonexistent": "new_name"})
    assert "col_a" in result.columns
    assert any("nonexistent" in msg for msg in caplog.messages)


# =============================================================================
# apply_drop_columns
# =============================================================================

def test_apply_drop_columns_removes_existing():
    class DroppableMockDF(_MockDF):
        def drop(self, *cols):
            return DroppableMockDF([c for c in self.columns if c not in cols])

    df = DroppableMockDF(["customer_id", "_corrupt_record", "_rescued_data", "name"])
    result = apply_drop_columns(df, ["_corrupt_record", "_rescued_data"])
    assert "_corrupt_record" not in result.columns
    assert "_rescued_data"   not in result.columns
    assert "customer_id"     in result.columns


def test_apply_drop_columns_ignores_missing():
    """Dropping columns not present in the DataFrame should not raise."""
    class DroppableMockDF(_MockDF):
        def drop(self, *cols):
            return DroppableMockDF([c for c in self.columns if c not in cols])

    df = DroppableMockDF(["customer_id", "name"])
    result = apply_drop_columns(df, ["nonexistent_col"])
    assert result.columns == ["customer_id", "name"]


# =============================================================================
# GovernanceEngine — _resolve_full_table (no Spark required)
# =============================================================================

class TestResolveFullTable:
    def setup_method(self):
        from utils.governance_engine import _resolve_full_table
        self._resolve = _resolve_full_table

    def test_uses_pipeline_cfg_when_provided(self):
        gov_cfg = {"catalog": "old_cat", "schema": "old_schema", "table": "old_table"}
        pipeline_cfg = {
            "catalog": "main",
            "target_schema": "silver",
            "target_table": "sales_customers",
        }
        catalog, schema, table, full_name = self._resolve(gov_cfg, pipeline_cfg)
        assert catalog   == "main"
        assert schema    == "silver"
        assert table     == "sales_customers"
        assert full_name == "main.silver.sales_customers"

    def test_strips_3part_target_table(self):
        pipeline_cfg = {
            "catalog": "main",
            "target_schema": "silver",
            "target_table": "main.silver.sales_customers",  # already 3-part
        }
        _, _, table, full_name = self._resolve({}, pipeline_cfg)
        assert table     == "sales_customers"
        assert full_name == "main.silver.sales_customers"

    def test_falls_back_to_governance_cfg(self):
        gov_cfg = {
            "catalog": "main",
            "schema": "silver",
            "table": "customers",
            "full_table": "main.silver.customers",
        }
        _, _, table, full_name = self._resolve(gov_cfg, pipeline_cfg=None)
        assert table     == "customers"
        assert full_name == "main.silver.customers"
