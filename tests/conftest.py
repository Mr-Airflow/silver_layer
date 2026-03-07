"""Pytest fixtures for Silver Layer tests."""
from pathlib import Path

import pytest


@pytest.fixture
def repo_root():
    """Absolute path to the repo root (contains utils/ and config/)."""
    return Path(__file__).resolve().parent.parent


@pytest.fixture
def pipeline_config_path():
    """Relative path to the default pipeline config from repo root."""
    return "config/pipeline_config.yml"


@pytest.fixture
def minimal_pipeline_yml():
    """Minimal valid pipeline YAML — used to build temp config dirs."""
    return """
pipeline:
  pipeline_name: test_pipeline
  source_table: bronze.test_table
  target_table: test_table
  catalog: main
  target_schema: silver
  load_strategy: append
  table_name: test_table
  config_paths:
    transformation: config/transformation.yml
    data_quality: config/data_quality.yml
    data_governance: config/data_governance.yml
"""


@pytest.fixture
def minimal_pipeline_merge_yml():
    """Minimal valid pipeline YAML for merge strategy tests."""
    return """
pipeline:
  pipeline_name: test_merge_pipeline
  source_table: bronze.test_table
  target_table: test_table
  catalog: main
  target_schema: silver
  load_strategy: merge
  merge:
    merge_keys:
      - id
  config_paths:
    transformation: config/transformation.yml
    data_quality: config/data_quality.yml
    data_governance: config/data_governance.yml
"""


@pytest.fixture
def temp_config_dir(tmp_path, minimal_pipeline_yml):
    """
    Temp directory with a complete but minimal set of config files.
    Layout mirrors the real project structure:
      tmp_path/
        config/
          pipeline_config.yml
          transformation.yml
          data_quality.yml
          data_governance.yml
    """
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    (config_dir / "pipeline_config.yml").write_text(minimal_pipeline_yml.strip())
    (config_dir / "transformation.yml").write_text(
        "transformation:\n  snake_case_columns: false\n"
    )
    (config_dir / "data_quality.yml").write_text(
        "data_quality:\n  checks: []\n  failure_threshold_pct: 10.0\n"
    )
    (config_dir / "data_governance.yml").write_text(
        "governance:\n  table_owner: data_engineering_team\n"
        "  audit:\n    enabled: false\n"
    )
    return tmp_path


@pytest.fixture
def sample_source_tables_config():
    """Multi-source pipeline config for MultiSourceReader tests."""
    return {
        "source_tables": [
            {"alias": "customers", "table": "bronze.customers", "is_primary": True},
            {"alias": "branches",  "table": "bronze.branches"},
        ]
    }


@pytest.fixture
def sample_single_source_config():
    """Single-source pipeline config for MultiSourceReader tests."""
    return {"source_table": "bronze.customers"}
