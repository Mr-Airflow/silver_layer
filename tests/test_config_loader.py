"""Tests for ConfigLoader and SilverPipelineConfig."""
import pytest

from utils.config_loader import ConfigLoader, SilverPipelineConfig


def test_silver_pipeline_config_defaults():
    """SilverPipelineConfig has default empty dicts for all sections."""
    cfg = SilverPipelineConfig()
    assert cfg.pipeline == {}
    assert cfg.transform == {}
    assert cfg.dq == {}
    assert cfg.governance == {}


def test_config_loader_load_from_temp(temp_config_dir):
    """ConfigLoader loads all 4 configs from a temp directory."""
    loader = ConfigLoader(str(temp_config_dir))
    cfg = loader.load("config/pipeline_config.yml")

    assert isinstance(cfg, SilverPipelineConfig)
    assert cfg.pipeline["pipeline_name"] == "test_pipeline"
    assert cfg.pipeline["source_table"] == "bronze.test_table"
    assert cfg.pipeline["target_table"] == "test_table"
    assert cfg.pipeline["load_strategy"] == "append"
    assert cfg.transform.get("snake_case_columns") is False
    assert cfg.dq.get("failure_threshold_pct") == 10.0


def test_config_loader_missing_file_raises(temp_config_dir):
    """ConfigLoader raises FileNotFoundError when pipeline config is missing."""
    loader = ConfigLoader(str(temp_config_dir))
    with pytest.raises(FileNotFoundError) as exc_info:
        loader.load("config/nonexistent.yml")
    assert "nonexistent.yml" in str(exc_info.value)


def test_config_loader_dbfs_path_normalised():
    """ConfigLoader normalises dbfs:/ to /dbfs/ in base_path."""
    loader = ConfigLoader("dbfs:/Repos/my-repo")
    assert loader.base_path == "/dbfs/Repos/my-repo"


def test_config_loader_invalid_load_strategy(temp_config_dir, tmp_path):
    """ConfigLoader raises ValueError for an unsupported load_strategy."""
    bad_yml = """
pipeline:
  pipeline_name: bad_pipeline
  source_table: bronze.t
  target_table: t
  catalog: main
  target_schema: silver
  load_strategy: streaming
  config_paths:
    transformation: config/transformation.yml
    data_quality: config/data_quality.yml
    data_governance: config/data_governance.yml
"""
    config_dir = tmp_path / "bad" / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "pipeline_config.yml").write_text(bad_yml.strip())
    (config_dir / "transformation.yml").write_text("transformation:\n")
    (config_dir / "data_quality.yml").write_text("data_quality:\n  checks: []\n")
    (config_dir / "data_governance.yml").write_text("governance:\n")

    loader = ConfigLoader(str(tmp_path / "bad"))
    with pytest.raises(ValueError, match="load_strategy"):
        loader.load("config/pipeline_config.yml")


def test_config_loader_merge_without_keys_raises(tmp_path):
    """ConfigLoader raises ValueError when merge strategy has no merge_keys."""
    bad_yml = """
pipeline:
  pipeline_name: merge_pipeline
  source_table: bronze.t
  target_table: t
  catalog: main
  target_schema: silver
  load_strategy: merge
  merge:
    merge_keys: []
  config_paths:
    transformation: config/transformation.yml
    data_quality: config/data_quality.yml
    data_governance: config/data_governance.yml
"""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "pipeline_config.yml").write_text(bad_yml.strip())
    (config_dir / "transformation.yml").write_text("transformation:\n")
    (config_dir / "data_quality.yml").write_text("data_quality:\n  checks: []\n")
    (config_dir / "data_governance.yml").write_text("governance:\n")

    loader = ConfigLoader(str(tmp_path))
    with pytest.raises(ValueError, match="merge_keys"):
        loader.load("config/pipeline_config.yml")
