# utils/config_loader.py 

import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict

import yaml

logger = logging.getLogger(__name__)


@dataclass
class SilverPipelineConfig:
    """Typed container holding all 4 config files as dictionaries."""
    pipeline: Dict[str, Any] = field(default_factory=dict)
    transform: Dict[str, Any] = field(default_factory=dict)
    dq: Dict[str, Any] = field(default_factory=dict)
    governance: Dict[str, Any] = field(default_factory=dict)


class ConfigLoader:
    """
    Loads all 4 YAML configs for a Silver layer pipeline.

    Usage:
        loader = ConfigLoader("/Workspace/Repos/my-repo/config")
        cfg = loader.load("config/pipeline_config.yml")

    Supports:
        - Local filesystem paths
        - /dbfs/ paths
        - dbfs:/ paths (normalised automatically)
    """

    def __init__(self, base_path: str):
        if base_path.startswith("dbfs:/"):
            base_path = base_path.replace("dbfs:/", "/dbfs/")
        self.base_path = base_path.rstrip("/")

    def _load_yaml(self, relative_path: str) -> Dict[str, Any]:
        full = os.path.join(self.base_path, relative_path)
        if not os.path.exists(full):
            raise FileNotFoundError(
                f"Config file not found: {full}\n"
                f"  base_path  = {self.base_path}\n"
                f"  relative   = {relative_path}"
            )
        with open(full, "r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def load(
        self,
        pipeline_config_path: str = "config/pipeline_config.yml",
    ) -> SilverPipelineConfig:
        """
        Load and validate all 4 configs.
        pipeline_config.yml is the entry point — it contains paths to the others.
        """
        raw_pipeline = self._load_yaml(pipeline_config_path)
        pipeline_cfg = raw_pipeline.get("pipeline", {})

        paths = pipeline_cfg.get("config_paths", {})
        _require(
            paths,
            ["transformation", "data_quality", "data_governance"],
            context="pipeline.config_paths",
        )

        raw_transform = self._load_yaml(paths["transformation"])
        raw_dq = self._load_yaml(paths["data_quality"])
        raw_governance = self._load_yaml(paths["data_governance"])

        cfg = SilverPipelineConfig(
            pipeline=pipeline_cfg,
            transform=raw_transform.get("transformation", {}),
            dq=raw_dq.get("data_quality", {}),
            governance=raw_governance.get("governance", {}),
        )
        self._validate(cfg)
        return cfg

    def _validate(self, cfg: SilverPipelineConfig) -> None:
        p = cfg.pipeline
        _require(
            p,
            ["pipeline_name", "source_table", "target_table", "load_strategy", "catalog"],
            context="pipeline_config.yml → pipeline",
        )

        valid = {"full_refresh", "append", "merge", "scd2"}
        if p["load_strategy"] not in valid:
            raise ValueError(
                f"load_strategy '{p['load_strategy']}' is invalid. "
                f"Must be one of: {valid}"
            )
        if p["load_strategy"] == "merge":
            keys = p.get("merge", {}).get("merge_keys", [])
            if not keys:
                raise ValueError(
                    "load_strategy='merge' requires pipeline.merge.merge_keys to be defined."
                )
        if not cfg.dq.get("checks"):
            logger.warning("data_quality.yml has no checks defined.")
        logger.info("Config validation passed")


def _require(d: dict, keys: list, context: str = "") -> None:
    missing = [k for k in keys if k not in d]
    if missing:
        raise KeyError(f"Missing required key(s) in {context}: {missing}")
