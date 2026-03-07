#!/usr/bin/env python3

# scripts/generate_job_yml.py

# Reads config/silver_config.csv and rewrites resources/silver_pipeline_job.yml
# including ONLY the stage tasks whose flag is true for at least one enabled row.

# Then deploy:
#   databricks bundle deploy


import argparse
import csv
import os
import sys
from pathlib import Path


 
# CSV flag parsing


def load_flags(csv_path: str) -> dict:
    """
    Read ALL rows from the CSV and return which stage flags are active.

    Stage flags are checked across every row — enabled or disabled.
    The enabled column is a runtime filter; it does not affect which tasks
    should exist in the Databricks job definition.

    Returns:
      {
        "any_run_dq":         bool,
        "any_run_governance": bool,
        "any_run_audit":      bool,
        "any_run_optimize":   bool,
        "enabled_count":      int,   # count of rows with enabled != false
      }
    """
    any_dq  = False
    any_gov = False
    any_aud = False
    any_opt = False
    enabled = 0

    if not os.path.exists(csv_path):
        print(f"ERROR: CSV not found at: {csv_path}", file=sys.stderr)
        sys.exit(1)

    with open(csv_path, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            row = {k.strip(): (v.strip() if v else "") for k, v in row.items()}

            # Count enabled rows (for informational output only)
            if row.get("enabled", "true").lower() not in ("false", "0"):
                enabled += 1

            # Check stage flags on ALL rows regardless of enabled status
            # opt-in: any "true" → include task
            if row.get("run_data_quality", "").lower() == "true":
                any_dq = True
            if row.get("run_governance", "").lower() == "true":
                any_gov = True

            # opt-out: any non-"false" → include task
            if row.get("run_audit", "true").lower() not in ("false", "0"):
                any_aud = True
            if row.get("run_optimize", "true").lower() not in ("false", "0"):
                any_opt = True

    return {
        "any_run_dq":         any_dq,
        "any_run_governance": any_gov,
        "any_run_audit":      any_aud,
        "any_run_optimize":   any_opt,
        "enabled_count":      enabled,
    }



# Task block definitions

def _base_params() -> str:
    return (
        "              config_base_path: ${workspace.file_path}\n"
        '              filter_ids:       ""'
    )


def task_validate() -> str:
    return (
        "        # STAGE 0 - VALIDATE (always runs)\n"
        "        - task_key: validate\n"
        '          description: "Pre-flight: infra setup, CSV->Delta, source checks, publish stage flags"\n'
        "          environment_key: silver_env\n"
        "          notebook_task:\n"
        "            notebook_path: ${workspace.file_path}/notebooks/stages/00_validate\n"
        "            base_parameters:\n"
        + _base_params() + "\n"
        "          max_retries: 0\n"
        "          timeout_seconds: 600"
    )


def task_transform() -> str:
    return (
        "        # STAGE 1 - TRANSFORM (always runs after validate)\n"
        "        - task_key: transform\n"
        '          description: "Read Bronze, apply SQL transforms, write Silver, update watermark"\n'
        "          depends_on:\n"
        "            - task_key: validate\n"
        "          environment_key: silver_env\n"
        "          notebook_task:\n"
        "            notebook_path: ${workspace.file_path}/notebooks/stages/01_transform\n"
        "            base_parameters:\n"
        + _base_params() + "\n"
        '              stop_on_error:    "false"\n'
        "          max_retries: 1\n"
        "          min_retry_interval_millis: 300000\n"
        "          timeout_seconds: 3600"
    )


def task_data_quality() -> str:
    return (
        "        # STAGE 2 - DATA QUALITY (conditional: run_data_quality=true)\n"
        "        - task_key: data_quality\n"
        '          description: "DQX quality scan for tables with run_data_quality=true"\n'
        "          depends_on:\n"
        "            - task_key: transform\n"
        "          environment_key: silver_env\n"
        "          notebook_task:\n"
        "            notebook_path: ${workspace.file_path}/notebooks/stages/02_data_quality\n"
        "            base_parameters:\n"
        + _base_params() + "\n"
        "          max_retries: 0\n"
        "          timeout_seconds: 1800"
    )


def task_governance(has_dq: bool) -> str:
    dep = "data_quality" if has_dq else "transform"
    return (
        "        # STAGE 3 - GOVERNANCE (conditional: run_governance=true)\n"
        "        - task_key: governance\n"
        '          description: "Unity Catalog governance for tables with run_governance=true"\n'
        "          depends_on:\n"
        f"            - task_key: {dep}\n"
        "          environment_key: silver_env\n"
        "          notebook_task:\n"
        "            notebook_path: ${workspace.file_path}/notebooks/stages/03_governance\n"
        "            base_parameters:\n"
        + _base_params() + "\n"
        "          max_retries: 0\n"
        "          timeout_seconds: 900"
    )


def task_audit(has_dq: bool, has_gov: bool) -> str:
    # audit always waits for transform; also waits for dq/gov if included
    deps = ["transform"]
    if has_dq:
        deps.append("data_quality")
    if has_gov:
        deps.append("governance")
    dep_lines = "".join(f"            - task_key: {d}\n" for d in deps)
    return (
        "        # STAGE 4 - AUDIT (conditional: run_audit=true)\n"
        "        # Also marks is_processed=True for completed rows.\n"
        "        - task_key: audit\n"
        '          description: "Write audit records + mark is_processed=True for tables with run_audit=true"\n'
        "          depends_on:\n"
        + dep_lines
        + "          environment_key: silver_env\n"
        "          notebook_task:\n"
        "            notebook_path: ${workspace.file_path}/notebooks/stages/04_audit\n"
        "            base_parameters:\n"
        + _base_params() + "\n"
        "          max_retries: 0\n"
        "          timeout_seconds: 300"
    )


def task_optimize() -> str:
    return (
        "        # STAGE 5 - OPTIMIZE (conditional: run_optimize=true)\n"
        "        - task_key: optimize\n"
        '          description: "OPTIMIZE + ZORDER + VACUUM for tables with run_optimize=true"\n'
        "          depends_on:\n"
        "            - task_key: transform\n"
        "          environment_key: silver_env\n"
        "          notebook_task:\n"
        "            notebook_path: ${workspace.file_path}/notebooks/stages/05_optimize\n"
        "            base_parameters:\n"
        + _base_params() + "\n"
        "          max_retries: 0\n"
        "          timeout_seconds: 3600"
    )


# 
# YAML assembly
# 

def build_yaml(flags: dict) -> str:
    has_dq  = flags["any_run_dq"]
    has_gov = flags["any_run_governance"]
    has_aud = flags["any_run_audit"]
    has_opt = flags["any_run_optimize"]

    included = ["validate", "transform"]
    if has_dq:  included.append("data_quality")
    if has_gov: included.append("governance")
    if has_aud: included.append("audit")
    if has_opt: included.append("optimize")

    task_blocks = [task_validate(), task_transform()]
    if has_dq:  task_blocks.append(task_data_quality())
    if has_gov: task_blocks.append(task_governance(has_dq))
    if has_aud: task_blocks.append(task_audit(has_dq, has_gov))
    if has_opt: task_blocks.append(task_optimize())

    tasks_section = "\n\n".join(task_blocks)

    header = (
        "# resources/silver_pipeline_job.yml\n"
        "# Databricks Asset Bundle job definition — Silver Layer Bulk Pipeline\n"
        "#\n"
        "# AUTO-GENERATED by scripts/generate_job_yml.py — do not edit manually.\n"
        "# Re-run the script whenever silver_config.csv changes:\n"
        "#\n"
        "#   python scripts/generate_job_yml.py\n"
        "#   databricks bundle deploy\n"
        "#\n"
        f"# Tasks included: {', '.join(included)}\n"
        f"# Enabled rows  : {flags['enabled_count']}\n"
        f"# Stage flags   : DQ={has_dq}  Gov={has_gov}  Audit={has_aud}  Optimize={has_opt}\n"
        "\n"
        "resources:\n"
        "  jobs:\n"
        "\n"
        "    silver_layer_pipeline:\n"
        '      name: "[${bundle.target}] Silver - Bulk Transform Pipeline"\n'
        "      description: >\n"
        "        CSV-driven Bronze->Silver pipeline.\n"
        "        All config details are driven by parameter.yml and the config Delta table.\n"
        "        Run scripts/generate_job_yml.py before deploying to include only tasks\n"
        "        whose stage flag is enabled in silver_config.csv.\n"
        "\n"
        "      email_notifications:\n"
        "        on_failure:\n"
        "          - ${var.notification_email}\n"
        "        on_start:   []\n"
        "        on_success: []\n"
        "\n"
        "      max_concurrent_runs: 1\n"
        "\n"
        "      parameters:\n"
        "        - name: filter_ids\n"
        '          default: ""\n'
        "\n"
        "      environments:\n"
        "        - environment_key: silver_env\n"
        "          spec:\n"
        '            environment_version: "2"\n'
        "            dependencies:\n"
        "              - pyyaml>=6.0\n"
        "              - databricks-labs-dqx\n"
        "              - databricks-sdk\n"
        "\n"
        "      tasks:\n"
        "\n"
    )

    return header + tasks_section + "\n"


# 
# Main
# 

def main():
    repo_root = Path(__file__).resolve().parent.parent

    parser = argparse.ArgumentParser(
        description="Generate silver_pipeline_job.yml from silver_config.csv"
    )
    parser.add_argument(
        "--csv",
        default=str(repo_root / "config" / "silver_config.csv"),
        help="Path to silver_config.csv (default: config/silver_config.csv)",
    )
    parser.add_argument(
        "--out",
        default=str(repo_root / "resources" / "silver_pipeline_job.yml"),
        help="Output YAML path (default: resources/silver_pipeline_job.yml)",
    )
    args = parser.parse_args()

    print(f"Reading CSV : {args.csv}")
    flags = load_flags(args.csv)

    print(
        f"  Enabled rows      : {flags['enabled_count']}\n"
        f"  run_data_quality  : {flags['any_run_dq']}\n"
        f"  run_governance    : {flags['any_run_governance']}\n"
        f"  run_audit         : {flags['any_run_audit']}\n"
        f"  run_optimize      : {flags['any_run_optimize']}"
    )

    yaml_text = build_yaml(flags)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(yaml_text, encoding="utf-8")

    included = ["validate", "transform"]
    if flags["any_run_dq"]:         included.append("data_quality")
    if flags["any_run_governance"]: included.append("governance")
    if flags["any_run_audit"]:      included.append("audit")
    if flags["any_run_optimize"]:   included.append("optimize")

    print(f"\nWritten     : {args.out}")
    print(f"Tasks       : {', '.join(included)}")
    print("\nNext step: databricks bundle deploy")


if __name__ == "__main__":
    main()
