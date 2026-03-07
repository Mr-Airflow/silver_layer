# Silver Layer Pipeline Framework

A production-grade, **config-driven** Bronze → Silver data pipeline built on Databricks, Delta Lake, and Unity Catalog. Zero code changes are required to onboard a new table — every behaviour is controlled by four YAML configuration files.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Project Structure](#3-project-structure)
4. [Prerequisites](#4-prerequisites)
5. [Quick Start](#5-quick-start)
6. [Deployment Workflow](#6-deployment-workflow)
7. [Pipeline Stages — End to End](#7-pipeline-stages--end-to-end)
   - [Stage 0 · Validate](#stage-0--validate)
   - [Stage 1 · Transform](#stage-1--transform)
   - [Stage 2 · Data Quality](#stage-2--data-quality)
   - [Stage 3 · Governance](#stage-3--governance)
   - [Stage 4 · Audit](#stage-4--audit)
   - [Stage 5 · Optimize](#stage-5--optimize)
8. [Configuration Reference](#8-configuration-reference)
   - [pipeline_config.yml](#pipeline_configyml)
   - [transformation.yml](#transformationyml)
   - [data_quality.yml](#data_qualityyml)
   - [data_governance.yml](#data_governanceyml)
9. [Transformation Engine — 6-Step Chain](#9-transformation-engine--6-step-chain)
10. [Load Strategies](#10-load-strategies)
11. [Incremental Loading & Watermarks](#11-incremental-loading--watermarks)
12. [Multi-Source Joins](#12-multi-source-joins)
13. [Stage Toggle System](#13-stage-toggle-system)
14. [Utility Modules](#14-utility-modules)
15. [Onboarding a New Table](#15-onboarding-a-new-table)
16. [Monitoring & Observability](#16-monitoring--observability)
17. [Troubleshooting](#17-troubleshooting)

---

## 1. Overview

The Silver Layer Pipeline Framework provides a **repeatable, fully declarative** path from raw Bronze data to a curated, governed Silver table. A single entry point (`pipeline_config.yml`) drives all six pipeline stages:

| Capability | How it works |
|---|---|
| Config-driven | 4 YAML files control everything — no Python changes per table |
| Deployment-time stage control | Optional stages are excluded from the Databricks DAG entirely at deploy time |
| Load strategies | `merge` (SCD1), `append`, `full_refresh`, `scd2` (SCD2) |
| Incremental loads | Watermark-based — only new/changed rows are processed |
| Multi-source | Join multiple Bronze tables before transformation |
| Data quality | Databricks DQX declarative checks with quarantine + threshold halt |
| Unity Catalog governance | Tags, column masking, row-level security, grants — all YAML-driven |
| Audit trail | Every run is logged to a Delta audit table |
| Delta optimisation | OPTIMIZE + ZORDER + VACUUM on every run |
| Idempotent deployment | Safe to deploy multiple times; all DDL is `IF NOT EXISTS` |

**Current pipeline:** `samples.bakehouse.sales_customers` → `cdl_silver.sales.customers`

---

## 2. Architecture

```
+------------------------------------------------------------------------+
|                     Databricks Asset Bundle (DAB)                      |
|                                                                        |
|  pipeline_config.yml ---> scripts/generate_job.py ---> DAB YAML       |
|                                           |                            |
|                    +----------------------v---------------------+      |
|                    |  Databricks Workflow (Serverless)          |      |
|                    |                                            |      |
|  +----------+  +----------+  +------+  +----------+  +------+  |      |
|  | Validate |->|Transform |->|  DQ  |->|Governance|->| Audit|  |      |
|  | (always) |  | (always) |  |(opt.)|  | (opt.)   |  |(alws)|  |      |
|  +----------+  +----------+  +------+  +----------+  +--+---+  |      |
|                                                          |       |      |
|                                                   +------v----+  |      |
|                                                   | Optimize  |  |      |
|                                                   | (opt.)    |  |      |
|                                                   +----------+   |      |
|                    +-------------------------------------------+  |      |
+------------------------------------------------------------------------+

Bronze Layer                   Silver Layer (Unity Catalog)
---------------------          ----------------------------------------
samples.bakehouse               cdl_silver.sales.customers
  .sales_customers  ----------> cdl_silver.data_quality.customers_quarantine
                                cdl_silver.data_quality.dq_results_customers
                                cdl_silver.logging.silver_audit_log
                                cdl_silver.watermark.pipeline_watermarks
                                cdl_silver.logging.pipeline_run_logs
```

### Data Flow Inside a Single Run

```
Bronze Table
    |
    v
MultiSourceReader ----------- reads source_table(s), registers temp views
    |
    v
SilverWriter.filter_incremental  applies watermark filter (or full scan)
    |
    v
TransformationEngine.run ----  6-step chain: snake_case > SQL > derived > drop
    |
    v
SilverWriter.write ----------- merge / append / full_refresh / scd2
    |
    +---> SilverWriter.update_watermark  persist max watermark for next run
    |
    v
DQEngine.run ----------------  DQX checks on full Silver table
    |                            valid rows  --> stay in Silver
    |                            failed rows --> quarantine table
    v
GovernanceEngine.apply_all --  tags / masking / RLS / grants (Unity Catalog)
    |
    v
GovernanceEngine.write_audit_log  append run record to audit Delta table
    |
    v
OPTIMIZE + ZORDER + VACUUM ---  compact and clean Silver table
```

---

## 3. Project Structure

```
Silver_Layer/
|
+-- config/                          <- All pipeline configuration lives here
|   +-- pipeline_config.yml          #  Master config (single entry point)
|   +-- transformation.yml           #  SQL transforms, derived columns, drops
|   +-- data_quality.yml             #  DQX check definitions
|   +-- data_governance.yml          #  Tags, masking, RLS, grants
|
+-- notebooks/
|   +-- run_silver_pipeline.py       #  Single-notebook reference implementation
|   +-- stages/
|       +-- 00_validate.py           #  Stage 0: pre-flight, catalog/schema creation
|       +-- 01_transform.py          #  Stage 1: read > transform > write Silver
|       +-- 02_data_quality.py       #  Stage 2: DQX scan on Silver table
|       +-- 03_governance.py         #  Stage 3: Unity Catalog governance
|       +-- 04_audit.py              #  Stage 4: write audit log record
|       +-- 05_optimize.py           #  Stage 5: OPTIMIZE + ZORDER + VACUUM
|
+-- utils/                           <- Python engine modules (no edits needed)
|   +-- config_loader.py             #  Loads & validates all 4 YAML configs
|   +-- transformation_engine.py     #  6-step transformation chain
|   +-- silver_writer.py             #  Facade: incremental filter + write + watermark
|   +-- incremental_load.py          #  Watermark reads, Delta write strategies
|   +-- multi_source_reader.py       #  Single/multi Bronze table reader
|   +-- dq_engine.py                 #  DQX runner, quarantine writer, summary writer
|   +-- governance_engine.py         #  Unity Catalog tags, masking, RLS, audit log
|
+-- resources/
|   +-- silver_customers_job.yml     #  AUTO-GENERATED -- do not hand-edit
|
+-- scripts/
|   +-- generate_job.py              #  Reads config -> generates resources/*.yml
|   +-- deploy.sh                    #  One-command deploy wrapper
|
+-- tests/                           #  Unit tests for utils/
+-- databricks.yml                   #  DAB root configuration
+-- pyproject.toml                   #  Python project / dependency metadata
```

---

## 4. Prerequisites

| Requirement | Version |
|---|---|
| Databricks workspace | Unity Catalog enabled |
| Databricks CLI | >= 0.200 (Asset Bundle support) |
| Python (local) | >= 3.9 |
| PyYAML (local) | >= 6.0 (`pip install pyyaml`) |
| Databricks runtime | Serverless, Environment version 2 |

Python dependencies installed automatically on Serverless by DAB:

- `pyyaml>=6.0`
- `databricks-labs-dqx`
- `databricks-sdk`

---

## 5. Quick Start

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd Silver_Layer

# 2. Configure your pipeline
#    Edit config/pipeline_config.yml  (source/target tables, catalog, stages)
#    Edit config/transformation.yml   (SQL transforms for your table)
#    Edit config/data_quality.yml     (DQ checks)
#    Edit config/data_governance.yml  (PII columns, tags, grants)

# 3. Install local dependency for the generator
pip install pyyaml

# 4. Deploy to dev
bash scripts/deploy.sh dev

# 5. Run the pipeline
databricks bundle run -t dev silver_customers_pipeline

# 6. Run with full refresh (rewrite entire Silver table)
databricks bundle run -t dev silver_customers_pipeline \
  --python-named-params run_mode=full_refresh
```

---

## 6. Deployment Workflow

### The problem with standard DAB

Databricks Asset Bundle YAML does not support conditional task inclusion based on variable values. If a task is defined in the YAML it always appears in the job — even if it is meant to be disabled. A runtime skip (`dbutils.notebook.exit`) still shows the task as "Succeeded" in the UI.

### The solution: deployment-time generation

This framework uses a **generator script** that reads `pipeline_config.yml` at deploy time and writes `resources/silver_customers_job.yml` with **only the enabled tasks** and automatically rewired `depends_on` chains. Disabled tasks are completely absent from the deployed job.

```
pipeline_config.yml
       |   stages:
       |     run_data_quality: false
       |     run_governance:   false
       v
scripts/generate_job.py
       |
       v
resources/silver_customers_job.yml   <- only: validate, transform, audit, optimize
       |
       v
databricks bundle deploy -t dev      <- data_quality & governance not in UI, not in DAG
```

### Step-by-step deployment

```bash
# Regenerate job YAML from current pipeline_config.yml
python scripts/generate_job.py

# Validate the bundle (no deploy — checks YAML syntax)
databricks bundle validate -t dev

# Deploy
databricks bundle deploy -t dev
databricks bundle deploy -t staging
databricks bundle deploy -t prod

# One-command wrapper (generate + validate + deploy)
bash scripts/deploy.sh dev
bash scripts/deploy.sh prod
```

### Generator output example

With `run_data_quality: false` and `run_governance: false`:

```
=================================================================
  [generate_job] Silver Layer -- Job YAML Generator
=================================================================
  Config  : config/pipeline_config.yml
  Output  : resources/silver_customers_job.yml
  Tasks   : validate -> transform -> audit -> optimize
  Skipped : data_quality, governance
=================================================================
```

Resulting DAG in the Databricks UI:

```
validate --> transform --> audit --> optimize
```

With all stages enabled:

```
validate --> transform --> data_quality --> governance --> audit --> optimize
```

### Runtime overrides

```bash
# Deploy with overrides
databricks bundle deploy -t prod --var="run_mode=full_refresh"
databricks bundle deploy -t prod --var="notification_email=ops@company.com"
```

---

## 7. Pipeline Stages — End to End

### Stage 0 · Validate

**File:** `notebooks/stages/00_validate.py`
**Always runs.** No toggle. Every run starts here.

**What it does:**

1. **Loads all 4 config files** via `ConfigLoader` and validates all required fields
2. **Validates config fields** — asserts `pipeline_name`, `load_strategy`, `source_table` / `source_tables`, `merge_keys` (when `load_strategy=merge`), all `config_paths` references
3. **Source table accessibility check** — runs `spark.table(source_table).count()` and raises a clear error if unreachable
4. **Discovers all Unity Catalog references** across all 4 config files by parsing every table name and masking function SQL, then:
   - Runs `CREATE CATALOG IF NOT EXISTS` for every catalog found
   - Runs `CREATE SCHEMA IF NOT EXISTS` for every schema found
5. **Checks Silver table existence** — prints row count if it exists, or notes it will be created
6. **Publishes task values** so every downstream stage shares the same run context

**Task values published:**

| Key | Type | Description |
|---|---|---|
| `run_id` | string | UUID4 unique to this run |
| `pipeline_name` | string | From `pipeline_config.yml` |
| `run_mode` | string | `incremental` or `full_refresh` |
| `catalog` | string | Unity Catalog catalog name |
| `target_schema` | string | Silver schema name |
| `target_table` | string | Silver table short name |
| `full_target_table` | string | 3-part: `catalog.schema.table` |
| `source_row_count` | int | Row count of the primary Bronze source |
| `start_time_epoch` | int | Unix epoch when this stage started |

**Schemas auto-created (current pipeline):**

| Schema | Purpose |
|---|---|
| `cdl_silver.sales` | Silver data table |
| `cdl_silver.data_quality` | Quarantine table, DQ results table |
| `cdl_silver.governance` | PII masking UDFs, RLS function, user_region_access |
| `cdl_silver.watermark` | Watermark state table |
| `cdl_silver.logging` | Audit log, pipeline run logs |

---

### Stage 1 · Transform

**File:** `notebooks/stages/01_transform.py`
**Always runs.** The core data processing stage.

**What it does:**

1. **Reads Bronze source(s)** via `MultiSourceReader` — supports a single table or multiple joined tables
2. **Applies incremental filter** via `SilverWriter.filter_incremental` — reads the watermark from the watermark state table and filters the source to only rows newer than the last run. Bypassed when `run_mode=full_refresh`
3. **Runs the transformation chain** via `TransformationEngine.run` — 6-step chain (see Section 9)
4. **Writes to Silver** via `SilverWriter.write` using the configured `load_strategy`
5. **Updates watermark** via `SilverWriter.update_watermark` — persists the max watermark from this batch

**Task values published:**

| Key | Type | Description |
|---|---|---|
| `transform_row_count` | int | Rows in the transformed DataFrame |
| `target_row_count` | int | Total rows in Silver after write |

**Retry policy:** 1 retry, 5-minute interval. Timeout: 60 minutes.

---

### Stage 2 · Data Quality

**File:** `notebooks/stages/02_data_quality.py`
**Optional.** Controlled by `run_data_quality` in `pipeline_config.yml stages`.

**What it does:**

1. **Reads the full Silver table** (post-write scan — all rows, not just the new batch)
2. **Validates DQX check syntax** before executing
3. **Applies DQX checks** from `data_quality.yml` — splits the DataFrame into `valid_df` and `quarantine_df`
4. **Writes quarantined rows** to the quarantine table with metadata columns (`_dq_run_id`, `_dq_quarantine_ts`)
5. **Writes DQ summary** to the dq_results table
6. **Raises `DQPipelineException`** if `fail% > failure_threshold_pct` (halts the pipeline before downstream stages)

**DQ row disposition:**

| Check criticality | Row passes | Row fails |
|---|---|---|
| `error` | Stays in Silver | Moved to quarantine only |
| `warn` | Stays in Silver | Stays in Silver AND copied to quarantine |

**Task values published:**

| Key | Type | Description |
|---|---|---|
| `dq_pass_count` | int | Rows passing all error checks |
| `dq_fail_count` | int | Rows sent to quarantine |
| `quarantine_row_count` | int | Same as `dq_fail_count` |
| `dq_status` | string | `PASS`, `WARN`, or `FAIL` |

**DQ results table schema** (`cdl_silver.data_quality.dq_results_customers`):

```sql
run_id      STRING,
total_rows  LONG,
pass_rows   LONG,
fail_rows   LONG,
fail_pct    DOUBLE,
status      STRING,      -- PASS | WARN | FAIL
created_at  TIMESTAMP
```

---

### Stage 3 · Governance

**File:** `notebooks/stages/03_governance.py`
**Optional.** Controlled by `run_governance` in `pipeline_config.yml stages`.

**What it does (in order via `GovernanceEngine.apply_all`):**

1. **Table-level tags** — `ALTER TABLE ... SET TAGS (...)` with all key/value pairs from `table_tags`
2. **Column-level tags** — tags PII columns with `pii`, `pii_type`, `sensitivity`; tags non-PII columns from `column_tags`
3. **Column masking** — for each PII column:
   - `CREATE OR REPLACE FUNCTION catalog.governance.mask_<col>(...)` — SQL UDF that returns the masked value for non-privileged users
   - `ALTER TABLE ... ALTER COLUMN <col> SET MASK catalog.governance.mask_<col>` — attaches the mask; users not in `pii_admin` group see masked values automatically
4. **Table grants** — uses Databricks SDK `WorkspaceClient` to `GRANT SELECT / MODIFY ON TABLE ...` for each principal
5. **Row-level security** — creates an RLS filter function and attaches it with `ALTER TABLE ... SET ROW FILTER ... ON (<filter_col>)`. Members of the `admin` group bypass the filter; all other users see only the rows matching their allowed regions from the mapping table
6. **Table ownership** — `ALTER TABLE ... SET OWNER TO <owner>` (SDK preferred, SQL fallback)

**Current masking functions (all in `cdl_silver.governance`):**

| Column | Function | Masked output example |
|---|---|---|
| `first_name` | `mask_first_name` | `J***` |
| `last_name` | `mask_last_name` | `D***` |
| `full_name` | `mask_full_name` | `John ***` |
| `email_address` | `mask_email` | `user@***.***` |
| `phone_number` | `mask_phone` | `******1234` |
| `address` | `mask_address` | `*** REDACTED ***` |

**Row-level security:**

- Filter column: `continent_region`
- Mapping table: `cdl_silver.governance.user_region_access`
- Schema: `(user_group STRING, allowed_continent_regions ARRAY<STRING>)`
- Members of `admin` group see all rows; others see only their allowed regions

---

### Stage 4 · Audit

**File:** `notebooks/stages/04_audit.py`
**Always runs.** Every pipeline run is audited for compliance and observability.

**What it does:**

1. **Collects metrics** from upstream task values. Optional stages (DQ, Governance) default to `-1` if they were not deployed this run
2. **Writes one audit record** to `cdl_silver.logging.silver_audit_log` (auto-created as Delta if absent)
3. **Prints a run summary** to the notebook output

**Audit log table schema:**

```sql
run_id               STRING,    -- UUID4 unique to this run
pipeline_name        STRING,    -- from pipeline_config.yml
table_name           STRING,    -- schema.table
run_by               STRING,    -- current_user() at run time
run_timestamp        TIMESTAMP,
source_row_count     LONG,      -- rows in Bronze source
target_row_count     LONG,      -- rows in Silver after write (-1 = transform skipped)
quarantine_row_count LONG,      -- rows quarantined       (-1 = DQ skipped)
dq_pass_count        LONG,      -- rows passing DQ        (-1 = DQ skipped)
dq_fail_count        LONG,      -- rows failing DQ        (-1 = DQ skipped)
load_strategy        STRING,    -- merge | append | full_refresh | scd2
status               STRING,    -- SUCCESS
error_message        STRING,    -- empty on success
duration_seconds     LONG       -- wall-clock seconds from validate start
```

---

### Stage 5 · Optimize

**File:** `notebooks/stages/05_optimize.py`
**Optional.** Controlled by `run_optimize` in `pipeline_config.yml stages`.

**What it does:**

1. **`OPTIMIZE ... ZORDER BY (...)`** — compacts small Delta files and co-locates data for faster query performance
2. **`VACUUM ... RETAIN N HOURS`** — purges Delta log files older than the retention window

**Configuration (`pipeline_config.yml -> write_options`):**

| Key | Default | Description |
|---|---|---|
| `zorder_columns` | `[]` | Columns to ZORDER by (most common filter / join keys) |
| `vacuum_retain_hours` | `168` | Hours of Delta history to retain (7 days) |

> ZORDER by columns most commonly used in `WHERE` and `JOIN`. For the customers table this is `customer_id` and `country`.

---

## 8. Configuration Reference

### pipeline_config.yml

The **single entry point** for the entire pipeline. Every other config file is referenced from here.

```yaml
pipeline:

  pipeline_name:    "silver_customers_pipeline"  # Unique identifier
  pipeline_version: "2.0.0"
  environment:      "prod"                        # dev | staging | prod

  # ---- Source ----------------------------------------------------------------
  source_table: "samples.bakehouse.sales_customers"  # Single Bronze table

  # Multi-source (remove source_table above to use this)
  # source_tables:
  #   - alias:      "customers"
  #     table:      "samples.bakehouse.sales_customers"
  #     is_primary: true
  #   - alias:      "branches"
  #     table:      "samples.bakehouse.branch_master"

  # ---- Target ----------------------------------------------------------------
  target_table:  "customers"     # Short name -- catalog+schema added automatically
  catalog:       "cdl_silver"    # Unity Catalog catalog
  target_schema: "sales"         # Full target: cdl_silver.sales.customers

  # ---- Config file paths (relative to config_base_path widget) ---------------
  config_paths:
    transformation:  "config/transformation.yml"
    data_quality:    "config/data_quality.yml"
    data_governance: "config/data_governance.yml"

  # ---- Stage toggles ---------------------------------------------------------
  # false  -> task excluded from deployed DAG (regenerate + redeploy to apply)
  # true   -> task included in DAG
  # absent -> defaults to true
  stages:
    run_transformations: true
    run_data_quality:    false   # excluded from DAG
    run_governance:      false   # excluded from DAG
    run_audit:           true
    run_optimize:        true

  # ---- Load strategy ---------------------------------------------------------
  load_strategy: "merge"   # merge | append | full_refresh | scd2

  # ---- Merge (load_strategy: merge) ------------------------------------------
  merge:
    merge_keys:                   ["customer_id"]
    update_columns:               []      # empty = update all non-key columns
    insert_when_not_matched:      true
    delete_when_not_in_source:    false

  # ---- Append (load_strategy: append) ----------------------------------------
  append:
    dedup_before_append: true
    dedup_keys:          ["customer_id"]

  # ---- SCD Type 2 (load_strategy: scd2) --------------------------------------
  scd2:
    business_keys:       ["customer_id"]
    tracked_columns:     ["email_address", "phone_number", "address", "city"]
    effective_start_col: "effective_start_date"
    effective_end_col:   "effective_end_date"
    current_flag_col:    "is_current"
    surrogate_key_col:   "sk_customer_id"

  # ---- Incremental filter ----------------------------------------------------
  incremental_filter:
    enabled: false           # true when source has a timestamp / updated_at column
    # watermark_column: "updated_at"
    # watermark_type:   "timestamp"
    # lookback_days:    1

  # ---- Write options ---------------------------------------------------------
  write_options:
    optimize_after_write:    true
    zorder_columns:          ["customer_id", "country"]
    vacuum_retain_hours:     168
    enable_change_data_feed: true

  # ---- Schema evolution ------------------------------------------------------
  schema_evolution:
    enabled:                true   # mergeSchema on write
    fail_on_column_removed: false

  # ---- Watermark state table -------------------------------------------------
  watermark_table: "cdl_silver.watermark.pipeline_watermarks"

  # ---- Alerting & logging ----------------------------------------------------
  alerting:
    enabled:          true
    email_on_failure: ["ops@company.com"]
  logging:
    log_level: "INFO"
    log_table: "cdl_silver.logging.pipeline_run_logs"
```

---

### transformation.yml

Defines the 6-step Bronze → Silver transformation chain. Steps execute **in order**.

```yaml
transformation:

  # Step 1: Rename ALL columns to snake_case before anything else
  # customerID -> customer_id, firstName -> first_name, ACCOUNT_ID -> account_id
  snake_case_columns: true

  # Step 2: Explicit renames applied AFTER snake_case conversion
  rename_columns: {}
  # rename_columns:
  #   old_col_name: new_col_name

  # Step 3: Named SQL queries executed in sequence
  # {source} is replaced at runtime with the current chain view name
  sql_transforms:

    - name: "structural_cleanse"
      description: "Standardise formats, filter invalid rows"
      sql: |
        SELECT
            customer_id,
            TRIM(first_name)                         AS first_name,
            TRIM(last_name)                          AS last_name,
            LOWER(TRIM(email_address))               AS email_address,
            TRIM(phone_number)                       AS phone_number,
            TRIM(address)                            AS address,
            INITCAP(TRIM(city))                      AS city,
            UPPER(TRIM(state))                       AS state,
            UPPER(TRIM(country))                     AS country,
            INITCAP(TRIM(continent))                 AS continent,
            TRIM(postal_zip_code)                    AS postal_zip_code,
            INITCAP(TRIM(gender))                    AS gender,
            current_timestamp()                      AS _silver_loaded_at
        FROM {source}
        WHERE customer_id IS NOT NULL

    - name: "business_conformance"
      description: "Add continent_region grouping and normalised gender"
      sql: |
        SELECT *,
            CASE continent
                WHEN 'Asia'          THEN 'APAC'
                WHEN 'Australia'     THEN 'APAC'
                WHEN 'Europe'        THEN 'EMEA'
                WHEN 'Africa'        THEN 'EMEA'
                WHEN 'North America' THEN 'AMERICAS'
                WHEN 'South America' THEN 'AMERICAS'
                ELSE                      'OTHER'
            END AS continent_region,
            CASE
                WHEN UPPER(gender) IN ('MALE', 'M')           THEN 'Male'
                WHEN UPPER(gender) IN ('FEMALE', 'F')         THEN 'Female'
                WHEN UPPER(gender) IN ('OTHER', 'NON-BINARY') THEN 'Other'
                ELSE                                               'Unknown'
            END AS gender_normalised
        FROM {source}

    - name: "deduplication"
      description: "Keep one record per customer_id"
      sql: |
        SELECT * EXCEPT (_row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS _row_num
            FROM {source}
        )
        WHERE _row_num = 1

  # Step 4: Column-level transforms (applied after SQL chain)
  # column_transforms:
  #   - column: "postal_zip_code"
  #     action: "trim"    # trim | upper | lower | cast | date_format | coalesce | regex_replace
  #     params:
  #       dtype: "STRING"

  # Step 5: Derived columns (new columns from Spark SQL expressions)
  derived_columns:
    - name:       "full_name"
      expression: "CONCAT(first_name, ' ', last_name)"
    - name:       "email_domain"
      expression: "SPLIT(email_address, '@')[1]"

  # Step 6: Drop Bronze metadata columns before writing to Silver
  drop_columns: []
  #   - "_corrupt_record"
  #   - "_rescued_data"
```

**What belongs in transformation.yml:**
- Data cleansing: nulls, whitespace, casing, encoding fixes
- Type casting and standardisation
- Deduplication via ROW_NUMBER
- Schema conformance: snake_case, explicit renames
- Shared business logic needed by many downstream consumers

**What does NOT belong here:**
- KPIs, aggregations, summaries -> Gold layer
- Cross-table joins for reporting -> Gold layer

---

### data_quality.yml

Defines Databricks DQX checks run on the Silver table after every write.

```yaml
data_quality:

  quarantine_table:  "cdl_silver.data_quality.customers_quarantine"
  dq_results_table:  "cdl_silver.data_quality.dq_results_customers"

  # Pipeline halts if fail% exceeds this. Set to 100 to never halt.
  failure_threshold_pct: 5.0

  checks:

    # Completeness
    - name: "customer_id_not_null"
      criticality: "error"
      user_metadata:
        check_category: "completeness"
        check_owner:    "data-team@company.com"
      check:
        function: is_not_null
        arguments:
          column: customer_id

    - name: "name_fields_not_empty"
      criticality: "error"
      check:
        function: is_not_null_and_not_empty
        for_each_column: [first_name, last_name]

    # Uniqueness
    - name: "customer_id_unique"
      criticality: "error"
      check:
        function: is_unique
        arguments:
          column: customer_id

    # Validity -- allowed values
    - name: "gender_normalised_valid"
      criticality: "warn"
      check:
        function: is_in_list
        arguments:
          column: gender_normalised
          allowed: ["Male", "Female", "Other", "Unknown"]

    - name: "continent_region_valid"
      criticality: "warn"
      check:
        function: is_in_list
        arguments:
          column: continent_region
          allowed: ["APAC", "EMEA", "AMERICAS", "OTHER"]

    # Validity -- regex format
    - name: "email_format_valid"
      criticality: "warn"
      check:
        function: regex_match
        arguments:
          column: email_address
          regex:  "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"

    # Volume
    - name: "row_count_not_zero"
      criticality: "error"
      check:
        function: is_aggr_not_less_than
        arguments:
          column:     "*"
          aggr_type:  count
          limit:      1
```

**Supported DQX check functions:**

| Function | Description |
|---|---|
| `is_not_null` | Column value must not be null |
| `is_not_null_and_not_empty` | Column must not be null or empty string |
| `is_unique` | Column values must be unique across the dataset |
| `is_in_list` | Value must be in `allowed` list |
| `regex_match` | Value must match the `regex` pattern |
| `sql_expression` | Arbitrary Spark SQL boolean expression |
| `is_aggr_not_less_than` | count/sum/avg must be >= `limit` |
| `is_aggr_not_greater_than` | count/sum/avg must be <= `limit` |

Use `for_each_column` instead of `arguments.column` to apply one rule to multiple columns.

---

### data_governance.yml

Controls all Unity Catalog governance applied to the Silver table.

> The catalog, schema, and table are resolved automatically from `pipeline_config.yml`. Do **not** duplicate them here.

```yaml
governance:

  # Ownership
  table_owner:  "data_engineering_team"
  data_steward: "ops@company.com"

  # Table-level tags (ABAC-compatible key/value pairs)
  table_tags:
    domain:              "retail_customers"
    data_classification: "confidential"
    pii_present:         "true"
    sla_tier:            "tier1"
    layer:               "silver"

  # PII columns: tag + create masking function + attach mask
  pii_columns:
    - column:           "email_address"
      pii_type:         "EMAIL"
      sensitivity:      "high"
      abac_tag_key:     "pii"
      abac_tag_value:   "email"
      masking_function: "mask_email"
      masking_sql: |
        CREATE OR REPLACE FUNCTION cdl_silver.governance.mask_email(email STRING)
        RETURN CASE
            WHEN IS_ACCOUNT_GROUP_MEMBER('pii_admin') THEN email
            ELSE SPLIT(email, '@')[0] || '@***.***'
        END

  # Non-PII column tags
  column_tags:
    - column: "customer_id"
      tags:
        sensitivity: "internal"
        is_pk:       "true"

  # Table access grants
  grants:
    - principal:      "data_analysts"
      privileges:     ["SELECT"]
      principal_type: "group"
    - principal:      "data_engineering_team"
      privileges:     ["SELECT", "MODIFY"]
      principal_type: "group"

  # Row-level security
  row_level_security:
    enabled:             true
    rls_function_name:   "customers_rls_filter"
    filter_column:       "continent_region"
    admin_group:         "admin"
    group_mapping_table: "cdl_silver.governance.user_region_access"

  # Audit trail
  audit:
    enabled:     true
    audit_table: "cdl_silver.logging.silver_audit_log"
```

---

## 9. Transformation Engine — 6-Step Chain

The `TransformationEngine` in `utils/transformation_engine.py` executes all steps from `transformation.yml` in a deterministic sequence.

```
Input DataFrame (Bronze)
        |
        v  Step 1 -- snake_case_columns
        |    to_snake_case("customerID")   -->  "customer_id"
        |    to_snake_case("firstName")    -->  "first_name"
        |    Handles: camelCase, PascalCase, ALL_CAPS, spaces, hyphens, dots
        |
        v  Step 2 -- rename_columns
        |    Explicit renames applied after snake_case
        |    { "old_name": "new_name" }
        |
        v  Step 3 -- sql_transforms (chain)
        |    Each SQL query reads from {source} = output of the previous step
        |    Each result is registered as a new Spark temp view
        |    Failures raise RuntimeError with the full rendered SQL for debugging
        |
        v  Step 4 -- column_transforms
        |    Fine-grained per-column operations
        |    cast | trim | upper | lower | date_format | coalesce | regex_replace
        |
        v  Step 5 -- derived_columns
        |    New columns via F.expr() (Spark SQL expressions)
        |    "CONCAT(first_name, ' ', last_name)"
        |    "SPLIT(email_address, '@')[1]"
        |
        v  Step 6 -- drop_columns
           Remove Bronze metadata columns before writing to Silver
           Missing columns silently ignored

Output DataFrame (Silver-ready)
```

### column_transforms actions

| Action | Required `params` | Description |
|---|---|---|
| `cast` | `dtype` | Cast column to target type |
| `trim` | — | Remove leading/trailing whitespace |
| `upper` | — | Convert to UPPERCASE |
| `lower` | — | Convert to lowercase |
| `date_format` | `input_format`, `output_format` | Reformat date string |
| `coalesce` | `default_value` | Replace null with a literal default |
| `regex_replace` | `pattern`, `replacement` | Replace regex matches |

### Supported cast types

`STRING`, `VARCHAR`, `INT`, `INTEGER`, `LONG`, `BIGINT`, `DOUBLE`, `FLOAT`, `BOOLEAN`, `BOOL`, `DATE`, `TIMESTAMP`, `DECIMAL(p,s)`

### Multi-source SQL transforms

When using `source_tables`, all DataFrames are registered as named temp views **before** the SQL chain executes so any transform can reference them by alias:

```yaml
sql_transforms:
  - name: "join_branches"
    sql: |
      SELECT c.*, b.branch_name, b.region
      FROM {source} c
      JOIN branches b ON c.branch_id = b.branch_id
```

---

## 10. Load Strategies

Set `pipeline.load_strategy` in `pipeline_config.yml`.

### `merge` — SCD Type 1 (default)

UPSERT: matched rows are updated in place, unmatched rows are inserted. History is overwritten.

```yaml
load_strategy: "merge"
merge:
  merge_keys:                ["customer_id"]
  update_columns:            []       # empty list = update ALL non-key columns
  insert_when_not_matched:   true
  delete_when_not_in_source: false    # true = hard-delete rows absent from source
```

### `append`

Insert new rows only. Existing rows are never modified.

```yaml
load_strategy: "append"
append:
  dedup_before_append: true
  dedup_keys:          ["customer_id"]
```

### `full_refresh`

Overwrite the entire Silver table. Use for initial loads, backfills, or schema changes.

```yaml
load_strategy: "full_refresh"
```

Trigger at runtime without changing the config:
```bash
databricks bundle run -t dev silver_customers_pipeline \
  --python-named-params run_mode=full_refresh
```

### `scd2` — SCD Type 2 (full history)

Tracks all historical changes. Each change creates a new row; the old row is closed with `effective_end_date` and `is_current=false`.

```yaml
load_strategy: "scd2"
scd2:
  business_keys:       ["customer_id"]
  tracked_columns:     ["email_address", "phone_number", "address", "city", "state", "country"]
  effective_start_col: "effective_start_date"
  effective_end_col:   "effective_end_date"
  current_flag_col:    "is_current"
  surrogate_key_col:   "sk_customer_id"   # UUID surrogate key auto-generated
```

---

## 11. Incremental Loading & Watermarks

When `incremental_filter.enabled: true`, only rows newer than the last successful run are processed.

```yaml
incremental_filter:
  enabled:          true
  watermark_column: "updated_at"   # Source column to compare against stored watermark
  watermark_type:   "timestamp"    # timestamp | date | integer
  lookback_days:    1              # Overlap window to catch late-arriving records
```

**How it works:**

```
Run N:
  Processes all rows.
  Stores watermark: max(updated_at) = "2024-01-15 10:00:00"

Run N+1 (incremental):
  Filter: WHERE updated_at > "2024-01-14 10:00:00"   (max - lookback_days)
  Processes only new / changed rows since Run N.

Run N+1 (full_refresh mode):
  Filter bypassed. All source rows processed.
  Watermark updated after write.
```

**Watermark state table** (`cdl_silver.watermark.pipeline_watermarks`):

```sql
pipeline_name   STRING,
watermark_col   STRING,
last_value      STRING,     -- stored as string; cast to correct type on read
updated_at      TIMESTAMP
```

**Reset watermark to reprocess all data:**

```sql
DELETE FROM cdl_silver.watermark.pipeline_watermarks
WHERE pipeline_name = 'silver_customers_pipeline';
```
Then run with `run_mode=full_refresh`.

> The current `samples.bakehouse.sales_customers` source has no timestamp column so `incremental_filter.enabled: false`. Enable it when your source gains an `updated_at` column.

---

## 12. Multi-Source Joins

To join multiple Bronze tables before transformation, replace `source_table` with `source_tables`:

```yaml
# Remove: source_table: "..."

source_tables:
  - alias:      "customers"
    table:      "samples.bakehouse.sales_customers"
    is_primary: true           # drives row count reporting and watermark
  - alias:      "branches"
    table:      "samples.bakehouse.branch_master"
```

Then reference aliases directly in `transformation.yml` SQL:

```yaml
sql_transforms:
  - name: "enrich_with_branch"
    sql: |
      SELECT c.*, b.branch_name, b.region
      FROM customers c
      LEFT JOIN branches b ON c.branch_id = b.branch_id
```

`MultiSourceReader` registers each table as a Spark temp view using its alias. Additional DataFrames are also passed to `TransformationEngine.run()` as `extra_views` so every SQL transform in the chain can reference them.

---

## 13. Stage Toggle System

### Always-on vs. optional stages

| Stage | Type | Reason |
|---|---|---|
| `validate` | Always-on | Pre-flight must run every time |
| `transform` | Always-on | Core write — the purpose of the pipeline |
| `data_quality` | **Optional** | May be disabled during initial loads or for trusted sources |
| `governance` | **Optional** | Only needed when tags / masks change; not required every run |
| `audit` | Always-on | Every run must be recorded for compliance |
| `optimize` | **Optional** | May run weekly instead of every load |

### How toggles work

1. Set `run_<stage>: false` in `pipeline_config.yml stages:`
2. Run `python scripts/generate_job.py` — this rewrites `resources/silver_customers_job.yml` with only enabled tasks and rewired `depends_on` chains
3. Run `databricks bundle deploy -t <target>` — the job is redeployed; the disabled task does not exist in the DAG

```
pipeline_config.yml            generated job YAML
---------------------          ------------------
stages:
  run_data_quality: false  -->  (no data_quality task)
  run_governance:   false  -->  (no governance task)
  run_audit:        true   -->  audit task present
  run_optimize:     true   -->  optimize task present
```

### Dependency auto-rewiring

When tasks are excluded the generator builds a gapless linear chain:

| Config | Deployed DAG |
|---|---|
| All enabled | `validate -> transform -> data_quality -> governance -> audit -> optimize` |
| DQ + Governance off | `validate -> transform -> audit -> optimize` |
| Governance only off | `validate -> transform -> data_quality -> audit -> optimize` |
| Optimize off | `validate -> transform -> data_quality -> governance -> audit` |

---

## 14. Utility Modules

### config_loader.py

```python
from utils.config_loader import ConfigLoader

cfg = ConfigLoader(config_base_path).load("config/pipeline_config.yml")
cfg.pipeline    # dict  -- pipeline section
cfg.transform   # dict  -- transformation section
cfg.dq          # dict  -- data_quality section
cfg.governance  # dict  -- governance section
```

Validates all required fields on load. Raises `KeyError` for missing fields, `ValueError` for invalid `load_strategy`.

---

### transformation_engine.py

```python
from utils.transformation_engine import TransformationEngine

engine = TransformationEngine(spark, cfg.transform)

# Single source
silver_df = engine.run(bronze_df)

# Multi-source (extra views accessible in SQL transforms)
silver_df = engine.run(primary_df, extra_views={"customers": df1, "branches": df2})
```

---

### silver_writer.py

```python
from utils.silver_writer import SilverWriter

writer = SilverWriter(spark, cfg.pipeline)

filtered_df = writer.filter_incremental(source_df, run_mode="incremental")
writer.write(transformed_df)          # uses load_strategy from config
writer.update_watermark(transformed_df)
```

`_resolve_target_table(pipeline_cfg)` builds the full 3-part `catalog.schema.table` name from config.

---

### dq_engine.py

```python
from utils.dq_engine import DQEngine, DQPipelineException

engine = DQEngine(spark, cfg.dq)
valid_df, quarantine_df, summary = engine.run(silver_df, run_id=run_id)
# Raises DQPipelineException if fail% > failure_threshold_pct
```

`summary` keys: `run_id`, `total_rows`, `pass_rows`, `fail_rows`, `fail_pct`, `status`

---

### governance_engine.py

```python
from utils.governance_engine import GovernanceEngine

# Table identity resolved automatically from pipeline_cfg
engine = GovernanceEngine(spark, cfg.governance, pipeline_cfg=cfg.pipeline)
engine.apply_all()                      # tags + masking + RLS + grants + ownership
engine.write_audit_log(audit_dict)      # append one record to the audit Delta table
```

---

### multi_source_reader.py

```python
from utils.multi_source_reader import MultiSourceReader

reader     = MultiSourceReader(spark, cfg.pipeline)
source_dfs = reader.read_all()               # dict: alias -> DataFrame
primary_df = reader.get_primary_df(source_dfs)
is_multi   = reader.is_multi_source()
```

---

## 15. Onboarding a New Table

No changes to `utils/` or `notebooks/` are ever required. Only config files and one resources YAML need to be created.

### Step-by-step

```
1.  Create a config folder for the new table
    config/
    +-- orders/
        +-- pipeline_config.yml
        +-- transformation.yml
        +-- data_quality.yml
        +-- data_governance.yml

2.  Fill in pipeline_config.yml
    - source_table:    the Bronze table name
    - target_table:    the Silver table name (short)
    - catalog:         your Unity Catalog catalog
    - target_schema:   Silver schema
    - load_strategy:   merge | append | full_refresh | scd2
    - merge.merge_keys (if merge)
    - config_paths:    point to the 3 other config files

3.  Write transformation.yml
    - sql_transforms: write SQL specific to the new table schema
    - derived_columns, drop_columns as needed

4.  Write data_quality.yml
    - checks: DQX rules for the new table's columns

5.  Write data_governance.yml
    - pii_columns: any PII in the new table
    - grants: who needs access
    - table_tags, rls if required

6.  Generate the job YAML
    python scripts/generate_job.py \
      config/orders/pipeline_config.yml \
      resources/silver_orders_job.yml

7.  Deploy
    databricks bundle deploy -t dev
```

### Minimum viable pipeline_config.yml

```yaml
pipeline:
  pipeline_name:    "silver_orders_pipeline"
  source_table:     "bronze.sales.raw_orders"
  target_table:     "orders"
  catalog:          "cdl_silver"
  target_schema:    "sales"
  load_strategy:    "merge"
  merge:
    merge_keys:     ["order_id"]
  config_paths:
    transformation:  "config/orders/transformation.yml"
    data_quality:    "config/orders/data_quality.yml"
    data_governance: "config/orders/data_governance.yml"
  stages:
    run_data_quality: true
    run_governance:   true
    run_audit:        true
    run_optimize:     true
  write_options:
    zorder_columns:      ["order_id", "customer_id"]
    vacuum_retain_hours: 168
  watermark_table:  "cdl_silver.watermark.pipeline_watermarks"
  logging:
    log_table: "cdl_silver.logging.pipeline_run_logs"
```

---

## 16. Monitoring & Observability

### Audit log — `cdl_silver.logging.silver_audit_log`

One record per run. The primary observability table.

```sql
-- Last 10 runs
SELECT run_timestamp, pipeline_name, status, duration_seconds,
       source_row_count, target_row_count, quarantine_row_count
FROM cdl_silver.logging.silver_audit_log
ORDER BY run_timestamp DESC
LIMIT 10;

-- Slow runs (> 30 minutes)
SELECT run_id, run_timestamp, duration_seconds
FROM cdl_silver.logging.silver_audit_log
WHERE duration_seconds > 1800
ORDER BY run_timestamp DESC;

-- Runs with quarantine activity
SELECT run_timestamp, quarantine_row_count, dq_fail_count,
       ROUND(dq_fail_count * 100.0 / NULLIF(source_row_count, 0), 2) AS fail_pct
FROM cdl_silver.logging.silver_audit_log
WHERE quarantine_row_count > 0
ORDER BY run_timestamp DESC;

-- Silver table row count over time
SELECT DATE(run_timestamp) AS run_date, MAX(target_row_count) AS silver_rows
FROM cdl_silver.logging.silver_audit_log
WHERE pipeline_name = 'silver_customers_pipeline'
GROUP BY DATE(run_timestamp)
ORDER BY run_date DESC;
```

### DQ results — `cdl_silver.data_quality.dq_results_customers`

```sql
-- DQ trend
SELECT DATE(created_at) AS run_date,
       AVG(fail_pct)    AS avg_fail_pct,
       MAX(fail_pct)    AS max_fail_pct,
       COUNT(*)         AS run_count
FROM cdl_silver.data_quality.dq_results_customers
GROUP BY DATE(created_at)
ORDER BY run_date DESC;
```

### Quarantine table — `cdl_silver.data_quality.customers_quarantine`

```sql
-- Rows quarantined in a specific run
SELECT * FROM cdl_silver.data_quality.customers_quarantine
WHERE _dq_run_id = '<run-id>';
```

### Watermark state

```sql
SELECT * FROM cdl_silver.watermark.pipeline_watermarks
WHERE pipeline_name = 'silver_customers_pipeline';
```

---

## 17. Troubleshooting

### Stage change not reflected in the Databricks UI

**Symptom:** Changed `run_data_quality: false` but the task still appears in the job.

**Cause:** `resources/silver_customers_job.yml` has not been regenerated and redeployed.

**Fix:**
```bash
python scripts/generate_job.py
databricks bundle deploy -t dev
```

---

### `Unable to access the notebook .../00_validate.py`

**Cause:** Databricks workspace notebook paths must NOT include the `.py` extension.

**Fix:** In any job YAML, use:
```yaml
notebook_path: ${workspace.file_path}/notebooks/stages/00_validate   # no .py
```

---

### `Cannot access source table: Table or view not found`

**Fix:**
1. Verify the 3-part name: `catalog.schema.table`
2. Grant access: `GRANT SELECT ON TABLE <table> TO <principal>`
3. Confirm the catalog: `SHOW CATALOGS`

---

### `load_strategy 'xyz' is invalid`

**Fix:** Use one of: `merge`, `append`, `full_refresh`, `scd2`

---

### `pipeline.merge.merge_keys is required`

**Fix:**
```yaml
merge:
  merge_keys: ["your_primary_key_column"]
```

---

### DQ pipeline halted — `DQPipelineException`

**Symptom:** Stage 2 fails with `PIPELINE HALTED: X% rows failed`.

**Investigation:**
```sql
SELECT * FROM cdl_silver.data_quality.customers_quarantine
WHERE _dq_run_id = '<run_id>';
```

**Options:**
- Raise the threshold: `failure_threshold_pct: 20.0`
- Run `full_refresh` after fixing source data

---

### Column masking or RLS not applying

**Cause 1:** Syntax error in `masking_sql`. Check the function SQL in `data_governance.yml`.

**Cause 2:** The `pii_admin` group does not exist in the Databricks account.

**Fix:** Create the group in the Databricks account console, then re-run Stage 3.

---

### `spark` / `dbutils` IDE warnings

These are Databricks runtime magic objects. They are always undefined in a local IDE. This is expected and not an error — notebooks run correctly on Databricks.

---

### Incremental load processing too many or too few rows

**Fix:**
```yaml
incremental_filter:
  watermark_type: "timestamp"   # must match actual column type
  lookback_days:  2             # increase to catch late-arriving data
```

**Reset watermark:**
```sql
DELETE FROM cdl_silver.watermark.pipeline_watermarks
WHERE pipeline_name = 'silver_customers_pipeline';
```
Then run with `run_mode=full_refresh`.
