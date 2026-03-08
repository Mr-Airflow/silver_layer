# Silver Layer Pipeline

CSV-driven Bronze → Silver pipeline on Databricks. All table definitions live in one config file — no code changes needed to add or modify a table.

---

## Project Structure

```
Silver_Layer/
├── config/
│   ├── parameter.yml          # Infrastructure locations (catalogs, schemas, tables)
│   ├── silver_config.csv      # One row per Silver table — the single source of truth
│   ├── dq/                    # Per-table DQ rule YAMLs
│   └── governance/            # Per-table governance YAMLs
├── notebooks/
│   └── stages/
│       ├── 00_validate.py     # Infra setup + source checks
│       ├── 01_transform.py    # Bronze read → SQL → Silver write
│       ├── 02_data_quality.py # Transform + DQ filter + Silver write (DQ-enabled tables)
│       ├── 03_governance.py   # Unity Catalog tags, masks, grants
│       ├── 04_audit.py        # Audit log records + mark processed=true
│       └── 05_optimize.py     # OPTIMIZE + VACUUM
├── resources/
│   └── silver_pipeline_job.yml  # Databricks job definition
├── scripts/
│   └── generate_job_yml.py    # Regenerates the job YAML from silver_config.csv
├── sql/                       # External SQL files for complex multi-table transforms
├── utils/
|   ├── config_loader.py       # Initial config load
|   ├── dq_engine.py           # DQX wrapper
|   ├── governance_engine.py   # Unity Catalog governance wrapper
|   ├── logger.py              # Logging wrapper
|   └── transform_engine.py    # Core pipeline engine
└── databricks.yml
```

---

## How It Works — End to End

### Pipeline stages

```
00_validate  →  01_transform  →  02_data_quality  →  03_governance  →  04_audit  →  05_optimize
  (always)        (always)        (if any DQ=true)   (if any Gov=true)  (always)    (if any Opt=true)
```

**Stage 0 — Validate**
Reads `parameter.yml`, creates all catalogs/schemas/tables that don't exist yet, writes `silver_config.csv` as a queryable Delta table in Unity Catalog, and verifies every Bronze source table is accessible. Fails immediately if any source is unreachable so the pipeline never starts a bad run.

**Stage 1 — Transform**
For each enabled row in the config table (rows where `processed=false`):
- Reads the full Bronze source table into memory
- Runs the SQL transform defined in `query` (or `sql_file` for multi-table joins). The source table is available as the `source` temp view in the SQL
- **If `run_data_quality=false`**: writes the transformed data directly to Silver using the configured `load_strategy`, then updates the control table
- **If `run_data_quality=true`**: skips the Silver write entirely and defers the table to Stage 2, which handles transform + DQ + write together

> **Incremental loads:** `load_type=incremental` is also supported — instead of reading the full source, the engine filters the source on a watermark column to only fetch rows newer than the last successful run. This is useful for large, append-only Bronze tables. For most use cases, `load_type=full_refresh` (full load) is the default.

**Stage 2 — Data Quality** *(only included in the job when at least one row has `run_data_quality=true`)*
For each DQ-enabled table, runs the complete pipeline from the beginning:
- Reads the full Bronze source table
- Runs the same SQL transform as Stage 1
- Applies DQ checks on the transformed data — rows that fail `error`-level checks are written to the quarantine table
- Writes **only the valid rows** to Silver
- Updates the control table after the write

This ensures data reaches Silver only after passing all quality checks — no intermediate Silver write happens in Stage 1 for these tables.

**Stage 3 — Governance** *(conditional: runs if at least one row has `run_governance=true`)*
Applies Unity Catalog ownership, table/column tags, PII column masking functions, and access grants for tables with `run_governance=true`.

**Stage 4 — Audit**
Writes one audit record per table to the pipeline run log. At the end of this stage, **`processed` is set to `true`** in the config Delta table for every row that completed the run. On the next pipeline execution, rows with `processed=true` are skipped entirely. To re-run a table, reset its `processed` column to `false` in the config Delta table.

**Stage 5 — Optimize** *(conditional: runs if at least one row has `run_optimize=true`)*
Runs `OPTIMIZE` on tables with `run_optimize=true`.

---

## Adding a New Table

Add one row to `silver_config.csv`, then regenerate the job and deploy:

```bash
python scripts/generate_job_yml.py
databricks bundle deploy
```

---

## silver_config.csv — Column Reference

| Column | Required | Description |
|---|---|---|
| `id` | Yes | Unique identifier for this transform row |
| `description` | No | Human-readable label |
| `source_catalog/schema/table` | Conditional | Bronze source. Leave blank if the SQL references tables directly (e.g. multi-table joins via `sql_file`) |
| `target_catalog/schema/table` | Yes | Silver destination |
| `pk` | Conditional | Primary key column(s), pipe-separated (`col1\|col2`). Required for `merge` strategy |
| `load_strategy` | Yes | `overwrite`, `append`, or `merge` — see table below |
| `load_type` | Yes | `full_refresh` (read entire source table) or `incremental` (filter by watermark column) |
| `watermark_column` | Conditional | Column used to filter new rows. Required when `load_type=incremental` |
| `query` | Conditional | Inline SQL transform. The Bronze table is exposed as the `source` temp view |
| `sql_file` | Conditional | Path to a `.sql` file for complex transforms (e.g. multi-table joins). Takes precedence over `query` |
| `run_optimize` | No | `true`/`false` — run OPTIMIZE after write |
| `run_data_quality` | No | `true`/`false` — run DQ checks; table is deferred to Stage 2 when enabled |
| `dq_config_path` | Conditional | Path to DQ YAML, required when `run_data_quality=true` |
| `run_governance` | No | `true`/`false` — apply UC tags, masks, and grants |
| `governance_config_path` | Conditional | Path to governance YAML, required when `run_governance=true` |
| `last_load_time` | No | Seed watermark timestamp used only on the first incremental run |
| `processed` | No | `true`/`false` — set to `true` by Stage 4 after a successful run; rows with `true` are skipped on the next run |

### Load strategies

| Strategy | Behaviour |
|---|---|
| `overwrite` | Drops and fully rewrites the Silver table on every run. No history is preserved |
| `append` | Appends transformed rows to Silver. Existing rows are never updated or deleted |
| `merge` | Upserts on `pk` — existing rows are updated, new rows are inserted |

---

## Transform — Detailed Flow & Edge Cases

The transform for each table follows this exact sequence:

```
1. Read full Bronze source table
2. Register as "source" temp view
3. Execute SQL transform (query / sql_file)
4. Count result rows
   ├── 0 rows → log SUCCESS (rows_loaded=0), skip Silver write, done
   └── >0 rows → continue
5. run_data_quality=false → write to Silver (load_strategy)
   run_data_quality=true  → defer; Stage 2 handles DQ + write
6. Update watermark state
7. Update control table (last_run_status, rows_loaded, last_run_ts)
8. Write migration log record
```

### Edge cases

**`processed=true` — row is skipped**
After a successful pipeline run, Stage 4 sets `processed=true` for every completed row. On the next run, Stage 1 sees `processed=true` and skips that row immediately — no Bronze read, no transform, no write. This prevents duplicate loads.
To re-run a specific table: update `processed` to `false` in the config Delta table (`cdl_silver.config_schema.silver_config_table`) and trigger the pipeline again.

**No source table — SQL-only transform**
If `source_catalog`, `source_schema`, and `source_table` are all blank, no source table is read and no `source` temp view is created. The SQL in `sql_file` is expected to reference all tables directly (e.g. multi-catalog joins). This is the correct pattern for derived tables like `cust_orders`.

**Multi-column primary key**
Pipe-separate the column names in the `pk` field: `order_id|line_id`. All listed columns are used together in the merge condition. Required when `load_strategy=merge`.

**0 rows after transform**
If the SQL transform produces 0 rows (e.g. source table is empty or all rows are filtered out), the Silver write is skipped. The run is recorded as `SUCCESS` with `rows_loaded=0`. The table is not written or altered.

**0 rows after DQ filter (DQ-enabled tables)**
If all transformed rows fail DQ checks and are quarantined, the Silver write is skipped. The run is still recorded as `SUCCESS` (the pipeline itself did not error), but `rows_loaded=0` and the quarantine table will contain the rejected rows.

**`run_data_quality=true` — full deferral to Stage 2**
Stage 1 does not read Bronze, does not transform, and does not write Silver for this table. Everything — Bronze read, transform, DQ filter, Silver write — happens in Stage 2. This guarantees data only reaches Silver after passing quality checks. Stage 1 logs the row as `SKIPPED (deferred to DQ stage)`.

**`run_data_quality=false` — no DQ at any stage**
Stage 1 transforms and writes directly to Silver. Stage 2 skips this table entirely. There is no DQ filtering — all transformed rows reach Silver.

**Schema change in `silver_config.csv`**
Stage 0 detects if the columns in `silver_config.csv` differ from the existing Delta config table. If a column is added or removed, it drops and recreates the config table with `overwriteSchema=true` to apply the new schema before any transforms run.

**Inline SQL (`query`) vs external file (`sql_file`)**
If both are set, `sql_file` takes precedence. If neither is set, the engine falls back to `SELECT * FROM source`. For simple column-level transforms use `query`; for multi-table joins or long SQL, use `sql_file` under the `sql/` directory.

**Incremental load**
When `load_type=incremental`, the engine reads the last successful watermark from the control table and filters the source on `watermark_column > last_watermark` before running the transform. On the very first run there is no watermark, so the full source table is loaded. After each successful write, the max value of `watermark_column` in the written data becomes the new watermark.

---

## Data Quality (DQ)

DQ uses the [Databricks DQX framework](https://github.com/databrickslabs/dqx). Rules are defined per table in a YAML file under `config/dq/`.

**What it does:**
- Runs checks against the transformed DataFrame before any rows reach Silver
- Rows failing an `error`-level check are written to the per-table quarantine table
- Rows flagged by a `warn`-level check are passed through to Silver but flagged in the DQ results table
- Only rows that pass all `error`-level checks are written to Silver

### Adding DQ rules for a new table

1. Create `config/dq/<your_id>.yml`:

```yaml
data_quality:

  quarantine_table: "cdl_silver.data_quality.<table>_quarantine"
  dq_results_table: "cdl_silver.data_quality.dq_results_<table>"

  failure_threshold_pct: 5.0   # fail the run if >5% of rows are quarantined

  checks:

    - name: "order_id_not_null"
      criticality: "error"        # error = quarantine the row; warn = flag only
      user_metadata:
        check_category: "completeness"
        check_owner: "your.email@company.com"
      check:
        function: is_not_null
        arguments:
          column: order_id

    - name: "amount_positive"
      criticality: "warn"
      check:
        function: is_not_less_than
        arguments:
          column: amount
          value: 0
```

2. In `silver_config.csv`, set:
   - `run_data_quality=true`
   - `dq_config_path=config/dq/<your_id>.yml`

---

## Data Governance (DG)

Governance uses the Databricks Unity Catalog Python SDK (`databricks-sdk`). Rules are defined per table in a YAML file under `config/governance/`.

**What it does:**
- Sets table ownership and data steward
- Applies table-level tags (domain, classification, PII flag, SLA tier, etc.)
- Tags individual columns and attaches PII masking functions
- Grants `SELECT` / `ALL_PRIVILEGES` to specified users or groups

### Adding governance for a new table

1. Create `config/governance/<your_id>.yml`:

```yaml
governance:

  table_owner:  "owner@company.com"
  data_steward: "steward@company.com"

  table_tags:
    domain:              "your_domain"
    data_classification: "internal"    # internal | confidential | restricted
    pii_present:         "false"
    layer:               "silver"
    env:                 "prod"

  pii_columns:               # omit this section if the table has no PII columns
    - column: "email"
      pii_type: "EMAIL"
      sensitivity: "high"
      abac_tag_key: "pii"
      abac_tag_value: "email"
      masking_function: "mask_email_<table>"
      masking_sql: |
        CREATE OR REPLACE FUNCTION cdl_silver.governance.mask_email_<table>(email STRING)
        RETURN CASE
            WHEN current_user() = 'admin@company.com' THEN email
            ELSE SPLIT(email, '@')[0] || '@***.***'
        END

  column_tags:
    - column: "id"
      tags:
        is_pk: "true"

  grants:
    - principal:      "analyst@company.com"
      privileges:     ["SELECT"]
      principal_type: "user"

  audit:
    enabled:     true
    audit_table: "cdl_silver.logging.silver_audit_log"
```

2. In `silver_config.csv`, set:
   - `run_governance=true`
   - `governance_config_path=config/governance/<your_id>.yml`

---

## Deploying Changes

After any change to `silver_config.csv`:

```bash
# Regenerate the Databricks job YAML (adds/removes stage tasks based on flags)
python scripts/generate_job_yml.py

# Deploy to Databricks
databricks bundle deploy
```

The job YAML is auto-generated and should not be edited manually.
