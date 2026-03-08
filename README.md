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
│       ├── 04_audit.py        # Audit log records
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
└──databricks.yml

```

---

## How It Works — End to End

### Pipeline stages

```
00_validate  →  01_transform  →  02_data_quality  →  03_governance  →  04_audit  →  05_optimize
  (always)        (always)        (if any DQ=true)   (if any Gov=true)  (always)    (if any Opt=true)
```

**Stage 0 — Validate**
Reads `parameter.yml`, creates all catalogs/schemas/tables that don't exist yet, writes `silver_config.csv` as a Delta table in Unity Catalog, and checks every source table is accessible. Fails fast if any source is unreachable.

**Stage 1 — Transform**
For each row in the config table:
- Reads the Bronze source table
- Applies the incremental watermark filter (if `load_type=incremental`)
- Runs the SQL transform (inline `query` or external `sql_file`)
- **If `run_data_quality=true`: defers the table entirely to Stage 2** (no Silver write in Stage 1)
- **If `run_data_quality=false`: writes directly to Silver** using the configured `load_strategy`
- Updates the watermark and control table

**Stage 2 — Data Quality** *(only included in the job when at least one row has `run_data_quality=true`)*
For each DQ-enabled table, runs the full pipeline from scratch:
- Bronze read → incremental filter → SQL transform → DQ filter → Silver write
- Bad rows are diverted to the quarantine table; **only valid rows reach Silver**
- Watermark and control table are updated after the write

**Stage 3 — Governance** *(conditional)*
Applies Unity Catalog ownership, table/column tags, PII column masking functions, and access grants for tables with `run_governance=true`.

**Stage 4 — Audit**
Writes one audit record per table to the pipeline run log and marks `processed=true` in the config Delta table.

**Stage 5 — Optimize** *(conditional)*
Runs `OPTIMIZE` on tables with `run_optimize=true`.

---

## Adding a New Table

Add one row to `silver_config.csv`:

```csv
id,             description,      source_catalog, source_schema, source_table,
target_catalog, target_schema,    target_table,   pk,            load_strategy,
load_type,      watermark_column, query,          sql_file,      run_optimize,
run_data_quality, dq_config_path, run_governance, governance_config_path,
last_load_time,   processed
```

Then regenerate the job and deploy:

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
| `source_catalog/schema/table` | Conditional | Bronze source. Leave blank if the SQL references tables directly |
| `target_catalog/schema/table` | Yes | Silver destination |
| `pk` | Conditional | Primary key column(s), pipe-separated (`col1\|col2`). Required for `merge` strategy |
| `load_strategy` | Yes | `overwrite`, `append`, or `merge` |
| `load_type` | Yes | `full_refresh` (ignore watermark) or `incremental` (apply watermark filter) |
| `watermark_column` | Conditional | Required when `load_type=incremental` |
| `query` | Conditional | Inline SQL. Use `source` as the temp view alias for the source table |
| `sql_file` | Conditional | Path to a `.sql` file relative to the repo root (for complex multi-table joins) |
| `run_optimize` | No | `true`/`false` — run OPTIMIZE after write |
| `run_data_quality` | No | `true`/`false` — run DQ checks before writing to Silver |
| `dq_config_path` | Conditional | Path to DQ YAML, required when `run_data_quality=true` |
| `run_governance` | No | `true`/`false` — apply UC tags, masks, and grants |
| `governance_config_path` | Conditional | Path to governance YAML, required when `run_governance=true` |
| `last_load_time` | No | Seed watermark for the first incremental run |
| `processed` | No | `true`/`false` — skip this row on next run (set by audit stage) |

### Load strategies

| Strategy | Behaviour |
|---|---|
| `overwrite` | Drops and rewrites the entire Silver table on every run |
| `append` | Appends new rows; never deletes or updates |
| `merge` | Upserts on `pk` columns — matched rows updated, new rows inserted |

### Transform edge cases

- **No source table** — leave `source_catalog/schema/table` blank and write a `sql_file` that joins across catalogs directly. The `source` temp view is not created.
- **Multi-column PK** — use pipe-separation: `pk=order_id|line_id`. Required for `merge`.
- **0 rows after transform** — the Silver write is skipped and the run is logged as `SUCCESS` with `rows_loaded=0`.
- **0 rows after DQ filter** — same as above; all rows went to quarantine, Silver write is skipped.
- **`processed=true`** — the row is skipped entirely on the next run. Reset to `false` to re-run.
- **DQ-enabled table** — the entire row is deferred by Stage 1 and handled exclusively by Stage 2 (transform + DQ + write happen together).
- **Schema change in config** — if columns are added/removed from `silver_config.csv`, Stage 0 detects the schema change and recreates the Delta config table with `overwriteSchema=true`.
- **Missing watermark** — on the first incremental run there is no previous watermark, so the full source table is loaded.

---

## Data Quality (DQ)

DQ uses the [Databricks DQX framework](https://github.com/databrickslabs/dqx). Rules are defined per table in a YAML file under `config/dq/`.

**What it does:**
- Runs checks on the transformed DataFrame before any rows reach Silver
- Rows failing an `error`-level check are written to the per-table quarantine table
- Only rows that pass all checks are written to Silver
- Results are written to the DQ results table for auditing

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
- Sets table ownership
- Applies table-level and column-level tags (ABAC-compatible)
- Creates PII column masking functions and attaches them to columns
- Grants `SELECT` / `ALL_PRIVILEGES` to specified principals

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

  pii_columns:               # omit section if no PII columns
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
