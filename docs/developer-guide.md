# Developer Guide

Comprehensive reference for developers working on or extending the dataflow engine.

---

## Architecture

The dataflow engine is a **config-driven PySpark pipeline** that executes three phases:

1. **Load Inputs** -- Read files (fixed-width, CSV, Parquet, delimited) into Spark DataFrames
2. **Run Transformations** -- Apply a sequence of transformation steps (filter, select, join, etc.)
3. **Write Outputs** -- Write DataFrames to files with optional header/trailer rows and control files

All behavior is controlled by a JSON configuration file. No code changes are needed to define new data flows.

```text
config.json --> config_loader --> DataFlowRunner
                                    |
                                    +-- load_inputs()
                                    +-- run_transformations()
                                    +-- write_outputs()
```

---

## Project Structure

```text
dataflow-engine/
  run_dataflow.py                    # CLI entry point
  requirements.txt                   # Python dependencies
  settings.json                      # Runtime settings (holidays, buckets)
  dataflow_engine/
    __init__.py
    config_loader.py                 # Config parsing, path resolution, date logic
    runner.py                        # DataFlowRunner pipeline orchestrator
    transformations.py               # 8 transformation types + validation
    oracle_loader.py                 # Oracle SQL*Loader integration
    vault.py                         # HashiCorp Vault credential access
    incident.py                      # Moogsoft incident connector
  samples/
    config.json                      # Example pipeline configuration
  tests/
    test_config_loader.py
    test_runner.py
    test_transformations.py
    test_oracle_loader.py
    test_vault.py
    test_incident.py
  docs/                              # This documentation site
```

---

## Module Reference

### run_dataflow.py

CLI entry point for the dataflow engine. Handles argument parsing, SparkSession creation, and pipeline orchestration.

**Key functions:**

| Function | Description |
|----------|-------------|
| `main()` | Entry point: parse args, build Spark, run pipeline |
| `_build_spark_session()` | Create OS-aware SparkSession (macOS/Linux/Windows) |
| `_error_messages()` | Format and collect error messages from pipeline execution |

**SparkSession configuration:**

- Detects operating system for platform-specific Spark settings
- Configures `local[*]` master by default (overridable via `--master`)
- Optionally enables Cobrix for COBOL copybook reading

---

### config_loader.py

Loads, validates, and normalizes the JSON configuration. Resolves file paths using V1, V2, or Legacy schema conventions.

**Key functions:**

| Function | Description |
|----------|-------------|
| `load_config()` | Parse JSON, normalize keys, resolve paths |
| `get_input_path()` | Resolve input file path (V1/V2/Legacy) |
| `get_output_path()` | Resolve output file path with date partitioning |
| `get_control_file_path()` | Build control file output path |
| `get_previous_business_day()` | Calculate previous business day (holidays only, not weekends) |

**Path resolution strategies:**

- **V1**: `source_path` + `source_file_name`
- **V2**: `dataset_name` with bucket prefixes from `settings.json`
- **Legacy**: Direct `path`, `s3_path`, or `dataset` fields

!!! note "Previous business day logic"
    The previous business day calculation skips **only** configured holidays
    (from `usa_holidays` in settings). Weekends are treated as valid business days.

---

### runner.py

`DataFlowRunner` is the main pipeline orchestrator. Manages the DataFrame registry, executes transformation steps, and writes output files.

**Class: `DataFlowRunner`**

| Method | Description |
|--------|-------------|
| `load_inputs()` | Read input files into named DataFrames |
| `run_transformations()` | Execute transformation steps sequentially |
| `write_outputs()` | Write DataFrames to output files |

**Input loading features:**

- Fixed-width parsing with positional field extraction
- Header and trailer row separation
- CSV, Parquet, and delimited format support
- Previous business day header date validation

**Output writing features:**

- Fixed-width formatting with field padding to defined lengths
- Header and trailer row insertion
- Control file generation
- Column selection and ordering

---

### transformations.py

`MainframeTransformationExecutor` implements all 8 transformation types using the PySpark DataFrame API (no Spark SQL).

**Class: `MainframeTransformationExecutor`**

| Transformation | Description |
|----------------|-------------|
| `filter` | Row filtering with conditions |
| `select` | Column projection or expression evaluation |
| `join` | DataFrame join operations |
| `aggregate` | Group-by with aggregation functions |
| `union` | Union multiple DataFrames |
| `validate` | Data validation with FLAG/DROP/ABORT modes |
| `custom` | Sort and merge operations |
| `oracle_write` | Oracle SQL*Loader bulk loading |

**Validation modes:**

| Mode | Behavior |
|------|----------|
| `FLAG` | Add a validation flag column; keep all rows |
| `DROP` | Remove rows that fail validation |
| `ABORT` | Raise an error and halt the pipeline on failure |

**Control file creation:**

Control files are written to `ctrl/<step_id>/<frequency>/<date>/<name>.CTL` and contain record counts, timestamps, and validation summaries.

---

### oracle_loader.py

Integrates with Oracle SQL*Loader for bulk database loading. Orchestrates the full flow: Vault credential fetch, CSV export, CTL file generation, and `sqlldr` execution.

**Key functions:**

| Function | Description |
|----------|-------------|
| `write_df_to_oracle()` | Full orchestration: export, generate CTL, run sqlldr |
| `generate_ctl_content()` | Build SQL*Loader control file content |

**Spark-to-SQLLoader type mapping:**

| Spark Type | SQL*Loader Type |
|------------|----------------|
| `StringType` | `CHAR` |
| `IntegerType` | `INTEGER EXTERNAL` |
| `LongType` | `INTEGER EXTERNAL` |
| `DoubleType` | `DECIMAL EXTERNAL` |
| `DecimalType` | `DECIMAL EXTERNAL` |
| `DateType` | `DATE "YYYY-MM-DD"` |
| `TimestampType` | `TIMESTAMP "YYYY-MM-DD HH24:MI:SS"` |

---

### vault.py

Fetches Oracle database credentials from HashiCorp Vault at runtime. Supports both token-based and AppRole authentication with KV v2/v1 fallback.

**Key functions:**

| Function | Description |
|----------|-------------|
| `get_oracle_credentials()` | Read username/password from Vault path |
| `store_oracle_credentials()` | Write credentials to Vault (for setup) |

**Authentication methods:**

1. **Token-based**: Uses `VAULT_TOKEN` environment variable
2. **AppRole**: Uses `VAULT_ROLE_ID` and `VAULT_SECRET_ID` environment variables

!!! tip "Security"
    Credentials are never stored in the config JSON. They are always fetched
    from Vault at runtime, ensuring secrets do not leak into version control.

---

### incident.py

`MoogsoftIncidentConnector` creates automated incidents in Moogsoft (or ServiceNow) when pipeline errors occur.

**Class: `MoogsoftIncidentConnector`**

| Method | Description |
|--------|-------------|
| `create_missing_file_incident()` | Input file not found |
| `create_date_mismatch_incident()` | Header date does not match expected date |
| `create_validation_failure_incident()` | Validation rules failed |
| `create_record_count_incident()` | Record count mismatch |
| `create_oracle_load_incident()` | SQL*Loader execution failed |
| `create_generic_incident()` | General pipeline error |

**Severity levels:** `critical`, `major`, `minor`, `warning`

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| PySpark | `>=3.0.0,<3.2` | Distributed DataFrame processing |
| Boto3 | Latest | AWS S3 file access |
| HVAC | Latest | HashiCorp Vault client library |
| Requests | Latest | HTTP client for REST APIs |
| PyArrow | Latest | Apache Parquet support |
