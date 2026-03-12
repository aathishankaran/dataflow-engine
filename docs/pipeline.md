# Pipeline Flow

The dataflow engine executes a three-phase pipeline controlled entirely by JSON configuration.

---

## Execution Overview

```text
CLI (run_dataflow.py)
  |
  +-> argparse (config path, base-path, master, dry-run, settings)
  |
  +-> _build_spark_session()  (OS-aware SparkSession)
  |
  +-> DataFlowRunner
        |
        +-> load_config()           -- Parse JSON, normalize keys
        |
        +-> load_inputs()           -- Read files into DataFrames
        |
        +-> run_transformations()   -- Apply steps via MainframeTransformationExecutor
        |
        +-> write_outputs()         -- Write DataFrames to files
```

---

## Phase 1: Input Loading

The `load_inputs()` method reads each configured input file into a named Spark DataFrame and registers it in the DataFrame registry.

### Supported Formats

| Format | Description | Key Options |
|--------|-------------|-------------|
| `FIXED` | Fixed-width positional fields | `fields` with `start`, `length` |
| `CSV` | Comma-separated values | `delimiter`, `header` |
| `DELIMITED` | Custom delimiter | `delimiter`, `quote_char` |
| `PARQUET` | Apache Parquet columnar | Automatic schema inference |
| `COBOL` | COBOL copybook via Cobrix | `copybook_path` |

### Header and Trailer Handling

For fixed-width files, the engine separates header and trailer rows from the data:

```text
+------------------+
| Header Row(s)    |  <-- Extracted, validated separately
+------------------+
| Data Row 1       |
| Data Row 2       |  <-- Parsed into DataFrame
| ...              |
+------------------+
| Trailer Row(s)   |  <-- Extracted, validated separately
+------------------+
```

- **Header rows**: Configurable count via `header_count` (default: 0)
- **Trailer rows**: Configurable count via `trailer_count` (default: 0)
- Header and trailer rows are stored separately for validation and output reconstruction

### Previous Business Day Check

When enabled, the engine validates that the header date matches the expected previous business day:

1. Read the date field from the header row
2. Calculate the expected previous business day (skipping holidays only)
3. Compare the two dates
4. On mismatch: create a Moogsoft incident and raise `RuntimeError`

!!! warning "Weekend handling"
    The previous business day calculation skips **only** configured holidays.
    Weekends are **not** skipped. For example, if today is Sunday March 9,
    the previous business day is Saturday March 8.

---

## Phase 2: Transformation Execution

The `run_transformations()` method applies each transformation step sequentially using `MainframeTransformationExecutor`.

### Registry Pattern

DataFrames are stored in a shared registry (dictionary) keyed by name. Each transformation step:

1. Reads its input DataFrame(s) from the registry
2. Applies the transformation logic
3. Writes the result back to the registry under the step's output name

```text
Registry: { "input_file" -> DF1 }
    |
    +-- Step 1 (filter) --> { "input_file" -> DF1, "filtered" -> DF2 }
    |
    +-- Step 2 (select) --> { ..., "selected" -> DF3 }
    |
    +-- Step 3 (join)   --> { ..., "joined" -> DF4 }
```

### Transformation Types

All 8 types are documented in [Transformation Types](transformations-guide.md):

| Type | Phase Role |
|------|-----------|
| `filter` | Row reduction |
| `select` | Column projection or computation |
| `join` | DataFrame combination by key |
| `aggregate` | Grouped summaries |
| `union` | Row-wise combination |
| `validate` | Data quality checks |
| `custom` | Sort and merge |
| `oracle_write` | Database loading |

---

## Phase 3: Output Writing

The `write_outputs()` method writes DataFrames from the registry to output files.

### Fixed-Width Formatting

For `FIXED` format outputs, each field is padded to its defined length:

```text
Field: name (start=1, length=20)
Value: "SMITH"
Output: "SMITH               "  (padded to 20 characters)
```

All fields are concatenated positionally to form a single fixed-width row.

### Header and Trailer Insertion

Output files can include header and trailer rows:

```text
+------------------+
| Header Row       |  <-- From header_fields schema
+------------------+
| Data Row 1       |
| Data Row 2       |  <-- From DataFrame + fields schema
+------------------+
| Trailer Row      |  <-- From trailer_fields schema
+------------------+
```

### Control File Generation

Control files are written alongside output files to provide processing metadata:

- **Path structure**: `ctrl/<step_id>/<frequency>/<date>/<name>.CTL`
- **Contents**: Record counts, timestamps, validation results

!!! note "Control file discovery"
    Control files are stored in a nested directory structure. The engine uses
    `rglob("*")` (not `iterdir()`) to discover control files recursively.

### Column Selection

Output writing supports explicit column selection and ordering. Only the columns specified in the output configuration are written, in the defined order.

---

## Error Handling

Errors at any phase trigger:

1. **Logging**: Detailed error messages with stack traces
2. **Incidents**: Moogsoft incident creation (when configured)
3. **Exit code**: Non-zero exit code (1) for pipeline failures

| Error Type | Phase | Response |
|-----------|-------|----------|
| Missing input file | Load | Incident + RuntimeError |
| Date mismatch | Load | Incident + RuntimeError |
| Validation failure (ABORT) | Transform | Incident + RuntimeError |
| Oracle load failure | Transform | Incident + RuntimeError |
| Output write failure | Write | Incident + RuntimeError |
