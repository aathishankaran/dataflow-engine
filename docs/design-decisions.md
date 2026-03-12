# Design Decisions

This page documents key architectural decisions and the rationale behind them, along with the config versioning strategy.

---

## Decision 1: Previous Business Day Skips Holidays Only

**Context:** The engine validates header dates in input files against the expected previous business day.

**Decision:** The previous business day calculation skips **only** configured holidays (from `usa_holidays` in `settings.json`). Weekends are treated as valid business days.

**Rationale:** Mainframe batch jobs often run on weekends. Weekend processing is normal in financial data pipelines, so Saturday and Sunday are valid business days. Only bank holidays and market closures need to be skipped.

**Example:**

| Today | Previous Business Day |
|-------|----------------------|
| Monday March 9 | Sunday March 8 |
| Sunday March 8 | Saturday March 7 |
| Thursday July 5 | Wednesday July 3 (July 4 is a holiday) |

!!! warning "Holiday configuration"
    Holidays must be maintained in `settings.json` under the `usa_holidays` key.
    Missing holidays will cause incorrect date validation and false-positive incidents.

---

## Decision 2: Select Columns/Expressions Mutual Exclusivity

**Context:** The `select` transformation type supports two modes: column projection and expression evaluation.

**Decision:** A `select` step must use **either** `columns` **or** `expressions`, never both simultaneously.

**Rationale:** Mixing projection and computation in a single step creates ambiguity about column ordering and naming. Separating the two modes keeps each step's intent clear and debuggable. If both column selection and expression evaluation are needed, use two sequential steps.

---

## Decision 3: No Spark SQL

**Context:** PySpark supports both the DataFrame API and Spark SQL for data manipulation.

**Decision:** All transformations use the PySpark DataFrame API exclusively. No Spark SQL queries are used.

**Rationale:**

- DataFrame API calls are composable and type-safe
- Easier to generate programmatically from JSON configuration
- No need to manage temporary views or SQL namespaces
- Better error messages when column names or types are wrong
- Simpler unit testing (mock DataFrames, not SQL strings)

---

## Decision 4: Registry Pattern for DataFrame Sharing

**Context:** Transformation steps need to read from and write to named DataFrames.

**Decision:** Use a shared dictionary (registry) keyed by DataFrame name. Each step reads inputs from the registry and writes results back.

**Rationale:**

- Simple and explicit data flow
- Steps can reference any previously created DataFrame by name
- Join steps can reference two different DataFrames
- No hidden state or implicit data passing
- Easy to inspect the registry at any point for debugging

```text
Registry = {
  "raw_accounts": DF1,        # From input loading
  "filtered": DF2,             # From filter step
  "enriched": DF3,             # From join step
  "final": DF4                 # From select step
}
```

---

## Decision 5: Credentials Never in Config

**Context:** Oracle database connections require username and password.

**Decision:** Credentials are **never** stored in the config JSON. They are fetched from HashiCorp Vault at runtime using the `vault_path` config field.

**Rationale:**

- Config files are stored in version control
- Credentials in config would be a security vulnerability
- Vault provides audit logging and secret rotation
- Different environments (dev/staging/prod) use different Vault paths
- AppRole authentication allows automated pipelines without human tokens

---

## Decision 6: Control File Directory Structure

**Context:** Transformation steps (especially validation) produce control files with metadata.

**Decision:** Control files are written to a nested directory structure: `ctrl/<step_id>/<frequency>/<date>/<name>.CTL`

**Rationale:**

- Step ID isolates control files per transformation step
- Frequency (daily, weekly, monthly) supports different processing cadences
- Date partitioning prevents overwrites and enables historical lookups
- The engine uses `rglob("*")` for recursive discovery

!!! note "Discovery pattern"
    Use `rglob("*")` (not `iterdir()`) to find control files, since they
    are nested several directories deep.

---

## Config Versioning

The dataflow engine supports three config schema versions for file path resolution. This allows gradual migration from legacy configurations.

### V1: Source Path + File Name

Paths are specified as separate `source_path` and `source_file_name` fields.

```json
{
  "source_path": "/data/inputs/daily",
  "source_file_name": "accounts.dat"
}
```

**Resolved path:** `/data/inputs/daily/accounts.dat`

### V2: Dataset Name with Bucket Prefixes

Uses `dataset_name` with bucket prefixes defined in `settings.json`.

```json
{
  "dataset_name": "accounts/daily/accounts.dat"
}
```

The engine prepends the appropriate bucket prefix from settings:

```json
{
  "input_bucket": "s3://my-bucket/inputs",
  "output_bucket": "s3://my-bucket/outputs"
}
```

**Resolved path:** `s3://my-bucket/inputs/accounts/daily/accounts.dat`

### Legacy: Direct Path

Uses `path`, `s3_path`, or `dataset` fields directly.

```json
{
  "path": "/data/inputs/accounts.dat"
}
```

**Resolved path:** `/data/inputs/accounts.dat` (used as-is)

### Version Comparison

| Feature | V1 | V2 | Legacy |
|---------|----|----|--------|
| Path fields | `source_path` + `source_file_name` | `dataset_name` | `path` / `s3_path` / `dataset` |
| Bucket support | No | Yes (from settings) | Manual (in path) |
| Date partitioning | Manual | Automatic | Manual |
| S3 support | Manual | Built-in | Manual |
| Recommended | No | Yes | No |

### Migration Guidance

!!! tip "Migrating to V2"
    New configurations should use the V2 schema with `dataset_name`.
    V1 and Legacy schemas are supported for backward compatibility but
    may be deprecated in future versions.

    **Steps to migrate:**

    1. Define `input_bucket` and `output_bucket` in `settings.json`
    2. Replace `source_path` + `source_file_name` with `dataset_name`
    3. Remove any hardcoded S3 prefixes from paths
    4. Test with `--dry-run` to verify path resolution
