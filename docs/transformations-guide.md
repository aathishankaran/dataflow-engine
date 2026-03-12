# Transformation Types

The dataflow engine supports 8 transformation types, all implemented using the PySpark DataFrame API (no Spark SQL). Each transformation is configured as a step in the pipeline JSON.

---

## Summary

| Type | Description | Key Config Fields |
|------|-------------|-------------------|
| `filter` | Row filtering by conditions | `conditions` |
| `select` | Column projection OR expression evaluation | `columns` or `expressions` |
| `join` | DataFrame join | `left`, `right`, `on`, `how` |
| `aggregate` | Group-by with aggregations | `group_by`, `aggregations` |
| `union` | Union multiple DataFrames | `source_inputs` |
| `validate` | Data quality validation | `rules`, `mode` |
| `custom` | Sort and merge operations | `operation`, `key` |
| `oracle_write` | Oracle SQL*Loader bulk load | `host`, `port`, `service_name`, `table`, `vault_path` |

---

## filter

Filters rows based on one or more conditions. Conditions are combined with AND logic by default.

**Config fields:**

| Field | Type | Description |
|-------|------|-------------|
| `conditions` | array | List of condition objects |
| `conditions[].field` | string | Column name to filter on |
| `conditions[].operation` | string | Comparison operator (`equals`, `not_equals`, `greater_than`, `less_than`, `contains`, `in`, `not_in`, `is_null`, `is_not_null`) |
| `conditions[].value` | any | Value to compare against |

**Example:**

```json
{
  "type": "filter",
  "name": "active_accounts",
  "input": "raw_accounts",
  "conditions": [
    { "field": "status", "operation": "equals", "value": "ACTIVE" },
    { "field": "balance", "operation": "greater_than", "value": 0 }
  ]
}
```

---

## select

Projects columns or evaluates expressions. These two modes are **mutually exclusive**.

!!! warning "Mutual exclusivity"
    A `select` step must use **either** `columns` **or** `expressions`, never both.
    If both are provided, the behavior is undefined.

### Column projection mode

Select and reorder specific columns from the DataFrame.

**Config fields:**

| Field | Type | Description |
|-------|------|-------------|
| `columns` | array | List of column names to select |

**Example:**

```json
{
  "type": "select",
  "name": "projected",
  "input": "raw_data",
  "columns": ["account_id", "name", "balance"]
}
```

### Expression mode

Evaluate expressions to create new or transformed columns. Uses COBOL-style keywords for operations.

**Config fields:**

| Field | Type | Description |
|-------|------|-------------|
| `expressions` | array | List of expression objects |
| `expressions[].output_field` | string | Name of the result column |
| `expressions[].operation` | string | Operation keyword (`ADD`, `SUBTRACT`, `MULTIPLY`, `DIVIDE`, `MOVE`, `COMPUTE`, `STRING`, `UNSTRING`, `INSPECT`) |
| `expressions[].operands` | array | Input fields or literal values |

**Example:**

```json
{
  "type": "select",
  "name": "computed",
  "input": "raw_data",
  "expressions": [
    {
      "output_field": "total",
      "operation": "ADD",
      "operands": ["amount1", "amount2"]
    },
    {
      "output_field": "full_name",
      "operation": "STRING",
      "operands": ["first_name", "' '", "last_name"]
    }
  ]
}
```

---

## join

Joins two DataFrames on one or more key columns.

**Config fields:**

| Field | Type | Description |
|-------|------|-------------|
| `left` | string | Name of the left DataFrame |
| `right` | string | Name of the right DataFrame |
| `on` | array | Join key column(s) |
| `how` | string | Join type (`inner`, `left`, `right`, `outer`, `cross`) |

**Example:**

```json
{
  "type": "join",
  "name": "enriched_accounts",
  "left": "accounts",
  "right": "customers",
  "on": ["customer_id"],
  "how": "inner"
}
```

---

## aggregate

Groups rows and applies aggregation functions.

**Config fields:**

| Field | Type | Description |
|-------|------|-------------|
| `group_by` | array | Columns to group by |
| `aggregations` | array | List of aggregation objects |
| `aggregations[].field` | string | Column to aggregate |
| `aggregations[].function` | string | Aggregation function (`sum`, `count`, `avg`, `min`, `max`) |
| `aggregations[].alias` | string | Output column name |

**Example:**

```json
{
  "type": "aggregate",
  "name": "account_totals",
  "input": "transactions",
  "group_by": ["account_id"],
  "aggregations": [
    { "field": "amount", "function": "sum", "alias": "total_amount" },
    { "field": "amount", "function": "count", "alias": "txn_count" }
  ]
}
```

---

## union

Combines rows from multiple DataFrames into one.

**Config fields:**

| Field | Type | Description |
|-------|------|-------------|
| `source_inputs` | array | Names of DataFrames to union |

**Example:**

```json
{
  "type": "union",
  "name": "all_transactions",
  "source_inputs": ["checking_txns", "savings_txns", "credit_txns"]
}
```

!!! note "Schema alignment"
    All source DataFrames must have the same column names and types for the union
    to succeed. Column ordering is preserved from the first source.

---

## validate

Applies data quality rules and handles failures according to the configured mode.

**Config fields:**

| Field | Type | Description |
|-------|------|-------------|
| `rules` | array | List of validation rule objects |
| `rules[].field` | string | Column to validate |
| `rules[].check` | string | Validation type (`not_null`, `in_range`, `regex`, `length`, `in_list`) |
| `rules[].params` | object | Parameters for the check |
| `mode` | string | Failure handling: `FLAG`, `DROP`, or `ABORT` |

**Validation modes:**

| Mode | Behavior |
|------|----------|
| `FLAG` | Add a `_validation_flag` column; keep all rows |
| `DROP` | Remove rows that fail any validation rule |
| `ABORT` | Raise `RuntimeError` and halt the pipeline |

**Example:**

```json
{
  "type": "validate",
  "name": "validated_accounts",
  "input": "raw_accounts",
  "mode": "DROP",
  "rules": [
    { "field": "account_id", "check": "not_null" },
    { "field": "balance", "check": "in_range", "params": { "min": 0, "max": 999999999 } },
    { "field": "status", "check": "in_list", "params": { "values": ["ACTIVE", "CLOSED", "FROZEN"] } }
  ]
}
```

**Control file output:**

Validation steps can generate control files containing record counts and validation summaries. These are written to `ctrl/<step_id>/<frequency>/<date>/<name>.CTL`.

**Previous business day validation:**

Validate steps can also check header dates against the expected previous business day, creating Moogsoft incidents on mismatch.

---

## custom

Performs sort and merge operations on DataFrames.

**Config fields:**

| Field | Type | Description |
|-------|------|-------------|
| `operation` | string | Operation type (`sort`, `merge`) |
| `key` | array | Column(s) to sort or merge on |
| `ascending` | boolean | Sort direction (default: `true`) |

**Example (sort):**

```json
{
  "type": "custom",
  "name": "sorted_transactions",
  "input": "transactions",
  "operation": "sort",
  "key": ["transaction_date", "amount"],
  "ascending": false
}
```

---

## oracle_write

Loads a DataFrame into an Oracle database table using SQL*Loader.

**Config fields:**

| Field | Type | Description |
|-------|------|-------------|
| `host` | string | Oracle database host |
| `port` | integer | Oracle database port |
| `service_name` | string | Oracle service name |
| `table` | string | Target table name |
| `vault_path` | string | Vault path for credentials |
| `load_mode` | string | SQL*Loader mode (`APPEND`, `TRUNCATE`, `INSERT`, `REPLACE`) |

**Example:**

```json
{
  "type": "oracle_write",
  "name": "load_to_oracle",
  "input": "final_accounts",
  "host": "oracle-db.example.com",
  "port": 1521,
  "service_name": "PRODDB",
  "table": "ACCOUNT_MASTER",
  "vault_path": "secret/data/oracle/prod",
  "load_mode": "APPEND"
}
```

!!! info "Credential security"
    Oracle credentials are fetched from HashiCorp Vault at runtime.
    The `vault_path` points to a Vault secret containing `username` and `password` keys.
    Credentials are never stored in the config JSON.

**Execution flow:**

1. Fetch credentials from Vault (`vault_path`)
2. Export DataFrame to temporary CSV
3. Generate SQL*Loader control (CTL) file
4. Execute `sqlldr` subprocess
5. Clean up temporary files
