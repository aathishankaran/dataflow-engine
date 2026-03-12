# COBOL-to-PySpark Mapping

This page documents how COBOL verbs and JCL dispositions map to PySpark transformation types in the dataflow engine.

---

## COBOL Verb Mapping

The dataflow engine uses COBOL-style keywords in transformation configurations to maintain familiarity for mainframe developers.

| COBOL Verb | PySpark Type | Description | Example Use |
|-----------|-------------|-------------|-------------|
| `ADD` | `aggregate` | Summation of numeric fields | Sum transaction amounts by account |
| `SUBTRACT` | `select` | Subtraction expression | Compute net amount (credits - debits) |
| `MULTIPLY` | `select` | Multiplication expression | Apply interest rate to balance |
| `DIVIDE` | `select` | Division expression | Calculate per-unit cost |
| `MOVE` | `select` | Column assignment / rename | Copy field value to new column |
| `COMPUTE` | `select` | Arithmetic expression | Evaluate compound formula |
| `IF` | `filter` | Conditional row filtering | Keep rows where status = ACTIVE |
| `SORT` | `custom` | Row ordering by key columns | Sort transactions by date |
| `MERGE` | `union` | Combine multiple DataFrames | Union daily files into monthly |
| `STRING` | `select` | String concatenation | Build full name from first + last |
| `UNSTRING` | `select` | String splitting / substring | Extract area code from phone |
| `INSPECT` | `select` | Regex replacement / counting | Replace special characters |

---

## Expression Examples

### ADD (via aggregate)

```json
{
  "type": "aggregate",
  "group_by": ["account_id"],
  "aggregations": [
    { "field": "amount", "function": "sum", "alias": "total_amount" }
  ]
}
```

### SUBTRACT / MULTIPLY / DIVIDE (via select expressions)

```json
{
  "type": "select",
  "expressions": [
    { "output_field": "net_amount", "operation": "SUBTRACT", "operands": ["credit", "debit"] },
    { "output_field": "interest", "operation": "MULTIPLY", "operands": ["balance", "rate"] },
    { "output_field": "unit_cost", "operation": "DIVIDE", "operands": ["total_cost", "quantity"] }
  ]
}
```

### MOVE (column assignment)

```json
{
  "type": "select",
  "expressions": [
    { "output_field": "customer_name", "operation": "MOVE", "operands": ["acct_holder_name"] }
  ]
}
```

### COMPUTE (compound expression)

```json
{
  "type": "select",
  "expressions": [
    { "output_field": "final_balance", "operation": "COMPUTE", "operands": ["balance + interest - fees"] }
  ]
}
```

### STRING (concatenation)

```json
{
  "type": "select",
  "expressions": [
    { "output_field": "full_address", "operation": "STRING", "operands": ["street", "' '", "city", "' '", "state"] }
  ]
}
```

### UNSTRING (splitting)

```json
{
  "type": "select",
  "expressions": [
    { "output_field": "area_code", "operation": "UNSTRING", "operands": ["phone_number", "0", "3"] }
  ]
}
```

### INSPECT (regex replace)

```json
{
  "type": "select",
  "expressions": [
    { "output_field": "clean_name", "operation": "INSPECT", "operands": ["raw_name", "[^A-Za-z ]", ""] }
  ]
}
```

---

## JCL Disposition Mapping

JCL DD statement dispositions map to input/output classification in the dataflow engine:

| JCL Disposition | Engine Role | Description |
|----------------|-------------|-------------|
| `SHR` | Input | Shared read access -- file is an input |
| `OLD` | Input | Exclusive read access -- file is an input |
| `NEW` | Output | Create new file -- file is an output |
| `MOD` | Output | Append to existing file -- file is an output |

### How dispositions are used

When converting JCL jobs to dataflow configurations:

- **SHR / OLD** dispositions indicate the DD is an **input** dataset. The file path is resolved via `get_input_path()` and the file is read into a DataFrame.
- **NEW / MOD** dispositions indicate the DD is an **output** dataset. The file path is resolved via `get_output_path()` and the DataFrame is written to the file.

```text
JCL:
  //INPUT1  DD DSN=PROD.ACCOUNTS,DISP=SHR      --> inputs[]
  //INPUT2  DD DSN=PROD.CUSTOMERS,DISP=OLD      --> inputs[]
  //OUTPUT1 DD DSN=PROD.MERGED,DISP=(NEW,CATLG) --> outputs[]
```

---

## COBOL Data Type Mapping

COBOL PIC clauses map to Spark data types:

| COBOL PIC | Spark Type | Example |
|-----------|-----------|---------|
| `PIC X(n)` | `StringType` | `PIC X(20)` -- 20-char string |
| `PIC 9(n)` | `IntegerType` or `LongType` | `PIC 9(5)` -- 5-digit integer |
| `PIC 9(n)V9(m)` | `DecimalType(n+m, m)` | `PIC 9(7)V99` -- decimal with 2 places |
| `PIC S9(n)` | `IntegerType` (signed) | `PIC S9(5)` -- signed integer |
| `PIC S9(n)V9(m)` | `DecimalType` (signed) | `PIC S9(5)V99` -- signed decimal |

!!! note "Fixed-width representation"
    In fixed-width files, all fields are stored as strings regardless of their
    logical data type. Type casting is applied during DataFrame creation based
    on the field schema configuration.
