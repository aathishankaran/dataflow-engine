# Dataflow Engine

Config-driven PySpark engine for mainframe-to-PySpark migration. Reads generated JSON config and executes dataflows with full mainframe keyword → PySpark transformation coverage.

## Project Structure

```
dataflow-engine/
├── dataflow_engine/          # Engine package
│   ├── __init__.py
│   ├── config_loader.py
│   ├── transformations.py
│   └── runner.py
├── samples/                  # Sample config and data for testing
│   ├── config.json           # Dataflow configuration
│   ├── data/
│   │   ├── customer.csv
│   │   └── trans.csv
│   └── output/               # Generated outputs
├── run_dataflow.py           # CLI entry point
├── requirements.txt
└── README.md
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run with sample config (CSV inputs, Parquet outputs)
python run_dataflow.py samples/config.json

# Dry run (transform only, no writes)
python run_dataflow.py samples/config.json --dry-run
```

## Sample Dataflow

The sample `samples/config.json` demonstrates:

- **Join**: CUSTOMER + TRANS on CM_CUST_ID = TR_CUST_ID (inner join)
- **Filter**: TR_AMOUNT > 50 (COBOL: IF GREATER THAN)
- **Select/MOVE**: Map columns to report layout (COBOL: MOVE)
- **Aggregate**: Group by CM_REGION, SUM(TR_AMOUNT), COUNT (COBOL: SUM/COUNT)

Outputs:
- `samples/output/report1/` - Parquet (filtered detail)
- `samples/output/report2/` - Parquet (region totals)
- `samples/output/report1_csv/` - CSV

## Config JSON Structure

```json
{
  "Inputs": {
    "CUSTOMER": {
      "name": "CUSTOMER",
      "format": "csv",
      "path": "samples/data/customer.csv"
    }
  },
  "Outputs": {
    "REPORT1": {
      "name": "REPORT1",
      "format": "parquet",
      "path": "samples/output/report1"
    }
  },
  "Transformations": {
    "steps": [
      {"id": "join_1", "type": "join", "logic": {...}, "output_alias": "joined"},
      {"id": "filter_1", "type": "filter", "logic": {"conditions": [...]}},
      {"id": "agg_1", "type": "aggregate", "logic": {"group_by": [...], "aggregations": [...]}}
    ]
  }
}
```

## Working storage (temp variables)

Mainframe uses in-memory working-storage variables (e.g. `WS_DEBIT_TOTAL`, `WS_CREDIT_TOTAL`) that are not in the input record. The framework supports this:

- **ADD** to a target that does not exist yet is treated as `0 + expression` (e.g. `WS_DEBIT_TOTAL + TXN_AMT` creates `WS_DEBIT_TOTAL`).
- **SUBTRACT** from a missing target uses 0 as base; **MULTIPLY** uses 1.
- In config you can optionally list names in `logic.working_storage` for clarity; the framework infers from the expression target.
- **COMPUTE** expressions may use `_` for minus (e.g. `WS_CREDIT_TOTAL _ WS_DEBIT_TOTAL`); it is normalized to `-` for Spark.

No need to add these variables to input schema or emit an explicit INITIALIZE step first.

## Supported Mainframe Keywords

| COBOL/JCL | PySpark |
|-----------|---------|
| ADD, SUBTRACT, MULTIPLY, DIVIDE, COMPUTE | `withColumn`, `expr` |
| MOVE, INITIALIZE, STRING, UNSTRING | `withColumn`, `concat_ws`, `split` |
| IF GREATER/LESS THAN, EVALUATE | `filter` |
| SORT, MERGE | `orderBy`, `unionByName` |
| SUM, COUNT | `groupBy`, `agg` |
| JOIN (multi-file) | `join` |
