# Dataflow Engine

PySpark-based, config-driven data processing pipeline for mainframe migration. Reads a config JSON file and executes a three-phase pipeline: **load inputs**, **run transformations**, and **write outputs**.

---

## Key Features

| Feature | Description |
|---------|-------------|
| 8 transformation types | `filter`, `select`, `join`, `aggregate`, `union`, `validate`, `custom`, `oracle_write` |
| Fixed-width I/O | Positional field padding with header and trailer record support |
| Oracle SQL*Loader | Generate CTL files and invoke `sqlldr` for bulk database loading |
| Vault credentials | Fetch Oracle passwords securely from HashiCorp Vault at runtime |
| Moogsoft incidents | Automated alerts for validation failures, missing files, and date mismatches |
| Previous business day | Header date checking with configurable holiday calendar |
| Multi-format support | COBOL copybook, CSV, Parquet, Delimited, and Fixed-width formats |
| Config-driven | No code changes required; all behavior is controlled by JSON configuration |

---

## How It Works

```text
config.json
    |
    v
run_dataflow.py  -->  SparkSession  -->  DataFlowRunner
                                            |
                                            +-- load_inputs()
                                            |     Read files into DataFrames
                                            |
                                            +-- run_transformations()
                                            |     Apply filter, select, join, etc.
                                            |
                                            +-- write_outputs()
                                                  Write DataFrames to files
```

---

## Quick Links

| Page | Description |
|------|-------------|
| [Getting Started](getting-started.md) | Installation, prerequisites, and first run |
| [Developer Guide](developer-guide.md) | Architecture, module reference, and project structure |
| [CLI Reference](cli.md) | Command-line arguments and usage examples |
| [Pipeline Flow](pipeline.md) | Detailed execution phases and data flow |
| [Transformation Types](transformations-guide.md) | All 8 transformation types with JSON examples |
| [COBOL-to-PySpark Mapping](cobol-mapping.md) | COBOL verb and JCL disposition mappings |
| [Design Decisions](design-decisions.md) | Rationale for key architectural choices |
| [Test Report](test-report.md) | Test results and coverage summary |
| [API Reference](api/run_dataflow.md) | Auto-generated module documentation |

---

## Quick Start

```bash
cd dataflow-engine
pip install -r requirements.txt
python run_dataflow.py samples/config.json
```

!!! tip "Config-driven pipeline"
    The entire pipeline behavior is controlled by a single JSON configuration file.
    No source code changes are needed to add new data flows.
