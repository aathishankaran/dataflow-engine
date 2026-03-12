# Getting Started

This guide walks through installation, configuration, and running your first dataflow pipeline.

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.6.8+ | Tested with 3.6.15 |
| Java | 8 or 11 | Required by PySpark |
| pip | Latest | For dependency installation |

!!! warning "Java requirement"
    PySpark requires Java 8 or 11. Java 17+ is **not** supported by PySpark 3.1.x.
    Ensure `JAVA_HOME` is set correctly before running the pipeline.

---

## Installation

```bash
cd dataflow-engine
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Verify the installation:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

---

## First Run

```bash
python run_dataflow.py samples/config.json
```

This reads the sample configuration, builds a local SparkSession, and executes the pipeline.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `JAVA_HOME` | Yes | Path to Java JDK (8 or 11) |
| `VAULT_ADDR` | No | HashiCorp Vault server URL |
| `VAULT_TOKEN` | No | Vault authentication token |
| `VAULT_ROLE_ID` | No | Vault AppRole role ID |
| `VAULT_SECRET_ID` | No | Vault AppRole secret ID |
| `MOOGSOFT_ENDPOINT` | No | Moogsoft API endpoint for incident creation |
| `MOOGSOFT_API_KEY` | No | Moogsoft API key for authentication |
| `AWS_ACCESS_KEY_ID` | No | AWS access key for S3 input/output |
| `AWS_SECRET_ACCESS_KEY` | No | AWS secret key for S3 input/output |

!!! info "Optional integrations"
    Vault, Moogsoft, and AWS variables are only needed when the pipeline uses
    Oracle loading, incident alerting, or S3-based file paths respectively.

---

## Dependencies

| Package | Version Constraint | Purpose |
|---------|--------------------|---------|
| PySpark | `>=3.0.0,<3.2` | Distributed data processing |
| Boto3 | Latest | AWS S3 file access |
| HVAC | Latest | HashiCorp Vault client |
| Requests | Latest | HTTP client for Moogsoft API |
| PyArrow | Latest | Parquet file support |

---

## Running Tests

```bash
pytest -v
```

Run a specific test file:

```bash
pytest tests/test_config_loader.py -v
```

---

## Project Layout

```text
dataflow-engine/
  run_dataflow.py          # CLI entry point
  requirements.txt         # Python dependencies
  dataflow_engine/
    __init__.py
    config_loader.py       # Config parsing and path resolution
    runner.py              # Pipeline orchestrator
    transformations.py     # 8 transformation types
    oracle_loader.py       # Oracle SQL*Loader integration
    vault.py               # Vault credential fetching
    incident.py            # Moogsoft incident connector
  samples/
    config.json            # Example configuration
  tests/
    ...                    # Test suite
```

---

## Next Steps

- Read the [CLI Reference](cli.md) for all command-line options
- Explore the [Pipeline Flow](pipeline.md) to understand execution phases
- See [Transformation Types](transformations-guide.md) for config examples
