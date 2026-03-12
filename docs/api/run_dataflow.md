# run_dataflow

CLI entry point for the dataflow engine. Parses command-line arguments, builds an OS-aware SparkSession, and orchestrates the pipeline via `DataFlowRunner`.

---

## Overview

This module is the main entry point invoked from the command line. It handles:

- Argument parsing (`argparse`)
- SparkSession creation with platform-specific configuration
- Pipeline execution via `DataFlowRunner`
- Error collection and formatting

---

## Usage

```bash
python run_dataflow.py [config] [--base-path PATH] [--no-cobrix] [--master URL] [--dry-run] [--settings PATH]
```

See [CLI Reference](../cli.md) for full argument documentation.

---

## API Documentation

::: run_dataflow
    options:
      show_root_heading: false
      members_order: source
      filters: ["!^_"]
