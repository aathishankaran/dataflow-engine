# oracle_loader

Oracle SQL*Loader integration for loading DataFrames into Oracle databases. Orchestrates: Vault fetch, CSV export, CTL generation, and `sqlldr` execution.

---

## Overview

The `oracle_loader` module handles the full Oracle loading pipeline:

1. Fetch credentials from HashiCorp Vault
2. Export the Spark DataFrame to a temporary CSV file
3. Generate a SQL*Loader control (CTL) file with type mappings
4. Execute the `sqlldr` command-line tool
5. Clean up temporary files

See [Transformation Types](../transformations-guide.md#oracle_write) for the `oracle_write` step configuration.

---

## API Documentation

::: dataflow_engine.oracle_loader
    options:
      show_root_heading: false
      members_order: source
      filters: []
