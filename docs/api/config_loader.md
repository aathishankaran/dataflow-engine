# config_loader

Config JSON loading, validation, and path resolution for PySpark execution. Handles V1 and V2 config schemas, frequency-based date partitioning, and previous business day calculation.

---

## Overview

The `config_loader` module is responsible for:

- Parsing and validating the pipeline JSON configuration
- Resolving input and output file paths (V1, V2, and Legacy schemas)
- Calculating the previous business day with holiday awareness
- Building control file paths with date and frequency partitioning

See [Design Decisions](../design-decisions.md) for config versioning details.

---

## API Documentation

::: dataflow_engine.config_loader
    options:
      show_root_heading: false
      members_order: source
      filters: []
