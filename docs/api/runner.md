# runner

`DataFlowRunner` -- the main PySpark pipeline orchestrator. Configuration-driven, step-by-step execution: load inputs, run transformations, write outputs with header/trailer rows and control files.

---

## Overview

The `runner` module contains the `DataFlowRunner` class, which:

- Manages the DataFrame registry (shared dictionary of named DataFrames)
- Loads input files in multiple formats (FIXED, CSV, Parquet, Delimited, COBOL)
- Executes transformation steps sequentially via `MainframeTransformationExecutor`
- Writes output files with fixed-width padding, header/trailer rows, and control files

See [Pipeline Flow](../pipeline.md) for the full execution model.

---

## API Documentation

::: dataflow_engine.runner
    options:
      show_root_heading: false
      members_order: source
      filters: ["!^_"]
