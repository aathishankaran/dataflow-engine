# transformations

Configuration-driven PySpark DataFrame transformations (no Spark SQL). Implements 8 transformation types using COBOL mainframe keywords for column operations.

---

## Overview

The `transformations` module contains `MainframeTransformationExecutor`, which implements:

- **filter**: Row filtering by conditions
- **select**: Column projection or expression evaluation (mutually exclusive modes)
- **join**: DataFrame join operations
- **aggregate**: Group-by with aggregation functions
- **union**: Combine multiple DataFrames
- **validate**: Data quality validation with FLAG/DROP/ABORT modes
- **custom**: Sort and merge operations
- **oracle_write**: Oracle SQL*Loader bulk loading

See [Transformation Types](../transformations-guide.md) for configuration examples and [COBOL-to-PySpark Mapping](../cobol-mapping.md) for COBOL verb mappings.

---

## API Documentation

::: dataflow_engine.transformations
    options:
      show_root_heading: false
      members_order: source
      filters: ["!^_"]
