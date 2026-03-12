# incident

Moogsoft/ServiceNow incident connector for automated alerting on pipeline failures, missing files, date mismatches, and record count discrepancies.

---

## Overview

The `incident` module provides `MoogsoftIncidentConnector`, which creates automated incidents when pipeline errors occur. It supports 6 incident types:

| Method | Trigger |
|--------|---------|
| `create_missing_file_incident()` | Input file not found on disk or S3 |
| `create_date_mismatch_incident()` | Header date does not match expected previous business day |
| `create_validation_failure_incident()` | Data validation rules failed (ABORT mode) |
| `create_record_count_incident()` | Actual record count does not match expected count |
| `create_oracle_load_incident()` | SQL*Loader execution failed |
| `create_generic_incident()` | General pipeline error or unhandled exception |

**Severity levels:** `critical`, `major`, `minor`, `warning`

Requires `MOOGSOFT_ENDPOINT` and `MOOGSOFT_API_KEY` environment variables.

---

## API Documentation

::: dataflow_engine.incident
    options:
      show_root_heading: false
      members_order: source
      filters: ["!^_"]
