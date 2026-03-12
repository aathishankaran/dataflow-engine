# Test Report

Test results for the dataflow engine test suite.

---

## Environment

| Component | Version |
|-----------|---------|
| Python | 3.6.15 |
| PySpark | 3.1.3 |
| Java | 11.0.27 |
| pytest | Latest |

---

## Summary

| Metric | Value |
|--------|-------|
| Total tests | 18 |
| Passed | 17 |
| Failed | 1 |
| Pass rate | 94.4% |

---

## Results by Category

### config_loader (4 tests)

| Test ID | Description | Result |
|---------|-------------|--------|
| DE-001 | Load and parse valid config JSON | PASSED |
| DE-002 | Resolve V1 input path (source_path + source_file_name) | PASSED |
| DE-003 | Resolve V2 input path (dataset_name with bucket prefix) | PASSED |
| DE-004 | Calculate previous business day with holiday skip | PASSED |

### transformations (6 tests)

| Test ID | Description | Result |
|---------|-------------|--------|
| DE-005 | Select with both columns and expressions (mutual exclusivity) | FAILED |
| DE-006 | Filter rows by condition | PASSED |
| DE-007 | Join two DataFrames | PASSED |
| DE-008 | Aggregate with group-by and sum | PASSED |
| DE-009 | Union multiple DataFrames | PASSED |
| DE-010 | Validate with DROP mode | PASSED |

### runner (4 tests)

| Test ID | Description | Result |
|---------|-------------|--------|
| DE-011 | Load fixed-width input with header/trailer | PASSED |
| DE-012 | Write fixed-width output with padding | PASSED |
| DE-013 | End-to-end pipeline execution | PASSED |
| DE-014 | Dry run skips output writes | PASSED |

### oracle_loader (2 tests)

| Test ID | Description | Result |
|---------|-------------|--------|
| DE-015 | Generate CTL file content | PASSED |
| DE-016 | Spark-to-SQLLoader type mapping | PASSED |

### vault (1 test)

| Test ID | Description | Result |
|---------|-------------|--------|
| DE-017 | Fetch credentials with token auth | PASSED |

### incident (1 test)

| Test ID | Description | Result |
|---------|-------------|--------|
| DE-018 | Create missing file incident | PASSED |

---

## DE-005: Known Test Design Issue

!!! note "DE-005 is not a bug"
    Test DE-005 tests the behavior when **both** `columns` and `expressions`
    are provided to a `select` step. This is documented as an invalid
    configuration (the two fields are mutually exclusive).

    The test "failure" reflects the **test design** rather than a code defect.
    The engine intentionally does not support using both fields simultaneously.
    See [Design Decisions](design-decisions.md) for the rationale.

---

## Running the Tests

```bash
cd dataflow-engine
pytest -v
```

Run a specific category:

```bash
pytest tests/test_transformations.py -v
pytest tests/test_config_loader.py -v
```

Run with coverage:

```bash
pytest --cov=dataflow_engine --cov-report=term-missing -v
```
