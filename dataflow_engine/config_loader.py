"""
Load and validate dataflow config JSON for PySpark execution.
"""

import calendar as _calendar
import json
import re as _re
from datetime import date as _date, timedelta as _timedelta
from pathlib import Path
from typing import Any


def load_config(path: str | Path) -> dict[str, Any]:
    """
    Load dataflow configuration from JSON file.

    Expected structure:
        {
            "Inputs": {
                "DD_NAME": {
                    name, format, source_path, source_file_name, fields,
                    control_file_name,          # companion count/control file name
                    has_count_file,             # bool – explicit count-file opt-in
                    count_file_path,            # directory for count file (overrides source_path)
                    count_field_name,           # field inside count file holding expected count
                    record_length, header_count, trailer_count, ...
                }
            },
            "Outputs": { "DD_NAME": { ... } },
            "Transformations": { "steps": [ { id, type, source_inputs, logic, output_alias } ], ... }
        }

    Schema: Populate "fields" and "copybook" (and "cobrix.copybook_path" for inputs) from
    COBOL copybooks so the runner can use Cobrix and fallback schemas; use the wiki's
    "Enrich copybooks" or import from ZIP with copybooks to fill these.

    Args:
        path: Path to config JSON file (local or S3).

    Returns:
        Parsed config dict.

    Raises:
        FileNotFoundError: If config file not found.
        ValueError: If config structure is invalid.
    """
    path = Path(path) if isinstance(path, str) else path
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    content = path.read_text(encoding="utf-8")
    config = json.loads(content)

    if not isinstance(config, dict):
        raise ValueError("Config must be a JSON object")

    # Support both PascalCase (from schema) and lowercase
    inputs_key = "Inputs" if "Inputs" in config else "inputs"
    outputs_key = "Outputs" if "Outputs" in config else "outputs"
    trans_key = "Transformations" if "Transformations" in config else "transformations"

    if inputs_key not in config or outputs_key not in config:
        raise ValueError("Config must contain Inputs and Outputs")

    config["Inputs"] = config.get(inputs_key, {})
    config["Outputs"] = config.get(outputs_key, {})
    config["Transformations"] = config.get(trans_key, {})

    return config


def _is_absolute_path(p: str) -> bool:
    """True if path looks absolute (local absolute, file:/, or s3:)."""
    if not p or not isinstance(p, str):
        return False
    p = p.strip()
    if p.startswith(("s3:", "s3://", "file:", "file://")):
        return True
    if p.startswith("/"):
        return True
    # Windows drive letter
    if len(p) >= 2 and p[1] == ":" and p[0].isalpha():
        return True
    return False


def _resolve_path(path: str, base_path: str | None) -> str:
    """Resolve path against base_path when path is relative."""
    if not path or not base_path or _is_absolute_path(path):
        return path or ""
    return str(Path(base_path) / path)


def _join_path_parts(prefix: str, filename: str) -> str:
    """Join a prefix/directory with a filename, handling trailing slashes."""
    if not prefix:
        return filename or ""
    if not filename:
        return prefix
    sep = "" if prefix.endswith("/") else "/"
    return prefix + sep + filename


def _resolve_partition_col(partition_col: str) -> str:
    """
    Resolve a partition column expression to a concrete date string for use in
    file-system path segments.

    Supported expressions (case-insensitive):
        load_date()                            → YYYYMMDD        e.g. 20260301
        current_date()                         → YYYY-MM-DD      e.g. 2026-03-01
        date_sub(current_date(), N)            → YYYY-MM-DD      e.g. 2026-02-28
        date_format(current_date(), 'fmt')     → custom format   e.g. 03-01-2026
        last_day(current_date())               → YYYY-MM-DD      e.g. 2026-03-31

    Anything else is returned as-is so literal values are passed through.
    """
    if not partition_col:
        return ""

    today = _date.today()
    expr  = partition_col.strip()
    low   = expr.lower()

    # load_date() — mainframe convention: YYYYMMDD
    if low == "load_date()":
        return today.strftime("%Y%m%d")

    # current_date() — standard SQL: YYYY-MM-DD
    if low == "current_date()":
        return today.strftime("%Y-%m-%d")

    # date_sub(current_date(), N) — N days ago
    m = _re.match(r"date_sub\s*\(\s*current_date\s*\(\s*\)\s*,\s*(\d+)\s*\)", low)
    if m:
        n = int(m.group(1))
        return (today - _timedelta(days=n)).strftime("%Y-%m-%d")

    # date_format(current_date(), 'spark_fmt') — custom format
    m = _re.match(r"date_format\s*\(\s*current_date\s*\(\s*\)\s*,\s*['\"]([^'\"]+)['\"]\s*\)", low)
    if m:
        spark_fmt = expr.split(",", 1)[-1].rstrip(")").strip().strip("'\"")
        py_fmt = (spark_fmt
                  .replace("yyyy", "%Y").replace("yy", "%y")
                  .replace("MM",   "%m")
                  .replace("dd",   "%d"))
        try:
            return today.strftime(py_fmt)
        except ValueError:
            pass

    # last_day(current_date()) — last calendar day of current month
    if low == "last_day(current_date())":
        last_day = _calendar.monthrange(today.year, today.month)[1]
        return today.replace(day=last_day).strftime("%Y-%m-%d")

    return expr  # literal — pass through unchanged


def _build_partitioned_path(
    source_path: str,
    source_file: str,
    frequency: str,
    partition_col: str,
) -> str:
    """
    Compose  <source_path>/<FREQUENCY>/<date>/<source_file>  from its parts.
    Any empty segment is skipped transparently.

    When *frequency* is present but *partition_col* is absent, the partition
    defaults to ``load_date()`` (today's date), so the layout always follows
    the  …/DAILY/YYYYMMDD/…  convention without requiring the field in every
    config node.

    Examples
    --------
    >>> _build_partitioned_path("s3://b/curated", "TX.DAT", "DAILY", "load_date()")
    's3://b/curated/DAILY/20260301/TX.DAT'
    >>> _build_partitioned_path("s3://b/output", "", "DAILY", "")
    's3://b/output/DAILY/20260301'
    """
    path = source_path
    if frequency:
        path = _join_path_parts(path, frequency.upper())
        date_part = _resolve_partition_col(partition_col or "load_date()")
        if date_part:
            path = _join_path_parts(path, date_part)
    if source_file:
        path = _join_path_parts(path, source_file)
    return path


def get_input_path(config_input: dict, base_path: str | None = None) -> str:
    """
    Resolve input file path from config.

    Priority order:
      1. source_path + source_file_name  (new schema) — path is enriched with
         frequency/date partition:  <source_path>/<FREQUENCY>/<YYYYMMDD>/<source_file_name>
      2. path                            (legacy — returned as-is, no partition injection)
      3. s3_path                         (legacy alias)
      4. cobrix.copybook_path / copybook (COBOL)
      5. dataset

    Relative paths are resolved against base_path so --base-path works correctly.
    """
    # New schema: source_path + source_file_name with frequency/date partition
    source_path = (config_input.get("source_path") or "").strip()
    source_file = (config_input.get("source_file_name") or "").strip()
    if source_path or source_file:
        frequency     = (config_input.get("frequency")     or "").strip()
        partition_col = (config_input.get("partition_col") or "").strip()
        combined = _build_partitioned_path(source_path, source_file, frequency, partition_col)
        return _resolve_path(combined, base_path) if base_path and not _is_absolute_path(combined) else combined

    # Legacy: explicit path key
    path = config_input.get("path")
    if path:
        return _resolve_path(path, base_path) if base_path else path

    # Legacy: s3_path alias
    s3 = config_input.get("s3_path")
    if s3:
        return _resolve_path(s3, base_path) if base_path else s3

    # COBOL cobrix path
    cobrix = config_input.get("cobrix") or {}
    cpy = cobrix.get("copybook_path") or config_input.get("copybook")
    if cpy and base_path and not _is_absolute_path(cpy):
        return str(Path(base_path) / cpy)
    if cpy:
        return cpy

    return config_input.get("dataset", "") or ""


def get_control_file_path(config_input: dict, base_path: str | None = None) -> str:
    """
    Resolve the control/count file path for fixed-width inputs.

    The control file lives in the same frequency/date partition directory as
    the data file:  <dir>/<FREQUENCY>/<YYYYMMDD>/<control_file_name>

    Priority for the base directory:
      1. count_file_path  (explicit override directory)
      2. source_path      (same directory as the data file)
      3. count_file_path  (legacy — bare directory, no partition injection)
    """
    frequency     = (config_input.get("frequency")     or "").strip()
    partition_col = (config_input.get("partition_col") or "").strip()

    control_name = (config_input.get("control_file_name") or "").strip()
    if control_name:
        # Prefer explicit count_file_path directory if provided
        count_dir  = (config_input.get("count_file_path") or "").strip()
        dir_to_use = count_dir or (config_input.get("source_path") or "").strip()
        # Apply the same frequency/date partition as the data file
        combined = _build_partitioned_path(dir_to_use, control_name, frequency, partition_col)
        return _resolve_path(combined, base_path) if base_path and not _is_absolute_path(combined) else combined

    legacy = (config_input.get("count_file_path") or "").strip()
    if legacy:
        return _resolve_path(legacy, base_path) if base_path and not _is_absolute_path(legacy) else legacy

    return ""


def get_count_file_path(config_input: dict, base_path: str | None = None) -> str:
    """
    Resolve the count file path when the input has an explicit count file
    (``has_count_file = true`` in the config JSON).

    This is an alias for :func:`get_control_file_path` that makes the intent
    clearer at call sites that deal with record-count validation.

    Returns:
        Full path to the count/control file, or empty string if not configured.
    """
    return get_control_file_path(config_input, base_path)


def has_count_file(config_input: dict) -> bool:
    """Return True if the input config declares a companion count file."""
    if config_input.get("has_count_file"):
        return True
    # Implicit: a control_file_name without the explicit flag is still a count file
    return bool((config_input.get("control_file_name") or "").strip())


def get_frequency(config: dict, node_name: str) -> str | None:
    """
    Return the frequency string (e.g. DAILY / WEEKLY / MONTHLY) for a named
    input or output node, or None if the attribute is absent.

    Searches both PascalCase ('Inputs'/'Outputs') and lowercase keys so the
    helper works regardless of which casing was used by the caller.

    Args:
        config:    Parsed dataflow config dict (as returned by load_config).
        node_name: Name/key of the input or output node to look up.

    Returns:
        Uppercase frequency string, or None if not set.
    """
    for section in ("Inputs", "inputs", "Outputs", "outputs"):
        nodes = config.get(section) or {}
        if node_name in nodes:
            freq = nodes[node_name].get("frequency")
            return freq.upper() if isinstance(freq, str) and freq.strip() else None
    return None


def get_output_path(config_output: dict, base_path: str | None = None) -> str:
    """
    Resolve output path from config.

    For the new schema (``source_path`` present) the path is enriched with the
    frequency/date partition so the output lands in the correct daily bucket::

        <source_path>/<FREQUENCY>/<YYYYMMDD>

    ``target_file_name`` (single-file rename) is handled separately by the
    runner via ``_coalesce_to_named_file`` and is *not* included here.

    Legacy keys (``path``, ``s3_path``, ``dataset``) are returned as-is
    without partition injection — this keeps test-mode temp paths unchanged.

    Relative paths are resolved against ``base_path``.
    """
    source_path = (config_output.get("source_path") or "").strip()
    if source_path:
        frequency     = (config_output.get("frequency")     or "").strip()
        partition_col = (config_output.get("partition_col") or "").strip()
        # No source_file_name for outputs — the directory IS the write target
        combined = _build_partitioned_path(source_path, "", frequency, partition_col)
        return _resolve_path(combined, base_path) if base_path and not _is_absolute_path(combined) else combined

    # Legacy keys — no partition injection (test-mode uses "path" directly)
    path = (
        config_output.get("path")
        or config_output.get("s3_path")
        or config_output.get("dataset", "")
    )
    if not path:
        return ""
    return _resolve_path(path, base_path) if base_path and not _is_absolute_path(path) else path
