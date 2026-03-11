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
                    name,                       # node name / ID
                    format,                     # FIXED | CSV | PARQUET | DELIMITED
                    frequency,                  # DAILY | WEEKLY | MONTHLY
                    dataset_name,               # file name with extension (v2 schema)
                    record_length,              # total char width per record (FIXED)
                    header_count,               # header lines to skip (FIXED/DELIMITED)
                    trailer_count,              # trailer lines to skip (FIXED)
                    delimiter_char,             # delimiter character (DELIMITED/CSV)
                    fields: [{                  # column definitions
                        name, type, start, length,
                        nullable, format        # format: date/timestamp pattern e.g. "yyyy-MM-dd"
                    }],
                    control_file_name,          # companion count/control file name
                    has_count_file,             # bool – explicit count-file opt-in
                    count_file_path,            # directory for count file (overrides source_path)
                    count_field_name,           # field inside count file holding expected count
                    source_path, source_file_name,  # v1 schema (legacy)
                    _schema_file, _test_file, _test_rows  # UI metadata
                }
            },
            "Outputs": {
                "DD_NAME": {
                    name,                       # node name / ID
                    format,                     # FIXED | PARQUET | CSV | DELIMITED
                    frequency,                  # DAILY | WEEKLY | MONTHLY
                    dataset_name,               # output file name with extension
                    write_mode,                 # OVERWRITE | APPEND
                    source_inputs,              # list of upstream step aliases
                    record_length,              # total char width per record (FIXED)
                    header_count,               # header lines (FIXED)
                    trailer_count,              # trailer lines (FIXED)
                    delimiter_char,             # delimiter character (DELIMITED)
                    ctrl_file_gen,              # bool – generate control file
                    fields: [{                  # column definitions
                        name, type, start, length,
                        nullable, format        # format: date/timestamp pattern
                    }],
                    output_columns,             # ordered list of column names to write
                    control_fields,             # control file field definitions
                    _schema_file, _test_file, _test_rows  # UI metadata
                }
            },
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
        current_date()                         → YYYYMMDD        e.g. 20260301
        date_sub(current_date(), N)            → YYYYMMDD        e.g. 20260228
        date_format(current_date(), 'fmt')     → custom format   e.g. 03-01-2026
        last_day(current_date())               → YYYYMMDD        e.g. 20260331

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

    # current_date() — YYYYMMDD (consistent with load_date for file paths)
    if low == "current_date()":
        return today.strftime("%Y%m%d")

    # date_sub(current_date(), N) — N days ago, YYYYMMDD
    m = _re.match(r"date_sub\s*\(\s*current_date\s*\(\s*\)\s*,\s*(\d+)\s*\)", low)
    if m:
        n = int(m.group(1))
        return (today - _timedelta(days=n)).strftime("%Y%m%d")

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

    # last_day(current_date()) — last calendar day of current month, YYYYMMDD
    if low == "last_day(current_date())":
        last_day = _calendar.monthrange(today.year, today.month)[1]
        return today.replace(day=last_day).strftime("%Y%m%d")

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


def get_input_path(
    config_input: dict,
    base_path: str | None = None,
    *,
    interface_name: str = "",
    settings: dict | None = None,
) -> str:
    """
    Resolve input file path from config.

    Priority order:
      0. dataset_name (v2 schema) — full path derived as
         <raw_bucket_prefix>/<interface_name>/<FREQUENCY>/<YYYYMMDD>/<dataset_name>
      1. source_path + source_file_name  (v1 schema) — path is enriched with
         frequency/date partition:  <source_path>/<FREQUENCY>/<YYYYMMDD>/<source_file_name>
      2. path                            (legacy — returned as-is, no partition injection)
      3. s3_path                         (legacy alias)
      4. cobrix.copybook_path / copybook (COBOL)
      5. dataset

    Relative paths are resolved against base_path so --base-path works correctly.
    """
    # V2 schema: dataset_name only (no source_path) — derive full path from settings
    dataset_name = (config_input.get("dataset_name") or "").strip()
    if dataset_name and not (config_input.get("source_path") or "").strip():
        bucket = ((settings or {}).get("raw_bucket_prefix") or "").strip()
        base = _join_path_parts(bucket, interface_name) if interface_name else bucket
        frequency = (config_input.get("frequency") or "").strip()
        return _build_partitioned_path(base, dataset_name, frequency, "load_date()")

    # V1 schema: source_path + source_file_name with frequency/date partition
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


def _extract_holiday_dates(usa_holidays) -> list:
    """Extract ISO date strings from either old format (list of strings) or
    new structured format (list of dicts with active/name/date keys).

    Only entries that are active (active=True, or old string format) are included.
    """
    result = []
    for h in (usa_holidays or []):
        if isinstance(h, str):
            result.append(h)
        elif isinstance(h, dict) and h.get("active", True):
            d = h.get("date", "")
            if d:
                result.append(d)
    return result


def get_previous_business_day(target_date: _date, holidays: list) -> _date:
    """Return the most-recent business day strictly before *target_date*,
    skipping weekends and any ISO-format date strings in *holidays*.

    Args:
        target_date: The reference date (usually today).
        holidays:    List of ISO-format date strings (``YYYY-MM-DD``) for
                     USA federal holidays on which no data is delivered.

    Returns:
        The previous business day as a :class:`datetime.date`.

    Examples:
        >>> from datetime import date
        >>> get_previous_business_day(date(2026, 3, 2), [])
        datetime.date(2026, 2, 27)  # Monday → Friday (skip weekend)
        >>> get_previous_business_day(date(2026, 7, 6), ['2026-07-04'])
        datetime.date(2026, 7, 2)   # Mon after 4th-of-July weekend → Thursday
    """
    holiday_set: set = set()
    for h in _extract_holiday_dates(holidays):
        try:
            holiday_set.add(_date.fromisoformat(h.strip()))
        except (ValueError, AttributeError):
            pass
    candidate = target_date - _timedelta(days=1)
    while candidate in holiday_set:  # only skip configured holidays; weekends are valid
        candidate -= _timedelta(days=1)
    return candidate


def get_input_path_for_date(
    config_input: dict,
    partition_date: _date,
    *,
    interface_name: str = "",
    settings: dict | None = None,
) -> str:
    """Like :func:`get_input_path` but uses *partition_date* instead of today.

    Used by the previous-day header check to build the path to the prior
    business day's raw input file.

    Supports V2 (``dataset_name``) and V1 (``source_path`` +
    ``source_file_name``) schemas.  Falls back to :func:`get_input_path`
    when neither schema key is present.

    Args:
        config_input:    Input node config dict.
        partition_date:  The specific date to inject into the path.
        interface_name:  Interface / pipeline name (used with V2 schema).
        settings:        Settings dict containing ``raw_bucket_prefix``.

    Returns:
        Full path string for the given date.
    """
    date_str = partition_date.strftime("%Y%m%d")

    # V2 schema: dataset_name only (no source_path) — derive from settings
    dataset_name = (config_input.get("dataset_name") or "").strip()
    if dataset_name and not (config_input.get("source_path") or "").strip():
        bucket = ((settings or {}).get("raw_bucket_prefix") or "").strip()
        base = _join_path_parts(bucket, interface_name) if interface_name else bucket
        frequency = (config_input.get("frequency") or "").strip()
        path = base
        if frequency:
            path = _join_path_parts(path, frequency.upper())
            path = _join_path_parts(path, date_str)
        return _join_path_parts(path, dataset_name)

    # V1 schema: source_path + source_file_name with frequency/date partition
    source_path = (config_input.get("source_path") or "").strip()
    source_file = (config_input.get("source_file_name") or "").strip()
    if source_path or source_file:
        frequency = (config_input.get("frequency") or "").strip()
        path = source_path
        if frequency:
            path = _join_path_parts(path, frequency.upper())
            path = _join_path_parts(path, date_str)
        return _join_path_parts(path, source_file)

    # Fallback — use today's path (safe no-op)
    return get_input_path(config_input, interface_name=interface_name, settings=settings)


def get_control_file_path(
    config_input: dict,
    base_path: str | None = None,
    *,
    interface_name: str = "",
    settings: dict | None = None,
) -> str:
    """
    Resolve the control/count file path for fixed-width inputs.

    The control file lives in the same frequency/date partition directory as
    the data file:  <dir>/<FREQUENCY>/<YYYYMMDD>/<control_file_name>

    Priority for the base directory:
      1. count_file_path  (explicit override directory)
      2. source_path      (same directory as the data file)
      3. raw_bucket_prefix + interface_name  (v2 schema — no source_path)
      4. count_file_path  (legacy — bare directory, no partition injection)
    """
    frequency     = (config_input.get("frequency")     or "").strip()
    partition_col = (config_input.get("partition_col") or "").strip()

    control_name = (config_input.get("control_file_name") or "").strip()
    if control_name:
        # Prefer explicit count_file_path directory if provided
        count_dir  = (config_input.get("count_file_path") or "").strip()
        dir_to_use = count_dir or (config_input.get("source_path") or "").strip()
        # V2 schema: no source_path — derive from settings
        if not dir_to_use and interface_name:
            bucket = ((settings or {}).get("raw_bucket_prefix") or "").strip()
            dir_to_use = _join_path_parts(bucket, interface_name) if bucket else ""
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


def get_output_path(
    config_output: dict,
    base_path: str | None = None,
    *,
    interface_name: str = "",
    settings: dict | None = None,
) -> str:
    """
    Resolve output path from config.

    For the v2 schema (``dataset_name`` present, no ``source_path``) the path
    is derived as  <curated_bucket_prefix>/<interface>/<FREQUENCY>/<YYYYMMDD>.

    For the v1 schema (``source_path`` present) the path is enriched with the
    frequency/date partition so the output lands in the correct daily bucket::

        <source_path>/<FREQUENCY>/<YYYYMMDD>

    ``target_file_name`` / ``dataset_name`` (single-file rename) is handled
    separately by the runner via ``_coalesce_to_named_file`` and is *not*
    included here.

    Legacy keys (``path``, ``s3_path``, ``dataset``) are returned as-is
    without partition injection — this keeps test-mode temp paths unchanged.

    Relative paths are resolved against ``base_path``.
    """
    # V2 schema: dataset_name only (no source_path) — derive from settings
    dataset_name = (config_output.get("dataset_name") or "").strip()
    if dataset_name and not (config_output.get("source_path") or "").strip():
        target_storage = (config_output.get("target_storage") or "s3").lower()
        if target_storage == "efs":
            bucket = ((settings or {}).get("efs_output_prefix") or "").strip()
        else:
            bucket = ((settings or {}).get("curated_bucket_prefix") or "").strip()
        base = _join_path_parts(bucket, interface_name) if interface_name else bucket
        frequency = (config_output.get("frequency") or "").strip()
        return _build_partitioned_path(base, "", frequency, "load_date()")

    # V1 schema: source_path with frequency/date partition
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


def get_validate_paths(
    logic: dict,
    *,
    interface_name: str = "",
    settings: dict | None = None,
) -> tuple[str, str, str, str]:
    """
    Derive validation output paths for a validate step.

    Returns:
        (validated_dir, validated_file, error_dir, error_file)

    V2 schema (``dataset_name`` present, no ``validated_path``):
        validated_dir = <validation_bucket_prefix>/<interface>/<FREQ>/<YYYYMMDD>
        error_dir     = <error_bucket_prefix>/<interface>/<FREQ>/<YYYYMMDD>

    V1 / legacy schema: returns ``validated_path`` and ``error_path`` as-is.
    """
    frequency = (logic.get("frequency") or "").strip()
    dataset_name = (
        logic.get("dataset_name") or logic.get("validated_file_name") or ""
    ).strip()
    error_dataset = (
        logic.get("error_dataset_name") or logic.get("error_file_name") or ""
    ).strip()

    has_legacy = bool((logic.get("validated_path") or "").strip())
    if dataset_name and not has_legacy:
        s = settings or {}
        val_bucket = (s.get("validation_bucket_prefix") or "").strip()
        err_bucket = (s.get("error_bucket_prefix") or "").strip()
        val_base = _join_path_parts(val_bucket, interface_name) if interface_name else val_bucket
        err_base = _join_path_parts(err_bucket, interface_name) if interface_name else err_bucket
        val_dir = _build_partitioned_path(val_base, "", frequency, "load_date()")
        err_dir = _build_partitioned_path(err_base, "", frequency, "load_date()")
        return val_dir, dataset_name, err_dir, error_dataset

    # Legacy
    val_path = (logic.get("validated_path") or "").strip()
    err_path = (logic.get("error_path") or "").strip()
    return val_path, dataset_name, err_path, error_dataset
