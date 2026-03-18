"""
Configuration-driven Spark DataFrame transformations (no Spark SQL).

Each step is implemented with the Spark DataFrame API:
- filter, join, groupBy/agg, select/withColumn, orderBy, unionByName.
The runner calls apply_transformation_step(step, datasets) for each step;
sources are resolved by name from the datasets registry and the result
is stored by output_alias for the next step.
"""

import logging
import os
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from .oracle_loader import write_df_to_oracle  # noqa: E402 — imported after std-lib

LOG = logging.getLogger(__name__)


def _spark_from_df(df: DataFrame) -> SparkSession:
    """Return the SparkSession associated with a DataFrame.

    DataFrame.sparkSession was added in PySpark 3.3.  On PySpark 3.1/3.2
    (Python 3.6 compatible) use sql_ctx.sparkSession instead.
    """
    try:
        return _spark_from_df(df)          # PySpark 3.3+
    except AttributeError:
        return df.sql_ctx.sparkSession  # PySpark 3.1 / 3.2


# ─────────────────────────────────────────────────────────────────────────────
# Protocol helpers
# ─────────────────────────────────────────────────────────────────────────────

def _extract_holiday_dates(usa_holidays) -> list:
    """Extract ISO date strings from old format (list of strings) or new structured
    format (list of dicts with active/name/date). Only active entries are included."""
    result = []
    for h in (usa_holidays or []):
        if isinstance(h, str):
            result.append(h)
        elif isinstance(h, dict) and h.get("active", True):
            d = h.get("date", "")
            if d:
                result.append(d)
    return result


def _get_previous_business_day(holidays=None, reference_date=None) -> "date":
    """Return the previous business day before *reference_date* (defaults to today),
    skipping weekends (Saturday, Sunday) and dates listed in the supplied holidays.

    Args:
        holidays: List of ISO date strings OR list of dicts {active, name, date}.
        reference_date: The date to look back from (defaults to date.today()).
    """
    from datetime import date, timedelta
    holiday_set: Set[str] = set(_extract_holiday_dates(holidays))
    d = (reference_date or date.today()) - timedelta(days=1)
    while d.weekday() >= 5 or d.isoformat() in holiday_set:
        d -= timedelta(days=1)
    return d


def _spark_to_py_strptime(spark_fmt: str) -> str:
    """Convert a Spark/SQL date format string to a Python strptime format.

    Handles both common cases: lowercase ``yyyyMMdd`` and uppercase ``YYYYMMDD``
    (the latter is used in COBOL-origin configs).  Longest patterns are replaced
    first to prevent partial matches.
    """
    f = spark_fmt.strip()
    # Year — 4-digit then 2-digit, both cases
    f = f.replace("YYYY", "%Y").replace("yyyy", "%Y")
    f = f.replace("YY",   "%y").replace("yy",   "%y")
    # Month (must come before day to avoid conflict)
    f = f.replace("MM", "%m")
    # Day — uppercase and lowercase
    f = f.replace("DD", "%d").replace("dd", "%d")
    # Hour / minute / second (common extras)
    f = f.replace("HH", "%H").replace("hh", "%I")
    f = f.replace("mm", "%M")
    f = f.replace("ss", "%S").replace("SS", "%S")
    return f


def _is_s3_path(path: str) -> bool:
    """Return True if the path refers to an S3 location."""
    return path.startswith("s3://") or path.startswith("s3a://") or path.startswith("s3n://")


def _s3_rename_part_to_named(spark, staging_dir: str, final_path: str, output_dir: str) -> None:
    """Promote the single part-file written to *staging_dir* to *final_path*.

    Uses the Hadoop FileSystem API (via PySpark's JVM bridge) so that the
    rename is an atomic server-side operation — no data is re-transferred.
    Works for s3://, s3a://, and s3n:// prefixes.

    Raises RuntimeError if no part file is found in the staging directory.
    """
    sc         = spark.sparkContext
    jvm        = sc._jvm
    hconf      = sc._jsc.hadoopConfiguration()
    HPath      = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem
    FileUtil   = jvm.org.apache.hadoop.fs.FileUtil
    URI        = jvm.java.net.URI

    staging_hpath = HPath(staging_dir)
    staging_fs    = FileSystem.get(URI.create(staging_dir), hconf)

    # Locate the single part-file (ignore _SUCCESS, .crc, etc.)
    part_path = None
    for status in staging_fs.listStatus(staging_hpath):
        fname = status.getPath().getName()
        if (fname.startswith("part-") and
                not fname.startswith(".") and
                not fname.endswith(".crc")):
            part_path = status.getPath()
            break

    if part_path is None:
        staging_fs.delete(staging_hpath, True)
        raise RuntimeError(
            f"[S3_WRITE] No part file found in S3 staging directory: {staging_dir}"
        )

    target_hpath  = HPath(final_path)
    target_dir_hp = HPath(output_dir)
    target_fs     = FileSystem.get(URI.create(output_dir), hconf)

    target_fs.mkdirs(target_dir_hp)
    if target_fs.exists(target_hpath):
        target_fs.delete(target_hpath, False)

    # Server-side copy then delete source (atomic on most S3 configurations)
    FileUtil.copy(staging_fs, part_path, target_fs, target_hpath, True, hconf)
    staging_fs.delete(staging_hpath, True)
    LOG.info("[S3_WRITE] Written '%s' → %s", part_path.getName(), final_path)


def _write_df_to_path(df: DataFrame, path: str, mode: str = "append", file_name: str = "") -> None:
    """Write a Spark DataFrame to path.

    When *file_name* is provided the DataFrame is coalesced to a single
    partition, written to a temporary staging directory, and the resulting
    part-file is renamed to *file_name* inside *path*.  The staging directory
    and all Spark artefacts (_SUCCESS, etc.) are removed automatically,
    leaving only the single named file.

    When *file_name* is absent the DataFrame is written as Parquet (original
    behaviour, preserved for backward compatibility).
    """
    actual_path = path
    if actual_path.startswith("file://"):
        actual_path = actual_path[7:]   # strip leading file://

    if file_name and not _is_s3_path(actual_path):
        # ── Local single-file write (named DAT/output file) ──────────────────
        import platform as _platform
        local_dir = Path(actual_path)
        local_dir.mkdir(parents=True, exist_ok=True)
        # Unique temp dir name derived from the target file name
        tmp_dir = local_dir / ("_val_tmp_" + file_name.replace(".", "_"))
        if tmp_dir.exists():
            shutil.rmtree(str(tmp_dir))
        tmp_path_str = str(tmp_dir)
        if _platform.system().lower() == "windows":
            tmp_path_str = tmp_path_str.replace("\\", "/")
        # Write pipe-delimited text (no header) — produces exactly the data rows
        (df.coalesce(1)
           .write.mode("overwrite")
           .option("header", "false")
           .option("sep", "|")
           .csv(tmp_path_str))
        part_files = sorted(tmp_dir.glob("part-*.csv"))
        if not part_files:
            raise RuntimeError(
                f"[VALIDATE_WRITE] No CSV part file found in temp dir '{tmp_dir}'"
            )
        dest_file = local_dir / file_name
        shutil.move(str(part_files[0]), str(dest_file))
        shutil.rmtree(str(tmp_dir))
        LOG.info("[VALIDATE_WRITE] Written '%s' to '%s'.", file_name, actual_path)

    elif file_name and _is_s3_path(actual_path):
        # ── S3: stage to temp prefix then rename via Hadoop FileSystem API ───
        tmp_s3 = actual_path.rstrip("/") + "/_val_tmp_" + file_name.replace(".", "_")
        (df.coalesce(1)
           .write.mode("overwrite")
           .option("header", "false")
           .option("sep", "|")
           .csv(tmp_s3))
        final_s3 = actual_path.rstrip("/") + "/" + file_name
        _s3_rename_part_to_named(_spark_from_df(df), tmp_s3, final_s3, actual_path)
        LOG.info("[VALIDATE_WRITE] Written '%s' to S3 path '%s'.", file_name, actual_path)

    else:
        # ── Original behaviour: Parquet directory write ───────────────────────
        df.write.mode(mode).parquet(actual_path)


def _write_fixed_width_to_path(
    df: "DataFrame",
    fields: List[dict],
    path: str,
    file_name: str,
    record_length: int = 0,
) -> None:
    """
    Write a Spark DataFrame as a single named fixed-width flat file.

    Mirrors the fixed-width formatting logic in DataFlowRunner._write_fixed_width()
    so that validated / error outputs use the exact same layout as the source input
    or curated output without requiring a dependency on the runner class.

    Each entry in *fields* must have at minimum:
        {"name": "TXN-ID", "type": "string", "length": 10}

    A staging temp-dir is used so the result is a single named file with no
    Spark artefacts (_SUCCESS, part-* files) in the destination directory.
    """
    actual_path = path
    if actual_path.startswith("file://"):
        actual_path = actual_path[7:]

    if not fields:
        LOG.warning(
            "[FIXED_WRITE] No field definitions provided; cannot write fixed-width "
            "file '%s' at '%s'. Skipping.", file_name, actual_path
        )
        return

    # ── Build a single fixed-width string column ──────────────────────────────
    line_expr = None
    for f in fields:
        col_name = (f.get("name") or "").replace("-", "_")
        length   = int(f.get("length") or 1)
        ftype    = (f.get("type") or "string").lower()

        if col_name not in df.columns:
            # Column not present (e.g. annotation columns on error df) — pad blanks
            piece = F.lpad(F.lit(""), length, " ")
        elif ftype in ("int", "integer", "long", "bigint", "double",
                       "float", "decimal", "number"):
            # Numeric: right-justify, truncate if too long
            piece = F.rpad(
                F.substring(F.lpad(F.col(col_name).cast("string"), length, " "), 1, length),
                length, " ",
            )
        else:
            # String: left-justify, pad / truncate to declared length
            piece = F.rpad(F.substring(F.col(col_name).cast("string"), 1, length), length, " ")

        line_expr = piece if line_expr is None else F.concat(line_expr, piece)

    if line_expr is None:
        LOG.warning("[FIXED_WRITE] All field expressions were None; skipping write of '%s'.", file_name)
        return

    if record_length > 0:
        line_expr = F.rpad(F.substring(line_expr, 1, record_length), record_length, " ")

    df_fixed = df.select(line_expr.alias("value"))

    # ── Write as a single named file using staging + rename ───────────────────
    if _is_s3_path(actual_path):
        # ── S3: stage to temp prefix then rename via Hadoop FileSystem API ───
        tmp_s3 = actual_path.rstrip("/") + "/_fw_tmp_" + file_name.replace(".", "_")
        df_fixed.coalesce(1).write.mode("overwrite").text(tmp_s3)
        final_s3 = actual_path.rstrip("/") + "/" + file_name
        _s3_rename_part_to_named(_spark_from_df(df_fixed), tmp_s3, final_s3, actual_path)
        LOG.info("[FIXED_WRITE] Written '%s' to S3 path '%s'.", file_name, actual_path)
    else:
        import platform as _platform
        local_dir = Path(actual_path)
        local_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir = local_dir / ("_fw_tmp_" + file_name.replace(".", "_"))
        if tmp_dir.exists():
            shutil.rmtree(str(tmp_dir))
        tmp_path_str = str(tmp_dir)
        if _platform.system().lower() == "windows":
            tmp_path_str = tmp_path_str.replace("\\", "/")

        df_fixed.coalesce(1).write.mode("overwrite").text(tmp_path_str)

        # Find the part file Spark produced (no extension for text output)
        part_files = sorted(
            p for p in tmp_dir.glob("part-*")
            if not p.name.startswith(".") and not p.suffix == ".crc"
        )
        if not part_files:
            shutil.rmtree(str(tmp_dir))
            raise RuntimeError(
                f"[FIXED_WRITE] No part file found in temp dir '{tmp_dir}'"
            )
        dest_file = local_dir / file_name
        shutil.move(str(part_files[0]), str(dest_file))
        shutil.rmtree(str(tmp_dir))
        LOG.info("[FIXED_WRITE] Fixed-width file written to '%s'.", dest_file)


def _copy_file_to_dir(src: str, dest_dir: str) -> None:
    """
    Copy a single control file to dest_dir.

    - If dest_dir is an S3 path and boto3 is available the file is uploaded.
    - Otherwise a local shutil copy is performed.
    - Errors are logged as warnings (non-fatal).
    """
    if not src or not dest_dir:
        return
    try:
        filename = Path(src).name if not _is_s3_path(src) else src.rstrip("/").split("/")[-1]
        if _is_s3_path(dest_dir):
            # ── S3 destination via boto3 ────────────────────────────────────
            try:
                import boto3  # type: ignore
                s3 = boto3.client("s3")
                # Parse bucket and key from dest_dir
                dest_no_proto = dest_dir.replace("s3a://", "").replace("s3n://", "").replace("s3://", "")
                bucket, _, prefix = dest_no_proto.partition("/")
                dest_key = (prefix.rstrip("/") + "/" + filename) if prefix else filename
                if _is_s3_path(src):
                    # S3 → S3 copy
                    src_no_proto = src.replace("s3a://", "").replace("s3n://", "").replace("s3://", "")
                    src_bucket, _, src_key = src_no_proto.partition("/")
                    s3.copy_object(
                        CopySource={"Bucket": src_bucket, "Key": src_key},
                        Bucket=bucket, Key=dest_key,
                    )
                else:
                    # Local → S3
                    s3.upload_file(src, bucket, dest_key)
                LOG.info("[COPY_FILE] Copied control file %s → s3://%s/%s", src, bucket, dest_key)
            except ImportError:
                LOG.warning("[COPY_FILE] boto3 not installed; cannot copy control file to S3 path %s", dest_dir)
        else:
            # ── Local destination ──────────────────────────────────────────
            local_dest = dest_dir.replace("file://", "")
            os.makedirs(local_dest, exist_ok=True)
            dest_file = str(Path(local_dest) / filename)
            if _is_s3_path(src):
                LOG.warning("[COPY_FILE] Cannot copy from S3 src %s to local dest %s without boto3 streaming", src, local_dest)
            else:
                shutil.copy2(src, dest_file)
                LOG.info("[COPY_FILE] Copied control file %s → %s", src, dest_file)
    except Exception as exc:
        LOG.warning("[COPY_FILE] Failed to copy control file '%s' to '%s': %s", src, dest_dir, exc)


def _check_file_exists(path: str) -> bool:
    """
    Return True if the file at *path* exists, False otherwise.
    Supports local paths and S3 (s3://, s3a://, s3n://) paths.
    """
    if not path:
        return False
    if _is_s3_path(path):
        try:
            import boto3  # type: ignore
            s3 = boto3.client("s3")
            no_proto = (path
                        .replace("s3a://", "")
                        .replace("s3n://", "")
                        .replace("s3://", ""))
            bucket, _, key = no_proto.partition("/")
            if not key:
                LOG.warning("[FILE_CHECK] No object key found in S3 path: %s", path)
                return False
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except ImportError:
            LOG.warning("[FILE_CHECK] boto3 not installed; cannot check S3 file: %s", path)
            return False
        except Exception:
            return False
    else:
        local_path = path.replace("file://", "")
        return os.path.isfile(local_path)


def _raise_input_file_missing_incident(
    pipeline_name: str,
    input_name: str,
    file_path: str,
) -> None:
    """
    Fire a CRITICAL ServiceNow incident when a required input file is missing.
    Silently no-ops when MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are not configured.
    The job must still be aborted by the caller after this function returns.

    Set MOOGSOFT_DRY_RUN=true to simulate incident creation for testing.
    """
    endpoint = os.environ.get("MOOGSOFT_ENDPOINT", "")
    api_key  = os.environ.get("MOOGSOFT_API_KEY", "")
    dry_run  = os.environ.get("MOOGSOFT_DRY_RUN", "").strip().lower() in ("1", "true", "yes")

    # ── Dry-run / test mode ──────────────────────────────────────────────────
    if dry_run:
        import uuid
        simulated_key = f"DRY-RUN-{uuid.uuid4().hex[:8].upper()}"
        LOG.info(
            "[INCIDENT] ServiceNow incident created (dry-run mode) — "
            "pipeline='%s' input='%s' missing_file='%s' alert_key=%s",
            pipeline_name, input_name, file_path, simulated_key,
        )
        return

    # ── No credentials configured ────────────────────────────────────────────
    if not endpoint or not api_key:
        LOG.warning(
            "[INCIDENT] Input file missing for pipeline '%s' input '%s', "
            "but MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are not set — "
            "ServiceNow incident was NOT created.  "
            "Set these environment variables to enable automated incident creation.  "
            "To test incident logging without real credentials set MOOGSOFT_DRY_RUN=true.",
            pipeline_name, input_name,
        )
        return

    # ── Real incident creation ───────────────────────────────────────────────
    try:
        from .incident import MoogsoftIncidentConnector
        connector = MoogsoftIncidentConnector(endpoint=endpoint, api_key=api_key)
        alert_key = connector.create_input_file_missing_incident(
            pipeline_name=pipeline_name,
            input_name=input_name,
            file_path=file_path,
        )
        LOG.info(
            "[INCIDENT] ServiceNow incident created — "
            "pipeline='%s' input='%s' missing_file='%s' alert_key=%s",
            pipeline_name, input_name, file_path, alert_key,
        )
    except Exception as exc:
        LOG.warning("[INCIDENT] Failed to create input-file-missing incident: %s", exc)


def _raise_last_run_file_missing_incident(
    pipeline_name: str,
    step_id: str,
    file_path: str,
    partition_column: str,
) -> None:
    """
    Fire a ServiceNow incident when the last run date file is missing.
    Silently no-ops when MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are not configured.

    Set MOOGSOFT_DRY_RUN=true to simulate incident creation for testing.
    """
    endpoint = os.environ.get("MOOGSOFT_ENDPOINT", "")
    api_key  = os.environ.get("MOOGSOFT_API_KEY", "")
    dry_run  = os.environ.get("MOOGSOFT_DRY_RUN", "").strip().lower() in ("1", "true", "yes")

    # ── Dry-run / test mode ──────────────────────────────────────────────────
    if dry_run:
        import uuid
        simulated_key = f"DRY-RUN-{uuid.uuid4().hex[:8].upper()}"
        LOG.info(
            "[INCIDENT] ServiceNow incident created (dry-run mode) — "
            "pipeline='%s' step='%s' missing_file='%s' alert_key=%s",
            pipeline_name, step_id, file_path, simulated_key,
        )
        return

    # ── No credentials configured ────────────────────────────────────────────
    if not endpoint or not api_key:
        LOG.warning(
            "[INCIDENT] Last run file missing for pipeline '%s' step '%s', "
            "but MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are not set — "
            "ServiceNow incident was NOT created.  "
            "Set these environment variables to enable automated incident creation.  "
            "To test incident logging without real credentials set MOOGSOFT_DRY_RUN=true.",
            pipeline_name, step_id,
        )
        return

    # ── Real incident creation ───────────────────────────────────────────────
    try:
        from .incident import MoogsoftIncidentConnector
        connector = MoogsoftIncidentConnector(endpoint=endpoint, api_key=api_key)
        alert_key = connector.create_last_run_file_missing_incident(
            pipeline_name=pipeline_name,
            step_id=step_id,
            file_path=file_path,
            partition_column=partition_column,
        )
        LOG.info(
            "[INCIDENT] ServiceNow incident created — "
            "pipeline='%s' step='%s' missing_file='%s' alert_key=%s",
            pipeline_name, step_id, file_path, alert_key,
        )
    except Exception as exc:
        LOG.warning("[INCIDENT] Failed to create last-run-file-missing incident: %s", exc)


def _raise_prev_day_check_incident(
    pipeline_name: str,
    input_name: str,
    file_path: str,
    expected_date,
    actual_date,
) -> None:
    """
    Fire a CRITICAL ServiceNow incident when the previous-day header check fails.

    Either the previous business day's raw input file is missing or its header
    date does not match the expected business day.

    Silently no-ops when MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are not configured.
    Set MOOGSOFT_DRY_RUN=true to simulate incident creation for testing.

    Parameters
    ----------
    pipeline_name : str
        Name of the dataflow pipeline / configuration.
    input_name : str
        Logical input name as defined in the config (e.g. 'HOGAN-INPUT').
    file_path : str
        Full path to the previous business day's input file that was checked.
    expected_date : date or str
        The date the header should have contained (previous business day).
    actual_date : date or str
        The date actually found in the previous day file's header (or the error
        string when the file was missing).
    """
    endpoint = os.environ.get("MOOGSOFT_ENDPOINT", "")
    api_key  = os.environ.get("MOOGSOFT_API_KEY", "")
    dry_run  = os.environ.get("MOOGSOFT_DRY_RUN", "").strip().lower() in ("1", "true", "yes")

    expected_str = str(expected_date)
    actual_str   = str(actual_date)

    # ── Dry-run / test mode ──────────────────────────────────────────────────
    if dry_run:
        import uuid
        simulated_key = f"DRY-RUN-{uuid.uuid4().hex[:8].upper()}"
        LOG.info(
            "[INCIDENT] Prev-day check incident created (dry-run) — "
            "pipeline='%s' input='%s' expected='%s' actual='%s' alert_key=%s",
            pipeline_name, input_name, expected_str, actual_str, simulated_key,
        )
        return

    # ── No credentials configured ────────────────────────────────────────────
    if not endpoint or not api_key:
        LOG.warning(
            "[INCIDENT] Prev-day check failed for pipeline '%s' input '%s', "
            "but MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are not set — "
            "ServiceNow incident was NOT created.  "
            "Set these environment variables to enable automated incident creation.  "
            "To test incident logging without real credentials set MOOGSOFT_DRY_RUN=true.",
            pipeline_name, input_name,
        )
        return

    # ── Real incident creation ───────────────────────────────────────────────
    try:
        from .incident import MoogsoftIncidentConnector
        connector = MoogsoftIncidentConnector(endpoint=endpoint, api_key=api_key)
        alert_key = connector.create_prev_day_check_incident(
            pipeline_name=pipeline_name,
            input_name=input_name,
            file_path=file_path,
            expected_date=expected_str,
            actual_date=actual_str,
        )
        LOG.info(
            "[INCIDENT] Prev-day check incident created — "
            "pipeline='%s' input='%s' expected='%s' actual='%s' alert_key=%s",
            pipeline_name, input_name, expected_str, actual_str, alert_key,
        )
    except Exception as exc:
        LOG.warning("[INCIDENT] Failed to create prev-day check incident: %s", exc)


def _raise_record_count_check_incident(
    pipeline_name: str,
    step_id: str,
    input_file_path: str,
    expected_count,
    actual_count,
) -> None:
    """
    Fire a CRITICAL ServiceNow incident when the trailer record count does not
    match the actual number of data records loaded from the input file.

    Silently no-ops when MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are not configured.
    Set MOOGSOFT_DRY_RUN=true to simulate incident creation for testing.
    """
    endpoint = os.environ.get("MOOGSOFT_ENDPOINT", "")
    api_key  = os.environ.get("MOOGSOFT_API_KEY", "")
    dry_run  = os.environ.get("MOOGSOFT_DRY_RUN", "").strip().lower() in ("1", "true", "yes")

    if dry_run:
        import uuid
        simulated_key = f"DRY-RUN-{uuid.uuid4().hex[:8].upper()}"
        LOG.info(
            "[INCIDENT] Record-count check incident created (dry-run) — "
            "pipeline='%s' step='%s' expected=%s actual=%s alert_key=%s",
            pipeline_name, step_id, expected_count, actual_count, simulated_key,
        )
        return

    if not endpoint or not api_key:
        LOG.warning(
            "[INCIDENT] Record count check failed for pipeline '%s' step '%s' "
            "(expected=%s, actual=%s), but MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are not set — "
            "ServiceNow incident was NOT created.  "
            "Set these environment variables to enable automated incident creation.  "
            "To test incident logging without real credentials set MOOGSOFT_DRY_RUN=true.",
            pipeline_name, step_id, expected_count, actual_count,
        )
        return

    try:
        from .incident import MoogsoftIncidentConnector
        connector = MoogsoftIncidentConnector(endpoint=endpoint, api_key=api_key)
        alert_key = connector.create_record_count_check_incident(
            pipeline_name=pipeline_name,
            step_id=step_id,
            input_file_path=input_file_path,
            expected_count=str(expected_count),
            actual_count=str(actual_count),
        )
        LOG.info(
            "[INCIDENT] Record-count check incident created — "
            "pipeline='%s' step='%s' expected=%s actual=%s alert_key=%s",
            pipeline_name, step_id, expected_count, actual_count, alert_key,
        )
    except Exception as exc:
        LOG.warning("[INCIDENT] Failed to create record-count-check incident: %s", exc)


def _resolve_metadata_refs(expression: str, file_metadata: dict) -> str:
    """Replace ``first(FIELD_NAME)`` references with literal values from file metadata.

    When the config-engine UI uses the *effective_date* or *as_of_date* presets
    the stored expression contains ``first(INP-HDR-FILE-DATE)`` or
    ``last_day(to_date(first(INP-HDR-FILE-DATE),'yyyyMMdd'))``.  The DataFrame
    passed to :func:`_create_ctrl_file` only has *data* rows — headers are
    stripped by ``runner.py._load_fixed_width_input``.  This helper resolves
    the ``first(FIELD)`` references by looking up the actual value from
    ``file_metadata`` (populated from the header/trailer rows at load time).

    Compound expressions resolve naturally because only the inner ``first()``
    call is replaced, leaving outer functions intact as valid Spark SQL.
    """
    if not file_metadata or "first(" not in expression:
        return expression

    def _replace_first_ref(match: "re.Match") -> str:
        raw_field = match.group(1).strip()
        # runner.py normalises hyphens → underscores in metadata keys
        norm_field = raw_field.replace("-", "_")
        for _meta_key, meta_vals in file_metadata.items():
            if not isinstance(meta_vals, dict):
                continue
            # Try normalised name first, then raw (use 'in' to handle empty-string values)
            if norm_field in meta_vals:
                val = meta_vals[norm_field]
            elif raw_field in meta_vals:
                val = meta_vals[raw_field]
            else:
                continue
            escaped = str(val).replace("'", "\\'")
            LOG.debug("[CTRL_FILE] Resolved first(%s) → '%s' from %s", raw_field, escaped, _meta_key)
            return "'" + escaped + "'"
        LOG.warning(
            "[CTRL_FILE] Could not resolve field '%s' (norm='%s') from file_metadata: %s",
            raw_field, norm_field,
            {k: list(v.keys()) if isinstance(v, dict) else type(v).__name__
             for k, v in file_metadata.items()},
        )
        return match.group(0)  # leave unchanged — Spark will give a clear error

    resolved = re.sub(r'\bfirst\(([^)]+)\)', _replace_first_ref, expression)
    if resolved != expression:
        LOG.info("[CTRL_FILE] Resolved expression: '%s' → '%s'", expression, resolved)
    return resolved


def _create_ctrl_file(
    df: "DataFrame",
    ctrl_file_fields: List[dict],
    ctrl_output_path: str,
    step_id: str = "validate",
    ctrl_file_name: str = "",
    include_header: bool = False,
    file_metadata: Optional[dict] = None,
) -> None:
    """
    Compute control file field values from PySpark expressions and write
    the result as a single-row fixed-width file.

    Each entry in *ctrl_file_fields* is::

        {"name": "RECORD_COUNT", "type": "INTEGER", "expression": "count(*)", "length": 10}

    Supported expression forms:
    - Aggregation   : ``count(*)``, ``sum(amount)``, ``max(date_col)``
    - Scalar/literal: ``current_date()``, ``'FILENAME.DAT'``, ``42``
    - Column ref    : ``status_code`` (single column value — uses first row)

    The function evaluates every expression via PySpark's ``F.expr()`` inside a
    single ``.agg()`` call so only one Spark action is triggered.

    Fields are formatted fixed-width:
    - Numeric types (LONG, INT, INTEGER, BIGINT): right-justified, zero-padded
    - String types: left-justified, space-padded
    - Default lengths when not specified: numeric=15, string=20

    When *ctrl_file_name* is provided, a single named file is written to
    ``ctrl_output_path/<ctrl_file_name>``.  Otherwise the result is written
    as a Spark text directory directly into *ctrl_output_path*.

    *include_header* controls whether the field-name header row is written
    at the top of the control file.  Defaults to ``False`` (no header).
    """
    if not ctrl_file_fields:
        LOG.debug("[CTRL_FILE:%s] No ctrl_file_fields defined; skipping control file creation.", step_id)
        return
    if not ctrl_output_path:
        LOG.warning("[CTRL_FILE:%s] ctrl_output_path is empty; skipping control file creation.", step_id)
        return

    LOG.info("[CTRL_FILE:%s] Creating fixed-width control file at '%s' with %d field(s)%s.",
             step_id, ctrl_output_path, len(ctrl_file_fields),
             f" → '{ctrl_file_name}'" if ctrl_file_name else "")

    _DEFAULT_NUM_LEN = 15
    _DEFAULT_STR_LEN = 20

    try:
        # ── 1. Evaluate aggregation expressions ───────────────────────────
        agg_exprs   = []
        field_specs = []   # (name, ftype, length, begin, fmt, just_right) per valid field
        for field_def in ctrl_file_fields:
            name       = (field_def.get("name") or "").strip()
            expression = (field_def.get("expression") or "").strip()
            if not name:
                LOG.debug("[CTRL_FILE:%s] Skipping field with missing name: %s", step_id, field_def)
                continue
            ftype  = (field_def.get("type") or "STRING").upper()
            length = int(field_def.get("length") or 0)
            if not length:
                length = _DEFAULT_NUM_LEN if ftype in ("LONG", "INT", "INTEGER", "BIGINT") else _DEFAULT_STR_LEN
            if not expression:
                if ftype in ("LONG", "INT", "INTEGER", "BIGINT"):
                    expression = "count(*)"
                else:
                    expression = "cast(null as string)"
                LOG.debug(
                    "[CTRL_FILE:%s] Field '%s' has no expression; defaulting to '%s'.",
                    step_id, name, expression,
                )
            begin      = int(field_def.get("begin") or 0)
            fmt        = (field_def.get("format") or "").strip()
            just_right = bool(field_def.get("just_right", False))
            resolved_expr = _resolve_metadata_refs(expression, file_metadata or {})
            agg_exprs.append(F.expr(resolved_expr).alias(name))
            field_specs.append((name, ftype, length, begin, fmt, just_right))

        if not agg_exprs:
            LOG.warning("[CTRL_FILE:%s] No valid fields found; skipping control file.", step_id)
            return

        ctrl_df = df.agg(*agg_exprs)

        # ── 2. Collect single-row result and format in Python ─────────────
        ctrl_row = ctrl_df.collect()[0]

        def _fmt_ctrl_field(value, ftype: str, length: int, fmt: str, just_right: bool = False) -> str:
            """Format a single ctrl-file field value as a fixed-width string.

            For STRING fields, COBOL JUSTIFIED RIGHT (just_right=True) right-aligns
            the value within the field width (space-pad on the left), mirroring the
            COBOL JUSTIFIED RIGHT clause behaviour.  All other STRING fields are
            left-aligned.  Numeric types are always zero-padded right-justified.
            """
            if value is None:
                raw = ""
            else:
                raw = str(value)
            if ftype in ("LONG", "INT", "INTEGER", "BIGINT"):
                # Numeric: zero-pad right-justified
                digits = raw.split(".")[0] if "." in raw else raw
                return digits.zfill(length)[:length]
            if ftype in ("DOUBLE", "FLOAT", "DECIMAL"):
                return raw.rjust(length)[:length]
            if ftype in ("DATE", "TIMESTAMP"):
                default_spark_fmt = "yyyyMMdd" if ftype == "DATE" else "yyyyMMddHHmmss"
                py_fmt = _spark_to_py_strptime(fmt or default_spark_fmt)
                if hasattr(value, "strftime"):
                    raw = value.strftime(py_fmt)
                return raw[:length].ljust(length)
            # STRING — apply format as date format when value supports strftime
            if fmt and hasattr(value, "strftime"):
                py_fmt = _spark_to_py_strptime(fmt)
                raw = value.strftime(py_fmt)
            # JUSTIFIED RIGHT: right-align with space padding on the left
            if just_right:
                return raw[:length].rjust(length)
            return raw[:length].ljust(length)

        formatted_values = [
            _fmt_ctrl_field(ctrl_row[name], ftype, length, fmt, just_right)
            for name, ftype, length, begin, fmt, just_right in field_specs
        ]

        # ── 3. Build output line — absolute (begin > 0) or sequential ─────
        has_begin = any(b > 0 for _, _, _, b, _, _jr in field_specs)
        if has_begin:
            total_len = max(b + l - 1 for _, _, l, b, _, _jr in field_specs if b > 0)
            line_chars = [" "] * total_len
            cur_pos = 0
            for (name, ftype, length, begin, fmt, just_right), fval in zip(field_specs, formatted_values):
                pos = (begin - 1) if begin > 0 else cur_pos
                line_chars[pos:pos + length] = list(fval)
                if begin == 0:
                    cur_pos += length
            output_line = "".join(line_chars)
        else:
            output_line = "".join(formatted_values)

        # Wrap into a one-row Spark DataFrame so the existing write
        # infrastructure (S3 rename, local temp dir, etc.) is unchanged.
        fixed_df = _spark_from_df(ctrl_df).range(1).select(F.lit(output_line).alias("value"))

        # ── 3. Prepend header row if requested ────────────────────────────
        if include_header:
            if has_begin:
                hdr_chars = [" "] * total_len
                cur_pos_h = 0
                for name, _ftype, length, begin, _fmt, _jr in field_specs:
                    pos = (begin - 1) if begin > 0 else cur_pos_h
                    hdr_chars[pos:pos + length] = list((name[:length]).ljust(length))
                    if begin == 0:
                        cur_pos_h += length
                header_line = "".join(hdr_chars)
            else:
                header_line = "".join(
                    (name[:length]).ljust(length)
                    for name, _ftype, length, _begin, _fmt, _jr in field_specs
                )
            # Use range(1)+lit to avoid Python 3.12+ cloudpickle recursion.
            # Attach an explicit sort key so header is guaranteed to come first
            # regardless of Spark partition ordering (plain .union() is unordered).
            # NOTE: select("__row_ord__", "value") ensures column order matches
            # header_df so .union() (which is positional) aligns correctly.
            header_df = _spark_from_df(df).range(1).select(
                F.lit(0).alias("__row_ord__"),
                F.lit(header_line).alias("value"),
            )
            fixed_df = (
                header_df
                .union(
                    fixed_df
                    .withColumn("__row_ord__", F.lit(1))
                    .select("__row_ord__", "value")
                )
                .orderBy("__row_ord__")
                .drop("__row_ord__")
            )

        # ── 4. Resolve output path ────────────────────────────────────────
        actual_path = ctrl_output_path
        if actual_path.startswith("file://"):
            actual_path = actual_path[7:]

        import platform as _platform
        os_name = _platform.system().lower()

        if ctrl_file_name:
            # ── Write a single named file ──────────────────────────────────
            if _is_s3_path(actual_path):
                # ── S3: stage to temp prefix then rename via Hadoop FileSystem API ─
                tmp_s3 = actual_path.rstrip("/") + "/_ctrl_tmp_" + step_id
                fixed_df.coalesce(1).write.mode("overwrite").text(tmp_s3)
                final_s3 = actual_path.rstrip("/") + "/" + ctrl_file_name
                _s3_rename_part_to_named(_spark_from_df(fixed_df), tmp_s3, final_s3, actual_path)
                LOG.info("[CTRL_FILE:%s] Control file written to S3 path '%s'.", step_id, final_s3)
            else:
                local_dir = Path(actual_path)
                local_dir.mkdir(parents=True, exist_ok=True)
                tmp_dir = local_dir / ("_ctrl_tmp_" + step_id)
                if tmp_dir.exists():
                    shutil.rmtree(str(tmp_dir))
                tmp_path_str = str(tmp_dir)
                if os_name == "windows":
                    tmp_path_str = tmp_path_str.replace("\\", "/")
                fixed_df.coalesce(1).write.mode("overwrite").text(tmp_path_str)
                # Locate the text part file (exclude .crc / _SUCCESS)
                part_files = sorted(
                    p for p in tmp_dir.glob("part-*")
                    if p.suffix not in (".crc",) and p.is_file()
                )
                if not part_files:
                    raise RuntimeError(
                        f"[CTRL_FILE:{step_id}] No part file found in temp dir '{tmp_dir}'"
                    )
                dest_file = local_dir / ctrl_file_name
                shutil.move(str(part_files[0]), str(dest_file))
                shutil.rmtree(str(tmp_dir))
                LOG.info("[CTRL_FILE:%s] Fixed-width control file written to '%s'.", step_id, dest_file)
        else:
            # ── Write Spark text directory ─────────────────────────────────
            if _is_s3_path(actual_path):
                fixed_df.coalesce(1).write.mode("overwrite").text(actual_path)
            else:
                local_dir = Path(actual_path)
                local_dir.mkdir(parents=True, exist_ok=True)
                fixed_df.coalesce(1).write.mode("overwrite").text(
                    actual_path if os_name != "windows"
                    else actual_path.replace("\\", "/")
                )
            LOG.info("[CTRL_FILE:%s] Fixed-width control file written to '%s'.", step_id, ctrl_output_path)

    except Exception as exc:
        LOG.error("[CTRL_FILE:%s] Failed to create control file: %s", step_id, exc)
        raise RuntimeError(
            f"[CTRL_FILE:{step_id}] Control file creation failed at '{ctrl_output_path}': {exc}"
        ) from exc


def _raise_moogsoft_incident(pipeline_name: str, step_id: str, invalid_count: int,
                              error_samples: Optional[list] = None) -> None:
    """
    Fire a MoogSoft/ServiceNow incident for validation failure.

    Behaviour:
      - MOOGSOFT_DRY_RUN=true  → simulates incident creation and logs success
                                  (useful for testing without real credentials)
      - MOOGSOFT_ENDPOINT + MOOGSOFT_API_KEY set → creates a real incident
      - Neither set            → logs a WARNING and returns without creating anything
    """
    endpoint = os.environ.get("MOOGSOFT_ENDPOINT", "")
    api_key  = os.environ.get("MOOGSOFT_API_KEY", "")
    dry_run  = os.environ.get("MOOGSOFT_DRY_RUN", "").strip().lower() in ("1", "true", "yes")

    # ── Dry-run / test mode ──────────────────────────────────────────────────
    if dry_run:
        import uuid
        simulated_key = f"DRY-RUN-{uuid.uuid4().hex[:8].upper()}"
        LOG.info(
            "[INCIDENT] ServiceNow incident created (dry-run mode) — "
            "pipeline='%s' step='%s' invalid_rows=%d alert_key=%s  "
            "Errors: %s",
            pipeline_name, step_id, invalid_count, simulated_key,
            "; ".join(error_samples or []),
        )
        return

    # ── No credentials configured ────────────────────────────────────────────
    if not endpoint or not api_key:
        LOG.warning(
            "[INCIDENT] Validation failed (%d invalid row(s)) in pipeline '%s' step '%s', "
            "but MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are not set — "
            "ServiceNow incident was NOT created.  "
            "Set these environment variables to enable automated incident creation.  "
            "To test incident logging without real credentials set MOOGSOFT_DRY_RUN=true.",
            invalid_count, pipeline_name, step_id,
        )
        return

    # ── Real incident creation ───────────────────────────────────────────────
    try:
        from .incident import MoogsoftIncidentConnector
        connector = MoogsoftIncidentConnector(endpoint=endpoint, api_key=api_key)
        alert_key = connector.create_validation_incident(
            pipeline_name=pipeline_name,
            step_id=step_id,
            invalid_count=invalid_count,
            error_samples=error_samples or [],
        )
        LOG.info(
            "[INCIDENT] ServiceNow incident created — "
            "pipeline='%s' step='%s' invalid_rows=%d alert_key=%s",
            pipeline_name, step_id, invalid_count, alert_key,
        )
    except Exception as exc:
        LOG.warning("[INCIDENT] Failed to create MoogSoft incident: %s", exc)


def _col(name: str) -> str:
    """Convert COBOL field name (hyphens) to Spark column name (underscores)."""
    return name.replace("-", "_")


def _resolve_col(df: DataFrame, name: str) -> str:
    """Resolve column name in df; COBOL uses hyphens, Spark uses underscores.
    If no exact match, try columns from expressions (e.g. 'WS_DEBIT_TOTAL + TXN_AMT'
    when asking for 'WS_DEBIT_TOTAL') so config JSON works when steps reference
    expression-created columns."""
    c = _col(name)
    cols = [x for x in df.columns if x.replace("-", "_") == c]
    if cols:
        return cols[0]
    # Fallback: Spark may name expression results like "COL_A + COL_B"
    candidates = [
        x for x in df.columns
        if (x.replace("-", "_").startswith(c + " ") or
            x.replace("-", "_").startswith(c + "+") or
            x.replace("-", "_") == c)
    ]
    return candidates[0] if len(candidates) == 1 else c


def _column_exists(df: DataFrame, name: str) -> bool:
    """True if a column matching name (or resolved name) exists in df.
    Used for working-storage: mainframe temp variables may not exist yet."""
    resolved = _resolve_col(df, name)
    return resolved in df.columns


def _expression_to_column(df: DataFrame, expr_str: str, op: str = "move"):
    """Turn an expression string into a Spark Column: literal (number/quoted string) or column reference.
    Used for MOVE so that '16' or \"0\" becomes F.lit(16) / F.lit(0), not F.col('16')."""
    if not expr_str or not isinstance(expr_str, str):
        return F.lit(None)
    s = expr_str.strip()
    if not s:
        return F.lit(None)
    # Explicit literal from code parser
    # (code parser may set "literal": true and "value": 16)
    # Handled by caller passing e.get("value") when literal is True
    # Numeric literal: use F.lit so we don't try to resolve column "16"
    if s.lstrip("-").isdigit():
        return F.lit(int(s))
    try:
        if "." in s and s.lstrip("-").replace(".", "", 1).isdigit():
            return F.lit(float(s))
    except ValueError:
        pass
    # Quoted string literal: 'x' or "x"
    if (len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"')):
        return F.lit(s[1:-1])
    # Column reference
    src = _col(s)
    resolved = _resolve_col(df, src)
    if resolved in df.columns:
        return F.col(resolved)
    # Fallback: still a literal (e.g. alphanumeric code like "OK"); use as string
    return F.lit(s)


def _parse_condition(cond: str, df: DataFrame):
    """Parse a condition string like \"TXN_TYPE = 'DR'\" into a Spark column expression."""
    if not cond or not isinstance(cond, str):
        return None
    cond = cond.strip()
    m = re.match(r"(\w+)\s*=\s*['\"]([^'\"]*)['\"]", cond, re.IGNORECASE)
    if m:
        col_name = _resolve_col(df, m.group(1))
        val = m.group(2)
        return F.col(col_name) == val
    return None


def apply_transformation_step(
    step: dict,
    datasets: Dict[str, "DataFrame"],
) -> Tuple[Optional[str], Optional["DataFrame"]]:
    """
    Apply one transformation step: resolve source dataset(s) from the registry,
    run a single Spark DataFrame transformation (filter, join, aggregate, select, etc.),
    return (output_alias, result_df). The caller stores the result in the registry
    so the next step can use it. No Spark SQL; uses only DataFrame API.
    """
    executor = MainframeTransformationExecutor(datasets)
    return executor.apply_step(step)


class MainframeTransformationExecutor:
    """
    Applies one step at a time: reads source DataFrame(s) from the registry,
    runs the Spark transformation for this step type, returns the result.
    """

    def __init__(self, dfs: Dict[str, DataFrame]):
        """
        Args:
            dfs: Registry of named DataFrames (inputs + outputs of previous steps).
                  This dict is updated in place when a step produces an output (same dict as runner's datasets).
        """
        self.dfs = dfs  # use same dict so step output is visible to next step

    def apply_step(self, step: dict) -> Tuple[Optional[str], Optional["DataFrame"]]:
        """
        Apply a single transformation step.

        Args:
            step: Dict with keys: id, type, source_inputs, logic, output_alias.

        Returns:
            (output_alias, result_df) or (None, None) on error.
        """
        step_type = (step.get("type") or "select").lower()
        logic = step.get("logic") or {}
        source_names = step.get("source_inputs") or []
        alias = step.get("output_alias") or step.get("id", "out")

        # Resolve source dataframe(s)
        source_df: Optional[DataFrame] = None
        for name in source_names:
            if name in self.dfs:
                source_df = self.dfs[name]
                break
        if source_df is None:
            LOG.warning("No source found for step %s, sources: %s", step.get("id"), source_names)
            return None, None

        result: Optional[DataFrame] = None

        if step_type == "filter":
            result = self._apply_filter(source_df, logic)
        elif step_type == "join":
            result = self._apply_join(logic)
        elif step_type == "aggregate":
            result = self._apply_aggregate(source_df, logic)
        elif step_type == "union":
            result = self._apply_union(logic, step)
        elif step_type == "custom":
            op = (logic.get("operation") or logic.get("op") or "").lower()
            if op == "sort":
                result = self._apply_sort(source_df, logic)
            elif op == "merge":
                result = self._apply_merge(logic, step)
            else:
                result = source_df
        elif step_type == "validate":
            # Inject step context so the validator can reference them in incidents
            logic.setdefault("_step_id", step.get("id") or alias)
            logic.setdefault("_pipeline_name", step.get("pipeline_name", "unknown"))
            logic.setdefault("_source_inputs", step.get("source_inputs") or [])
            result = self._apply_validate(source_df, logic)
        elif step_type == "select":
            result = self._apply_select(source_df, logic)
        elif step_type == "ctrl_file":
            step_id = step.get("id") or alias
            self._apply_ctrl_file(source_df, logic, step_id)
            result = source_df   # pass-through so alias remains resolvable
        elif step_type == "oracle_write":
            # Oracle Write is a terminal sink — loads data into Oracle via SQL*Loader.
            # The source DataFrame is passed through unchanged so downstream steps
            # (if any) can still reference the same alias.
            pipeline_name = step.get("pipeline_name", "unknown")
            step_id       = step.get("id") or alias
            self._apply_oracle_write(source_df, logic, pipeline_name, step_id)
            result = source_df   # pass-through so alias remains resolvable
        else:
            result = source_df

        if result is not None:
            self.dfs[alias] = result
            return alias, result
        return None, None

    def _apply_filter(self, df: DataFrame, logic: dict) -> DataFrame:
        conditions = logic.get("conditions") or []
        # Support else_branch: filter where condition is False (invalid path → output_alias)
        if logic.get("else_branch") and isinstance(logic.get("value_list"), list):
            # Invalid path: keep rows where field NOT IN value_list
            field = _col(logic.get("field", ""))
            val_list = logic["value_list"]
            col_ref = F.col(_resolve_col(df, field))
            return df.filter(~col_ref.isin(val_list))
        if not conditions:
            return df
        cond_expr = None
        for c in conditions:
            field = _col(c.get("field", ""))
            op = (c.get("operation") or c.get("op") or "==")
            op = str(op).strip().lower()
            val = c.get("value")
            col_ref = F.col(_resolve_col(df, field))
            if op in ("in", "in_list"):
                vals = val if isinstance(val, (list, tuple)) else [val]
                expr = col_ref.isin(vals)
            elif op in ("not_in", "not_in_list", "nin"):
                vals = val if isinstance(val, (list, tuple)) else [val]
                expr = ~col_ref.isin(vals)
            elif op in (">", "gt", "greater", "greater_than"):
                expr = col_ref > val
            elif op in ("<", "lt", "less", "less_than"):
                expr = col_ref < val
            elif op in (">=", "ge", "greater_equal"):
                expr = col_ref >= val
            elif op in ("<=", "le", "less_equal"):
                expr = col_ref <= val
            elif op in ("!=", "<>", "ne", "not_equal"):
                expr = col_ref != val
            else:
                expr = col_ref == val
            cond_expr = expr if cond_expr is None else (cond_expr & expr)
        if cond_expr is not None:
            return df.filter(cond_expr)
        return df

    def _apply_join(self, logic: dict) -> Optional[DataFrame]:
        left_name = logic.get("left")
        right_name = logic.get("right")
        on_spec = logic.get("on") or []
        how = (logic.get("how") or "inner").lower()

        left_df = self.dfs.get(left_name) if left_name else None
        right_df = self.dfs.get(right_name) if right_name else None
        if left_df is None or right_df is None:
            return None

        join_expr = None
        for pair in on_spec:
            if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                lc = _col(str(pair[0]))
                rc = _col(str(pair[1]))
                ex = F.col(_resolve_col(left_df, lc)) == F.col(_resolve_col(right_df, rc))
                join_expr = ex if join_expr is None else (join_expr & ex)
            elif isinstance(pair, str):
                c = _col(pair)
                ex = F.col(_resolve_col(left_df, c)) == F.col(_resolve_col(right_df, c))
                join_expr = ex if join_expr is None else (join_expr & ex)
        if join_expr is None:
            return None
        return left_df.join(right_df, join_expr, how)

    def _apply_aggregate(self, df: DataFrame, logic: dict) -> DataFrame:
        group_cols = logic.get("group_by") or []
        aggs = logic.get("aggregations") or []

        g_cols = [F.col(_resolve_col(df, _col(x))) for x in group_cols]
        agg_exprs = []
        for a in aggs:
            f = a.get("field", "*")
            op = (a.get("operation") or a.get("op") or "sum").lower()
            alias = a.get("alias") or f"{op}_{_col(f)}"
            col_ref = F.col(_resolve_col(df, _col(f))) if f != "*" else None
            cond = a.get("condition")
            cond_expr = _parse_condition(cond, df) if cond else None
            if op in ("sum", "add"):
                if f != "*":
                    if cond_expr is not None:
                        agg_exprs.append(F.sum(F.when(cond_expr, col_ref).otherwise(0)).alias(alias))
                    else:
                        agg_exprs.append(F.sum(col_ref).alias(alias))
            elif op in ("count", "tallying"):
                if cond_expr is not None:
                    agg_exprs.append(F.sum(F.when(cond_expr, 1).otherwise(0)).alias(alias))
                else:
                    agg_exprs.append(F.count(F.lit(1) if f == "*" else col_ref).alias(alias))
            elif op == "avg":
                if cond_expr is not None:
                    agg_exprs.append(F.avg(F.when(cond_expr, col_ref).otherwise(None)).alias(alias))
                else:
                    agg_exprs.append(F.avg(col_ref).alias(alias))
            elif op == "min":
                agg_exprs.append(F.min(col_ref).alias(alias))
            elif op == "max":
                agg_exprs.append(F.max(col_ref).alias(alias))
            else:
                if cond_expr is not None and col_ref is not None:
                    agg_exprs.append(F.sum(F.when(cond_expr, col_ref).otherwise(0)).alias(alias))
                else:
                    agg_exprs.append(F.sum(col_ref).alias(alias))
        if not agg_exprs:
            return df
        if g_cols:
            return df.groupBy(*g_cols).agg(*agg_exprs)
        return df.agg(*agg_exprs)

    def _apply_union(self, logic: dict, step: Optional[dict] = None) -> Optional[DataFrame]:
        source_names = logic.get("source_inputs") or (step.get("source_inputs") if step else [])
        frames = [self.dfs[n] for n in source_names if n in self.dfs]
        if len(frames) < 2:
            return frames[0] if frames else None
        return frames[0].unionByName(frames[1], allowMissingColumns=True)

    def _apply_sort(self, df: DataFrame, logic: dict) -> DataFrame:
        key = logic.get("key") or logic.get("keys", [])
        asc = (logic.get("ascending", True) if "ascending" in logic
               else "desc" not in (logic.get("order") or "asc").lower())
        if isinstance(key, str):
            key = [key]
        cols = [F.col(_resolve_col(df, _col(k))) for k in key]
        return df.orderBy(*cols, ascending=asc)

    def _apply_merge(self, logic: dict, step: Optional[dict] = None) -> Optional[DataFrame]:
        return self._apply_union(logic, step)

    def _apply_select(self, df: DataFrame, logic: dict) -> DataFrame:
        expressions = logic.get("expressions") or []
        columns = logic.get("columns")

        if columns:
            if columns == ["*"]:
                return df
            cols = [F.col(_resolve_col(df, _col(c))).alias(_col(c)) for c in columns]
            return df.select(*cols) if cols else df

        result = df
        for e in expressions:
            target = _col(e.get("target", ""))
            expr_str = e.get("expression", "")
            op = (e.get("operation") or e.get("op") or "move").lower()

            if op == "move":
                if e.get("literal") is True:
                    result = result.withColumn(target, F.lit(e.get("value", expr_str)))
                else:
                    result = result.withColumn(target, _expression_to_column(result, expr_str, op))

            elif op == "add":
                # Working storage: if target doesn't exist (e.g. WS_DEBIT_TOTAL), start from 0
                parts = expr_str.replace("+", " ").split()
                val_cols = [_resolve_col(result, _col(p)) for p in parts if p != target]
                val_consts = [int(p) for p in parts if p.lstrip("-").isdigit()]
                if _column_exists(result, target):
                    base = _resolve_col(result, target)
                    acc = F.col(base)
                else:
                    acc = F.lit(0)
                for v in val_cols:
                    if v in result.columns:
                        acc = acc + F.col(v)
                for v in val_consts:
                    acc = acc + F.lit(v)
                result = result.withColumn(target, acc)

            elif op == "subtract":
                src = _col(expr_str.strip().split()[0] if expr_str else "")
                src_resolved = _resolve_col(result, src)
                if _column_exists(result, target):
                    base = _resolve_col(result, target)
                    base_val = F.col(base)
                else:
                    base_val = F.lit(0)
                if src_resolved in result.columns:
                    result = result.withColumn(target, base_val - F.col(src_resolved))
                else:
                    result = result.withColumn(target, base_val)

            elif op == "multiply":
                parts = expr_str.replace("*", " ").split()
                other = next((_resolve_col(result, _col(p)) for p in parts if p != target), None)
                if _column_exists(result, target):
                    base = _resolve_col(result, target)
                    base_val = F.col(base)
                else:
                    base_val = F.lit(1)
                if other and other in result.columns:
                    result = result.withColumn(target, base_val * F.col(other))

            elif op == "divide":
                src = _col(expr_str.strip().split()[0] if expr_str else "")
                src_resolved = _resolve_col(result, src)
                if _column_exists(result, target):
                    base = _resolve_col(result, target)
                    base_val = F.col(base)
                else:
                    base_val = F.lit(0)
                if src_resolved in result.columns:
                    result = result.withColumn(
                        target,
                        F.when(F.col(src_resolved) != 0, base_val / F.col(src_resolved))
                        .otherwise(F.lit(None)),
                    )
                else:
                    result = result.withColumn(target, F.lit(None))

            elif op == "compute":
                # Normalize: COBOL often uses _ for minus in expressions; Spark expr needs -
                safe_expr = expr_str.replace("-", "_")
                safe_expr = re.sub(r"\s+_\s+", " - ", safe_expr)
                try:
                    result = result.withColumn(target, F.expr(safe_expr))
                except Exception:
                    result = result.withColumn(target, F.lit(expr_str))

            elif op == "initialize":
                default = e.get("value", 0)
                result = result.withColumn(target, F.lit(default))

            elif op == "string":
                delim = e.get("delimiter", "")
                parts = [p.strip() for p in expr_str.split() if p.strip() and p.upper() not in ("BY", "DELIMITED", "SIZE")]
                cols = [F.col(_resolve_col(result, _col(p))) for p in parts if p]
                if cols:
                    result = result.withColumn(target, F.concat_ws(delim, *cols))

            elif op == "unstring":
                delim = e.get("delimiter", ",")
                src = _col(expr_str.strip().split()[0] if expr_str else "")
                result = result.withColumn(target, F.split(F.col(_resolve_col(result, src)), delim).getItem(0))

            elif op == "inspect":
                src = _col(expr_str.strip().split()[0] if expr_str else "")
                before = e.get("before", "")
                after = e.get("after", "")
                result = result.withColumn(
                    target,
                    F.regexp_replace(F.col(_resolve_col(result, src)), F.lit(before), F.lit(after)),
                )

            else:
                try:
                    result = result.withColumn(target, F.expr(expr_str.replace("-", "_")))
                except Exception:
                    result = result.withColumn(target, F.lit(expr_str))

        return result

    # ------------------------------------------------------------------
    def _apply_oracle_write(
        self,
        df: DataFrame,
        logic: dict,
        pipeline_name: str = "unknown",
        step_id: str = "",
    ) -> None:
        """Load the source DataFrame into Oracle Database via SQL*Loader.

        Credentials are fetched from HashiCorp Vault at runtime — they are
        *never* stored in the dataflow config JSON.

        Steps
        -----
        1. Validate required config fields (host, service_name, table, vault_path).
        2. Retrieve Oracle username / password from HashiCorp Vault.
        3. Coalesce DataFrame → single CSV partition in a local temp directory.
        4. Generate a SQL*Loader .ctl control file from the DataFrame schema.
        5. Execute ``sqlldr`` as a subprocess.
        6. Clean up temp files.

        Args:
            df            : Source DataFrame (output of the previous step).
            logic         : ``oracle_write`` step logic dict (from config JSON).
            pipeline_name : Pipeline name for log / error messages.
            step_id       : Step ID for log / error messages.

        Raises:
            RuntimeError  : if config is incomplete, Vault is unreachable,
                            or SQL*Loader returns a fatal exit code.
        """
        LOG.info("[oracle_write:%s] Starting Oracle write for pipeline '%s'", step_id, pipeline_name)
        try:
            write_df_to_oracle(
                df=df,
                logic=logic,
                pipeline_name=pipeline_name,
                step_id=step_id,
            )
        except RuntimeError:
            raise   # already has context — re-raise as-is
        except Exception as exc:
            raise RuntimeError(
                f"[oracle_write:{step_id}] Unexpected error during Oracle write: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    def _apply_validate(self, df: DataFrame, logic: dict) -> DataFrame:
        """
        Data validation transformation.

        Checks each field against its declared rule:
          - data_type  : verifies the value can be cast to the expected type
          - max_length : verifies len(value) <= max_length
          - nullable   : when False, rejects null / empty-string values
          - format     : pattern-based check —
                          alpha        ^[A-Za-z\\s]+$
                          numeric      ^\\d+(\\.\\d+)?$
                          alphanumeric ^[A-Za-z0-9\\s]+$
                          date         to_date(value, date_format) must not be null
                          email        simple RFC-5321 shape
                          regex        user-supplied pattern

        fail_mode controls what happens when a row fails:
          FLAG  (default) – adds ``_validation_errors`` (semicolon-joined messages)
                            and ``_is_valid`` (boolean) to every row
          DROP            – returns only rows where all rules pass; invalid rows
                            are written to error_path and count is logged
          ABORT           – raises RuntimeError listing the first bad rows

        Optional path fields (from logic dict):
          error_path        – path to write invalid rows (s3:// or local)
          validated_path    – path to write valid rows (s3:// or local)

        On validation failure (invalid_count > 0), a MoogSoft/ServiceNow incident
        is raised automatically if MOOGSOFT_ENDPOINT / MOOGSOFT_API_KEY are configured.
        """
        rules             = logic.get("rules") or []
        fail_mode         = (logic.get("fail_mode") or "FLAG").upper()
        # Map UI alias FLAGGED → FLAG for backward compatibility
        if fail_mode == "FLAGGED":
            fail_mode = "FLAG"

        # ── Path resolution (v2 schema or legacy) ──────────────────────────
        from .config_loader import get_validate_paths as _gvp
        _iface   = logic.get("_interface_name", "")
        _settings = logic.get("_settings") or {}
        validated_path, validated_file_name, error_path, error_file_name = \
            _gvp(logic, interface_name=_iface, settings=_settings)

        # Source input field definitions injected by the runner so validated /
        # error files are written as true fixed-width (same layout as the input).
        _source_fields  = logic.get("_source_fields") or []
        _record_length  = int(logic.get("_record_length") or 0)
        pipeline_name   = logic.get("_pipeline_name", "unknown")
        step_id         = logic.get("_step_id", "validate")

        # Header/trailer metadata injected by the runner from fixed-width file processing.
        _file_metadata  = logic.get("_file_metadata") or {}

        def _reconcile_trailer_count(actual_count: int) -> None:
            """Warn when a trailer record-count field differs from the actual valid-row count."""
            for meta_key, meta_vals in _file_metadata.items():
                if not meta_key.endswith("_trailer"):
                    continue
                count_field = next(
                    (k for k in meta_vals if "count" in k.lower() or "cnt" in k.lower()), None
                )
                if not count_field:
                    continue
                try:
                    expected = int(meta_vals[count_field])
                except (ValueError, TypeError):
                    continue
                if expected != actual_count:
                    LOG.warning(
                        "[VALIDATE] %s: trailer record count MISMATCH — %s.%s=%d, actual_valid=%d",
                        step_id, meta_key, count_field, expected, actual_count,
                    )
                else:
                    LOG.info(
                        "[VALIDATE] %s: trailer record count VERIFIED — %s.%s=%d",
                        step_id, meta_key, count_field, expected,
                    )

        # ── Path partition (frequency / date sub-directory) — legacy only ──
        # V2 schema already has partitioning baked into the paths returned by
        # get_validate_paths().  Legacy configs still need it applied here.
        _frequency     = (logic.get("frequency")          or "").strip()
        _partition_col = (logic.get("path_partition_col") or "").strip()
        _has_legacy_path = bool((logic.get("validated_path") or "").strip())
        if _frequency and _has_legacy_path:
            from .config_loader import _build_partitioned_path as _bpp
            if validated_path:
                validated_path = _bpp(validated_path, "", _frequency, _partition_col)
            if error_path:
                error_path = _bpp(error_path, "", _frequency, _partition_col)

        # ── Last Run / Previous Day File Check ───────────────────────────────
        # Supports both new `previous_day_check` (auto-derived) and legacy
        # `last_run_check` (explicit paths).
        last_run_check     = bool(logic.get("last_run_check", False) or logic.get("previous_day_check", False))
        last_run_file_path = (logic.get("last_run_file_path") or "").strip()
        last_run_file_name = (logic.get("last_run_file_name") or "").strip()
        last_run_frequency = (logic.get("last_run_frequency") or "").strip().upper()
        partition_column   = (logic.get("partition_column") or "").strip()

        if last_run_check and partition_column:
            LOG.info(
                "[VALIDATE] last_run_check: using config-provided partition_column='%s' "
                "(prev-day auto-calc skipped)",
                partition_column,
            )

        # Use explicit previous_day_* keys written by the Studio (preferred over auto-derive)
        # previous_day_file_path points to the curated output of the previous day's run
        _prev_day_path = (logic.get("previous_day_file_path") or "").strip()
        _prev_day_name = (logic.get("previous_day_file_name") or "").strip()
        _prev_day_freq = (logic.get("previous_day_frequency") or "").strip().upper()
        _is_prev_day_auto = bool(logic.get("previous_day_check", False))
        if last_run_check and _prev_day_path:
            last_run_file_path = last_run_file_path or _prev_day_path
            # In previous_day_check (auto) mode the file name must come from the source
            # input config — _prev_day_name may hold an output dataset name by mistake.
            if not _is_prev_day_auto:
                last_run_file_name = last_run_file_name or _prev_day_name
            last_run_frequency = last_run_frequency or _prev_day_freq
            if not partition_column:
                _holidays = (logic.get("_settings") or {}).get("usa_holidays") or []
                # Expected previous business day — skips holidays only.
                # Weekends are valid days (files are loaded every day).
                _expected_date = _get_previous_business_day(_holidays)
                from datetime import date as _dt_date
                LOG.info(
                    "[VALIDATE] last_run_check: today=%s  expected_prev=%s  (holidays=%d, weekends skipped)",
                    _dt_date.today().isoformat(), _expected_date.isoformat(), len(_holidays),
                )
                # Build the previous day file path (resolve source dataset name as fallback).
                _src_cfg          = logic.get("_source_input_config") or {}
                _anticipated_freq = (last_run_frequency or _prev_day_freq or "").upper()
                _anticipated_base = last_run_file_path or _prev_day_path or ""
                # In auto mode fall back to the source input's own dataset name so the
                # path resolves to the actual raw file, not a bare directory.
                _anticipated_name = (
                    last_run_file_name or _prev_day_name
                    or ((_src_cfg.get("dataset_name") or _src_cfg.get("source_file_name") or "")
                        if _is_prev_day_auto else "")
                )
                if _anticipated_base and _anticipated_freq:
                    from .config_loader import _build_partitioned_path as _bpp_early
                    _anticipated_path = _bpp_early(
                        _anticipated_base, _anticipated_name,
                        _anticipated_freq, _expected_date.strftime("%Y%m%d"),
                    )
                else:
                    _anticipated_path = _anticipated_base + "/" + _anticipated_name
                LOG.info(
                    "[VALIDATE] Previous day file path (expected) → %s",
                    _anticipated_path,
                )

                _prev_day_hdr_field = (logic.get("previous_day_header_date_field") or "").strip()
                if _prev_day_hdr_field:
                    # ── Validate header date by reading the previous day's actual file ──
                    _pipeline  = logic.get("_pipeline_name") or ""
                    _step_id   = logic.get("_step_id") or ""
                    _hdr_fdef  = next(
                        (f for f in (_src_cfg.get("header_fields") or [])
                         if (f.get("name") or "").strip() == _prev_day_hdr_field),
                        None,
                    )
                    if not _hdr_fdef:
                        _msg = (
                            f"[VALIDATE] previous_day_header_date_field '{_prev_day_hdr_field}' "
                            f"not found in source input header_fields — cannot validate previous day."
                        )
                        LOG.error(_msg)
                        _raise_prev_day_check_incident(
                            pipeline_name=_pipeline, input_name=_step_id,
                            file_path="", expected_date=_expected_date, actual_date="field_not_found",
                        )
                        raise RuntimeError(_msg)

                    # Read the header date from the PREVIOUS DAY'S file (not today's metadata)
                    _hdr_start  = int(_hdr_fdef.get("start") or 1) - 1   # 0-based
                    _hdr_length = int(_hdr_fdef.get("length") or 8)
                    _prev_hdr_line = ""
                    if _anticipated_path.startswith("file://"):
                        _prev_local = _anticipated_path[len("file://"):]
                        try:
                            with open(_prev_local, "r") as _pf:
                                _prev_hdr_line = _pf.readline().rstrip("\r\n")
                            LOG.info(
                                "[VALIDATE] Read previous day header line from: %s",
                                _anticipated_path,
                            )
                        except (IOError, OSError) as _read_err:
                            _msg = (
                                f"[VALIDATE] Job aborted: cannot read previous day file "
                                f"'{_anticipated_path}': {_read_err}. "
                                f"Pipeline: '{_pipeline}', Step: '{_step_id}'."
                            )
                            LOG.error(_msg)
                            _raise_prev_day_check_incident(
                                pipeline_name=_pipeline, input_name=_step_id,
                                file_path=_anticipated_path, expected_date=_expected_date,
                                actual_date="FILE_MISSING",
                            )
                            raise RuntimeError(_msg)
                    else:
                        # S3 / HDFS — use the active Spark session via any loaded dataset
                        _spark_ds = next(iter(self.dfs.values()), None) if self.dfs else None
                        _spark_ss = _spark_from_df(_spark_ds) if _spark_ds is not None else None
                        if _spark_ss is None:
                            _msg = "[VALIDATE] No Spark session available to read previous day S3 file."
                            LOG.error(_msg)
                            raise RuntimeError(_msg)
                        try:
                            _prev_rows = (
                                _spark_ss.read.option("wholetext", "false").text(_anticipated_path)
                                .withColumn("value", F.regexp_replace(F.col("value"), r"\r$", ""))
                                .limit(1).select("value").collect()
                            )
                            _prev_hdr_line = _prev_rows[0].value if _prev_rows else ""
                            LOG.info(
                                "[VALIDATE] Read previous day header line from: %s",
                                _anticipated_path,
                            )
                        except Exception as _read_err:  # noqa: BLE001
                            _msg = (
                                f"[VALIDATE] Job aborted: cannot read previous day file "
                                f"'{_anticipated_path}': {_read_err}. "
                                f"Pipeline: '{_pipeline}', Step: '{_step_id}'."
                            )
                            LOG.error(_msg)
                            _raise_prev_day_check_incident(
                                pipeline_name=_pipeline, input_name=_step_id,
                                file_path=_anticipated_path, expected_date=_expected_date,
                                actual_date="FILE_MISSING",
                            )
                            raise RuntimeError(_msg)

                    _hdr_date_str = (
                        _prev_hdr_line[_hdr_start:_hdr_start + _hdr_length].strip()
                        if len(_prev_hdr_line) >= _hdr_start + _hdr_length
                        else ""
                    )
                    LOG.info(
                        "[VALIDATE] Header date extracted: field='%s', start=%d, length=%d, raw='%s'",
                        _prev_day_hdr_field, _hdr_start + 1, _hdr_length, _hdr_date_str,
                    )

                    if not _hdr_date_str:
                        _msg = (
                            f"[VALIDATE] Header field '{_prev_day_hdr_field}' could not be extracted "
                            f"from previous day file '{_anticipated_path}' — cannot validate previous day."
                        )
                        LOG.error(_msg)
                        _raise_prev_day_check_incident(
                            pipeline_name=_pipeline, input_name=_step_id,
                            file_path=_anticipated_path, expected_date=_expected_date,
                            actual_date="header_not_read",
                        )
                        raise RuntimeError(_msg)

                    # Parse the header date string
                    _spark_fmt = (_hdr_fdef.get("format") or "yyyyMMdd").strip()
                    _py_fmt    = _spark_to_py_strptime(_spark_fmt)
                    try:
                        from datetime import datetime as _hdr_dt
                        _actual_date = _hdr_dt.strptime(_hdr_date_str.strip(), _py_fmt).date()
                    except ValueError as _parse_exc:
                        # Distinguish impossible calendar dates (Feb 30, Apr 31 …)
                        # from genuine format mismatches (wrong chars / length).
                        # Python raises messages like "day is out of range for month"
                        # or "month must be in 1..12" for non-existent dates.
                        _exc_lower = str(_parse_exc).lower()
                        _is_impossible_date = any(
                            k in _exc_lower
                            for k in ("out of range", "day is", "month must", "invalid date")
                        )
                        if _is_impossible_date:
                            _msg = (
                                f"[VALIDATE] Invalid calendar date in header field "
                                f"'{_prev_day_hdr_field}': '{_hdr_date_str}' does not "
                                f"exist in the calendar (e.g. February has no day 30). "
                                f"Format: '{_spark_fmt}'."
                            )
                            _incident_actual = f"INVALID_DATE:{_hdr_date_str}"
                        else:
                            _msg = (
                                f"[VALIDATE] Cannot parse header date '{_hdr_date_str}' "
                                f"with format '{_py_fmt}' (Spark: '{_spark_fmt}')."
                            )
                            _incident_actual = f"parse_error:{_hdr_date_str}"
                        LOG.error(_msg)
                        _raise_prev_day_check_incident(
                            pipeline_name=_pipeline, input_name=_step_id,
                            file_path="", expected_date=_expected_date,
                            actual_date=_incident_actual,
                        )
                        raise RuntimeError(_msg)

                    # Compare
                    if _actual_date != _expected_date:
                        _msg = (
                            f"[VALIDATE] Previous day check FAILED: "
                            f"header field '{_prev_day_hdr_field}' = {_actual_date} "
                            f"but expected previous business day = {_expected_date}."
                        )
                        LOG.error(_msg)
                        _raise_prev_day_check_incident(
                            pipeline_name=_pipeline, input_name=_step_id,
                            file_path="", expected_date=_expected_date, actual_date=_actual_date,
                        )
                        raise RuntimeError(_msg)

                    LOG.info(
                        "[VALIDATE] Previous day check PASSED: header field '%s' = %s "
                        "(expected previous business day = %s).",
                        _prev_day_hdr_field, _actual_date, _expected_date,
                    )
                    partition_column = _actual_date.strftime("%Y%m%d")
                else:
                    partition_column = _expected_date.strftime("%Y%m%d")
                    LOG.info("[VALIDATE] Previous business day resolved to: %s (holidays=%d)", _expected_date, len(_holidays))

        # ── Record Count Check ────────────────────────────────────────────────
        _rc_check         = bool(logic.get("record_count_check", False))
        _rc_trailer_field = (logic.get("record_count_trailer_field") or "").strip()

        if _rc_check and _rc_trailer_field:
            _rc_src_cfg    = logic.get("_source_input_config") or {}
            _rc_input_name = (_rc_src_cfg.get("name") or "").strip()
            _rc_trailer    = _file_metadata.get(_rc_input_name + "_trailer") or {}

            # Support hyphen-normalised field names (e.g. RECORD-COUNT stored as RECORD_COUNT)
            _rc_raw_val = (
                _rc_trailer.get(_rc_trailer_field)
                or _rc_trailer.get(_rc_trailer_field.replace("-", "_"))
                or _rc_trailer.get(_rc_trailer_field.replace("_", "-"))
            )

            # Build a human-readable path for log / incident messages
            _rc_base = (logic.get("record_count_file_path") or _rc_src_cfg.get("source_path") or "").rstrip("/")
            _rc_freq = (logic.get("record_count_frequency") or _rc_src_cfg.get("frequency") or "").upper()
            _rc_file = (logic.get("record_count_file_name") or _rc_src_cfg.get("dataset_name") or _rc_src_cfg.get("source_file_name") or "")
            _rc_display_path = f"{_rc_base}/{_rc_freq}/{{YYYYMMDD}}/{_rc_file}" if _rc_freq else f"{_rc_base}/{_rc_file}"

            LOG.info(
                "[VALIDATE] Record count check enabled — input='%s' trailer_field='%s' path='%s'",
                _rc_input_name, _rc_trailer_field, _rc_display_path,
            )

            if _rc_raw_val is None:
                _msg = (
                    f"[VALIDATE] record_count_trailer_field '{_rc_trailer_field}' not found in "
                    f"trailer metadata for input '{_rc_input_name}'. "
                    f"Available trailer fields: {list(_rc_trailer.keys())}."
                )
                LOG.error(_msg)
                _raise_record_count_check_incident(
                    pipeline_name=pipeline_name, step_id=step_id,
                    input_file_path=_rc_display_path, expected_count=-1, actual_count="FIELD_NOT_FOUND",
                )
                raise RuntimeError(_msg)

            try:
                _rc_raw_int = int(str(_rc_raw_val).strip())
            except (ValueError, TypeError) as _rc_err:
                _msg = (
                    f"[VALIDATE] Cannot parse record count value '{_rc_raw_val}' "
                    f"from trailer field '{_rc_trailer_field}' as integer: {_rc_err}."
                )
                LOG.error(_msg)
                _raise_record_count_check_incident(
                    pipeline_name=pipeline_name, step_id=step_id,
                    input_file_path=_rc_display_path, expected_count=-1, actual_count=f"PARSE_ERROR:{_rc_raw_val}",
                )
                raise RuntimeError(_msg)

            # The trailer count field is an INCLUSIVE total: it counts the
            # header row(s), all data rows, and the trailer row(s) itself.
            # The source DataFrame only contains data rows (header + trailer
            # are stripped by the runner), so we must subtract header_count
            # and trailer_count from the raw value to get the data-only
            # expected count before comparing.
            _rc_hdr_count = int(_rc_src_cfg.get("header_count") or 0)
            _rc_trl_count = int(_rc_src_cfg.get("trailer_count") or 0)
            _rc_expected  = _rc_raw_int - _rc_hdr_count - _rc_trl_count

            LOG.info(
                "[VALIDATE] Record count: raw trailer value=%d, header_count=%d, "
                "trailer_count=%d → data-row expected=%d",
                _rc_raw_int, _rc_hdr_count, _rc_trl_count, _rc_expected,
            )

            _rc_source_inputs = logic.get("_source_inputs") or []
            _rc_df = self.dfs.get(_rc_source_inputs[0]) if _rc_source_inputs else None
            if _rc_df is None:
                _msg = f"[VALIDATE] Record count check: source input DataFrame not found for step '{step_id}'."
                LOG.error(_msg)
                raise RuntimeError(_msg)

            _rc_actual = _rc_df.count()

            if _rc_expected != _rc_actual:
                _msg = (
                    f"[VALIDATE] Record count check FAILED: "
                    f"trailer field '{_rc_trailer_field}' = {_rc_raw_int} "
                    f"(inclusive total; data-only expected = {_rc_expected}, "
                    f"header_count={_rc_hdr_count}, trailer_count={_rc_trl_count}) "
                    f"but actual loaded record count = {_rc_actual}. "
                    f"Input: '{_rc_display_path}'."
                )
                LOG.error(_msg)
                _raise_record_count_check_incident(
                    pipeline_name=pipeline_name, step_id=step_id,
                    input_file_path=_rc_display_path,
                    expected_count=_rc_expected, actual_count=_rc_actual,
                )
                raise RuntimeError(_msg)

            LOG.info(
                "[VALIDATE] Record count check PASSED: trailer field '%s' = %d "
                "(inclusive; data-only = %d), actual = %d. Input: '%s'.",
                _rc_trailer_field, _rc_raw_int, _rc_expected, _rc_actual, _rc_display_path,
            )

        # Auto-derive from source input config when previous_day_check is set
        # but explicit last_run fields are still absent
        if last_run_check and (not last_run_file_path or not last_run_file_name):
            src_cfg = logic.get("_source_input_config") or {}
            if src_cfg:
                last_run_file_name = last_run_file_name or src_cfg.get("dataset_name", "") or src_cfg.get("source_file_name", "")
                last_run_frequency = last_run_frequency or (src_cfg.get("frequency") or "DAILY").upper()
                partition_column   = partition_column or "date_sub(current_date(), 1)"
                # Use raw bucket (yesterday's input file) not curated bucket
                _raw_bucket     = (_settings.get("raw_bucket_prefix") or "").rstrip("/")
                _base_bucket    = _raw_bucket
                if _base_bucket and _iface:
                    last_run_file_path = f"{_base_bucket}/{_iface}"

        if last_run_check:
            if not last_run_file_path or not last_run_file_name:
                raise RuntimeError(
                    f"[VALIDATE] last_run_check/previous_day_check is enabled in step '{step_id}' but "
                    f"'last_run_file_path' or 'last_run_file_name' could not be determined. "
                    f"Ensure the source input has a dataset_name configured."
                )
            # Mainframe convention: fixed-width output files use .DAT extension
            if not last_run_file_name.upper().endswith(".DAT"):
                last_run_file_name = last_run_file_name + ".DAT"
            # Apply frequency / date-partition to the base path (same convention as
            # input and output files: <base>/<FREQUENCY>/<date>/<file>).
            if last_run_frequency and partition_column:
                from .config_loader import _build_partitioned_path as _bpp
                last_run_file_path = _bpp(last_run_file_path, "", last_run_frequency, partition_column)
                LOG.info(
                    "[VALIDATE] Last run file partitioned path: %s  (frequency=%s, partition=%s)",
                    last_run_file_path, last_run_frequency, partition_column,
                )
            # Build the full path to the last run date file
            if _is_s3_path(last_run_file_path):
                full_last_run_path = last_run_file_path.rstrip("/") + "/" + last_run_file_name
            else:
                full_last_run_path = str(
                    Path(last_run_file_path.replace("file://", "")) / last_run_file_name
                )

            LOG.info("[VALIDATE] Checking for last run date file: %s", full_last_run_path)

            if not _check_file_exists(full_last_run_path):
                LOG.error(
                    "[VALIDATE] Previous day file not found at '%s'. "
                    "Creating ServiceNow incident and aborting job.",
                    full_last_run_path,
                )
                _raise_last_run_file_missing_incident(
                    pipeline_name=pipeline_name,
                    step_id=step_id,
                    file_path=full_last_run_path,
                    partition_column=partition_column,
                )
                raise RuntimeError(
                    f"[VALIDATE] Job aborted: previous day file not found at "
                    f"'{full_last_run_path}'. Pipeline: '{pipeline_name}', "
                    f"Step: '{step_id}'. A ServiceNow incident has been raised."
                )

            LOG.info("[VALIDATE] Last run date file found: %s", full_last_run_path)

        # ── format → regex pattern map ──────────────────────────────────
        # Both lowercase (legacy) and uppercase (new UI) forms are supported.
        FORMAT_PATTERNS: Dict[str, str] = {
            "alpha":        r"^[A-Za-z\s]+$",
            "ALPHA":        r"^[A-Za-z\s]+$",
            "numeric":      r"^\d+(\.\d+)?$",
            "NUMERIC":      r"^\d+(\.\d+)?$",
            "alphanumeric": r"^[A-Za-z0-9\s]+$",
            "ALPHANUMERIC": r"^[A-Za-z0-9\s]+$",
            "email":        r"^[^\s@]+@[^\s@]+\.[^\s@]+$",
            "EMAIL":        r"^[^\s@]+@[^\s@]+\.[^\s@]+$",
        }

        # ── type → Spark cast type ───────────────────────────────────────
        from pyspark.sql.types import (
            IntegerType, LongType, DoubleType, FloatType, StringType
        )
        TYPE_CAST: dict = {
            "int":     IntegerType(),
            "integer": IntegerType(),
            "long":    LongType(),
            "bigint":  LongType(),
            "double":  DoubleType(),
            "float":   FloatType(),
            "decimal": DoubleType(),
            "number":  DoubleType(),
            # UI-facing simplified types (TEXT/NUMBER lowercased by engine)
            "text":    StringType(),   # from UI 'TEXT'
            # 'number' already maps above; 'date'/'timestamp' handled via to_date/to_timestamp
        }

        error_exprs: list = []   # each entry is a Column that yields an error
                                 # string or NULL when the check passes

        for rule in rules:
            field = (rule.get("field") or "").replace("-", "_")
            if not field:
                continue

            col_name = _resolve_col(df, field)
            if col_name not in df.columns:
                LOG.warning("[VALIDATE] Field '%s' not in DataFrame — rule skipped.", field)
                continue

            col_ref    = F.col(col_name)
            data_type  = (rule.get("data_type") or "string").lower()
            max_length = rule.get("max_length")
            nullable   = rule.get("nullable", True)   # True = null is allowed
            fmt        = (rule.get("format") or "any").lower()
            date_fmt   = rule.get("date_format") or rule.get("pattern") or "yyyyMMdd"
            pattern    = rule.get("pattern") or ""

            # 1. NULL / EMPTY check ──────────────────────────────────────
            if not nullable:
                error_exprs.append(
                    F.when(
                        col_ref.isNull() | (F.trim(col_ref.cast("string")) == ""),
                        F.lit(f"'{field}' must not be null or empty")
                    ).otherwise(F.lit(None).cast("string"))
                )

            # 2. DATA TYPE check ─────────────────────────────────────────
            if data_type in TYPE_CAST:
                cast_type = TYPE_CAST[data_type]
                # Guard: empty / whitespace-only strings must NOT trigger a type
                # error — they represent a null / missing value and are handled
                # by the nullable check above.  This matters most for fixed-width
                # inputs where all columns arrive as trimmed strings: an all-space
                # numeric field trims to "" which would fail the numeric cast and
                # produce a spurious "not a valid number" error if not guarded.
                error_exprs.append(
                    F.when(
                        col_ref.isNotNull() &
                        (F.trim(col_ref.cast("string")) != "") &
                        col_ref.cast(cast_type).isNull(),
                        F.lit(f"'{field}' is not a valid {data_type}")
                    ).otherwise(F.lit(None).cast("string"))
                )
            elif data_type == "date":
                fmt_str = rule.get("date_format") or "yyyyMMdd"
                error_exprs.append(
                    F.when(
                        col_ref.isNotNull() &
                        F.to_date(col_ref.cast("string"), fmt_str).isNull(),
                        F.lit(f"'{field}' is not a valid date (expected: {fmt_str})")
                    ).otherwise(F.lit(None).cast("string"))
                )
            elif data_type == "timestamp":
                fmt_str = rule.get("date_format") or "yyyyMMdd HH:mm:ss"
                error_exprs.append(
                    F.when(
                        col_ref.isNotNull() &
                        F.to_timestamp(col_ref.cast("string"), fmt_str).isNull(),
                        F.lit(f"'{field}' is not a valid timestamp (expected: {fmt_str})")
                    ).otherwise(F.lit(None).cast("string"))
                )

            # 3. MAX LENGTH check ────────────────────────────────────────
            if max_length is not None:
                try:
                    ml = int(max_length)
                    error_exprs.append(
                        F.when(
                            col_ref.isNotNull() &
                            (F.length(col_ref.cast("string")) > ml),
                            F.lit(f"'{field}' exceeds max length {ml}")
                        ).otherwise(F.lit(None).cast("string"))
                    )
                except (ValueError, TypeError):
                    LOG.warning("[VALIDATE] Invalid max_length '%s' for field '%s'.", max_length, field)

            # 4. FORMAT check ─────────────────────────────────────────────
            if fmt == "date":
                fmt_str = date_fmt or "yyyyMMdd"
                error_exprs.append(
                    F.when(
                        col_ref.isNotNull() &
                        F.to_date(col_ref.cast("string"), fmt_str).isNull(),
                        F.lit(f"'{field}' does not match date format '{fmt_str}'")
                    ).otherwise(F.lit(None).cast("string"))
                )
            elif fmt == "regex" and pattern:
                error_exprs.append(
                    F.when(
                        col_ref.isNotNull() &
                        (~col_ref.cast("string").rlike(pattern)),
                        F.lit(f"'{field}' does not match pattern '{pattern}'")
                    ).otherwise(F.lit(None).cast("string"))
                )
            elif fmt in FORMAT_PATTERNS:
                pat = FORMAT_PATTERNS[fmt]
                error_exprs.append(
                    F.when(
                        col_ref.isNotNull() &
                        (~col_ref.cast("string").rlike(pat)),
                        F.lit(f"'{field}' is not a valid {fmt} value")
                    ).otherwise(F.lit(None).cast("string"))
                )

        # ── No rules → pass through with metadata columns ───────────────
        if not error_exprs:
            LOG.info("[VALIDATE] No rules defined; passing through with _is_valid=True.")
            return (df
                    .withColumn("_is_valid", F.lit(True))
                    .withColumn("_validation_errors", F.lit("").cast("string")))

        # ── Combine all rule errors into a single string (concat_ws ignores NULLs) ──
        errors_col   = F.concat_ws("; ", *error_exprs)
        is_valid_col = (F.length(errors_col) == 0)

        if fail_mode == "DROP":
            valid_df      = df.filter(is_valid_col)
            invalid_df    = df.filter(~is_valid_col)
            invalid_count = invalid_df.count()
            LOG.info("[VALIDATE] DROP mode: %d invalid row(s) removed.", invalid_count)
            if error_path and invalid_count > 0:
                # Annotate invalid rows with error details before writing to the error path
                invalid_annotated = (invalid_df
                                     .withColumn("_validation_errors",
                                                 F.concat_ws("; ", *error_exprs))
                                     .withColumn("_is_valid", F.lit(False)))
                if _source_fields and error_file_name:
                    _write_fixed_width_to_path(
                        invalid_annotated, _source_fields, error_path, error_file_name, _record_length)
                else:
                    _write_df_to_path(invalid_annotated, error_path, mode="append", file_name=error_file_name)
                LOG.info(
                    "[VALIDATE] Wrote %d invalid row(s) to error path: %s",
                    invalid_count, error_path
                )
            if invalid_count > 0:
                # Collect sample error messages for incident
                sample_rows = (invalid_df
                               .withColumn("_validation_errors", F.concat_ws("; ", *error_exprs))
                               .select("_validation_errors").limit(5).collect())
                error_samples = [r["_validation_errors"] for r in sample_rows]
                _raise_moogsoft_incident(pipeline_name, step_id, invalid_count, error_samples)
                # Abort the job — any validation failure must stop the pipeline.
                raise RuntimeError(
                    f"[VALIDATE] Job aborted: {invalid_count} invalid row(s) detected "
                    f"in pipeline '{pipeline_name}', step '{step_id}'. "
                    f"All invalid records written to error path. "
                    f"A ServiceNow incident has been raised. "
                    f"First errors: {error_samples}"
                )
            if validated_path:
                valid_count = valid_df.count()
                _reconcile_trailer_count(valid_count)
                if _source_fields and validated_file_name:
                    _write_fixed_width_to_path(
                        valid_df, _source_fields, validated_path, validated_file_name, _record_length)
                else:
                    _write_df_to_path(valid_df, validated_path, mode="append", file_name=validated_file_name)
                LOG.info(
                    "[VALIDATE] Wrote %d valid row(s) to validated path: %s",
                    valid_count, validated_path
                )
            return valid_df

        # Build annotated DataFrame for FLAG / ABORT
        annotated = (df
                     .withColumn("_validation_errors", errors_col)
                     .withColumn("_is_valid", is_valid_col))

        if fail_mode == "ABORT":
            invalid_count = annotated.filter(~F.col("_is_valid")).count()
            if invalid_count > 0:
                # Write invalid records to error_path before aborting
                if error_path:
                    invalid_annotated = (annotated
                                         .filter(~F.col("_is_valid"))
                                         .withColumn("_is_valid", F.lit(False)))
                    if _source_fields and error_file_name:
                        _write_fixed_width_to_path(
                            invalid_annotated, _source_fields, error_path, error_file_name, _record_length)
                    else:
                        _write_df_to_path(invalid_annotated, error_path, mode="append", file_name=error_file_name)
                    LOG.info(
                        "[VALIDATE] ABORT: wrote %d invalid row(s) to error path: %s",
                        invalid_count, error_path,
                    )
                samples = (annotated
                           .filter(~F.col("_is_valid"))
                           .select("_validation_errors")
                           .limit(5)
                           .collect())
                msgs = [r["_validation_errors"] for r in samples]
                _raise_moogsoft_incident(pipeline_name, step_id, invalid_count, msgs)
                raise RuntimeError(
                    f"[VALIDATE] Data validation failed: {invalid_count} invalid row(s). "
                    f"First errors: {msgs}"
                )
            # ABORT mode — all rows valid.
            # Write valid records to validated layer so downstream consumers
            # can read from the validated path if needed.
            if validated_path:
                valid_count = df.count()
                _reconcile_trailer_count(valid_count)
                if _source_fields and validated_file_name:
                    _write_fixed_width_to_path(
                        df, _source_fields, validated_path, validated_file_name, _record_length)
                else:
                    _write_df_to_path(df, validated_path, mode="append", file_name=validated_file_name)
                LOG.info(
                    "[VALIDATE] ABORT: wrote %d valid row(s) to validated path: %s",
                    valid_count, validated_path,
                )
            return df

        # Default: FLAG — annotate rows, but abort if any are invalid.
        LOG.info("[VALIDATE] FLAG mode: validation columns _is_valid, _validation_errors added.")
        invalid_count = annotated.filter(~F.col("_is_valid")).count()
        if invalid_count > 0:
            # Write ALL invalid rows to error_path with full error annotations.
            if error_path:
                invalid_annotated = annotated.filter(~F.col("_is_valid"))
                if _source_fields and error_file_name:
                    _write_fixed_width_to_path(
                        invalid_annotated, _source_fields, error_path, error_file_name, _record_length)
                else:
                    _write_df_to_path(invalid_annotated, error_path, mode="append", file_name=error_file_name)
                LOG.info("[VALIDATE] Wrote %d invalid row(s) to error path: %s", invalid_count, error_path)
            sample_rows = (annotated
                           .filter(~F.col("_is_valid"))
                           .select("_validation_errors").limit(5).collect())
            error_samples = [r["_validation_errors"] for r in sample_rows]
            _raise_moogsoft_incident(pipeline_name, step_id, invalid_count, error_samples)
            # Abort the job — any validation failure must stop the pipeline.
            raise RuntimeError(
                f"[VALIDATE] Job aborted: {invalid_count} invalid row(s) detected "
                f"in pipeline '{pipeline_name}', step '{step_id}'. "
                f"All invalid records written to error path. "
                f"A ServiceNow incident has been raised. "
                f"First errors: {error_samples}"
            )
        # All rows valid — write to validated path and create ctrl file.
        if validated_path:
            valid_df    = annotated.filter(F.col("_is_valid"))
            valid_count = valid_df.count()
            _reconcile_trailer_count(valid_count)
            if _source_fields and validated_file_name:
                _write_fixed_width_to_path(
                    valid_df, _source_fields, validated_path, validated_file_name, _record_length)
            else:
                _write_df_to_path(valid_df, validated_path, mode="append", file_name=validated_file_name)
            LOG.info(
                "[VALIDATE] Wrote %d valid row(s) to validated path: %s",
                valid_count, validated_path
            )
        return annotated

    # ── Control File step ──────────────────────────────────────────────
    def _apply_ctrl_file(self, df: DataFrame, logic: dict, step_id: str) -> None:
        """
        Standalone control file creation step.  Reads ctrl_file_fields from
        the step logic, resolves the output path, and delegates to the shared
        ``_create_ctrl_file`` utility.

        The source DataFrame is **not** transformed — it is passed through
        unchanged so downstream steps can still reference the same alias.
        """
        ctrl_file_name    = (logic.get("ctrl_file_name") or "").strip()
        ctrl_file_fields  = logic.get("ctrl_file_fields") or []
        ctrl_include_hdr  = bool(logic.get("ctrl_include_header", False))

        if not ctrl_file_fields:
            LOG.warning("[CTRL_FILE:%s] No ctrl_file_fields in step logic; skipping.", step_id)
            return

        # Determine output path — prefer explicit, else derive from settings
        _file_metadata = getattr(self, "_file_metadata", None) or logic.get("_file_metadata")

        # Build the ctrl output path from the validated/curated bucket path
        # similar to how _apply_validate did it.
        _explicit_path = (logic.get("ctrl_output_path") or "").strip()
        _validated_path = (logic.get("validated_path") or "").strip()
        ctrl_output_path = _explicit_path or _validated_path

        if not ctrl_output_path:
            LOG.warning(
                "[CTRL_FILE:%s] No output path could be resolved; skipping ctrl file.",
                step_id,
            )
            return

        # Apply frequency/date partition if needed
        _frequency = (logic.get("frequency") or "").upper()
        if _frequency and _explicit_path:
            try:
                from .config_loader import _build_partitioned_path as _bpp
                _partition_col = logic.get("partition_column") or "load_date()"
                ctrl_output_path = _bpp(_explicit_path, "", _frequency, _partition_col)
            except Exception:
                pass  # fall back to unpartitioned path

        LOG.info("[CTRL_FILE:%s] Creating control file at '%s'", step_id, ctrl_output_path)
        _create_ctrl_file(
            df, ctrl_file_fields, ctrl_output_path, step_id,
            ctrl_file_name, ctrl_include_hdr,
            file_metadata=_file_metadata,
        )
