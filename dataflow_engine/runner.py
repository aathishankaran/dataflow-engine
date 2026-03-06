"""
PySpark Dataflow Runner - configuration-driven, step-by-step execution.
Python 3.12+ / Cross-platform (Linux, macOS, Windows).

Reads the config JSON and runs the dataflow as explicit steps:
  1. Load all Inputs into a registry of named DataFrames.
  2. For each transformation step in order:
     - Resolve source dataset(s) from the registry by name (source_inputs).
     - Apply one Spark DataFrame transformation (filter, join, groupBy.agg, select, etc.).
     - Store the result DataFrame in the registry under output_alias so the next step can use it.
  3. Write each configured Output from the registry to its path.

No Spark SQL is used; all steps use the Spark DataFrame API (filter, join, withColumn, etc.)
so the pipeline is easy to follow and the config JSON can be edited to change behavior.

Input/Output node properties:
  name           – node name / ID
  format         – FIXED | CSV | PARQUET | DELIMITED
  frequency      – DAILY | WEEKLY | MONTHLY (used for partitioned path layout)
  dataset_name   – file name with extension (v2 schema — derives path from settings)
  record_length  – total character width per record (FIXED format)
  header_count   – number of header lines to skip (FIXED/DELIMITED)
  trailer_count  – number of trailer lines to skip (FIXED)
  delimiter_char – field separator character (DELIMITED/CSV)
  write_mode     – OVERWRITE | APPEND (output only)
  source_inputs  – list of upstream step aliases (output only)

Schema field properties supported:
  name      – column name (hyphens normalized to underscores internally)
  type      – string | int | long | double | decimal | date | timestamp
  nullable  – bool (default true); used for schema validation / empty-DataFrame creation
  format    – optional format string (e.g. "yyyy-MM-dd" for date columns);
              used for format-aware type casting on CSV/delimited inputs
  start     – 1-based start position (fixed-width files only)
  length    – character width (fixed-width files only)

Control file support (fixed-width format):
  count_file_path  – path to a file whose first line is the expected record count
  control_fields   – list of {name, start, length} for structured control-file columns
  control_file_path – path to write the output control file (output nodes only)

OS / runtime detection:
  At startup the runner detects the host OS (Linux / macOS / Windows) via
  ``platform.system()`` and applies the appropriate Spark / Java / path
  configuration automatically.  The active OS is logged at INFO level.
"""

import logging
import os
import platform
import sys
from datetime import date as _date, datetime as _datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config_loader import (
    load_config,
    get_input_path,
    get_input_path_for_date,
    get_previous_business_day,
    get_control_file_path,
    get_output_path,
    get_frequency,
)
from .transformations import (
    apply_transformation_step,
    _is_s3_path,
    _raise_input_file_missing_incident,
    _raise_prev_day_check_incident,
    _create_ctrl_file,
)

LOG = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# OS detection & cross-platform Spark bootstrap
# ─────────────────────────────────────────────────────────────────────────────

_OS_NAME: str = platform.system().lower()   # 'linux', 'darwin', 'windows'
_IS_WINDOWS: bool = _OS_NAME == "windows"
_IS_MACOS:   bool = _OS_NAME == "darwin"
_IS_LINUX:   bool = _OS_NAME == "linux"

LOG.info("Dataflow Engine running on OS: %s (Python %s)", platform.system(), sys.version.split()[0])


def _configure_spark_for_os() -> None:
    """
    Apply OS-specific environment variables before SparkSession is created.

    - PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON are set to the current interpreter
      so the worker Python version always matches the driver (Python 3.12).
    - On Windows, HADOOP_HOME must point to a directory containing winutils.exe.
      Set the HADOOP_HOME env-var before calling this if you have a custom location.
    - On Linux / macOS no special settings are required beyond a valid JAVA_HOME.
    """
    python_exec = sys.executable

    # Always point PySpark workers at the same Python that launched the driver
    os.environ.setdefault("PYSPARK_PYTHON",        python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)

    if _IS_WINDOWS:
        # On Windows, Spark needs winutils.exe.  Use HADOOP_HOME if set; otherwise
        # try the bundled winutils from pyspark (available in PySpark ≥ 3.3).
        if not os.environ.get("HADOOP_HOME"):
            try:
                import pyspark  # type: ignore
                winutils_dir = Path(pyspark.__file__).parent / "bin"
                if (winutils_dir / "winutils.exe").exists():
                    os.environ["HADOOP_HOME"] = str(winutils_dir.parent)
                    LOG.debug("HADOOP_HOME auto-set to bundled winutils: %s", os.environ["HADOOP_HOME"])
            except Exception as _exc:
                LOG.debug("Could not locate bundled winutils: %s", _exc)

        # Hadoop temp dir — use Windows temp (avoid UNC path issues)
        if not os.environ.get("SPARK_LOCAL_DIRS"):
            tmp = os.environ.get("TEMP") or os.environ.get("TMP") or "C:\\Temp"
            spark_tmp = str(Path(tmp) / "spark_tmp")
            os.environ["SPARK_LOCAL_DIRS"] = spark_tmp
            Path(spark_tmp).mkdir(parents=True, exist_ok=True)
            LOG.debug("SPARK_LOCAL_DIRS set to: %s", spark_tmp)

    LOG.info("PySpark configured: PYSPARK_PYTHON=%s, OS=%s", python_exec, platform.system())


# Apply OS configuration once at import time (before any SparkSession creation)
_configure_spark_for_os()


# ─────────────────────────────────────────────────────────────────────────────
# Protocol-aware path helpers
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_path(path: str) -> str:
    """
    Strip ``file://`` prefix for local reads/writes so Spark's local FS driver
    is used.  S3 paths (s3://, s3a://, s3n://) are passed through unchanged.
    """
    if path.startswith("file://"):
        return path[7:]
    return path


def _configure_s3_if_needed(spark: SparkSession, path: str) -> None:
    """
    Apply S3 / hadoop-aws configuration when the path points to S3.

    Reads credentials from environment variables (compatible with AWS IAM roles,
    ~/.aws/credentials, and explicit key/secret pairs):
      AWS_ACCESS_KEY_ID      – optional explicit key (omitted when using IAM role)
      AWS_SECRET_ACCESS_KEY  – optional explicit secret
      AWS_SESSION_TOKEN      – optional session token
      AWS_REGION             – optional region (default: us-east-1)
    """
    if not _is_s3_path(path):
        return
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    # Use s3a:// (preferred) — re-write any legacy s3:// or s3n:// prefixes
    key    = os.environ.get("AWS_ACCESS_KEY_ID", "")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    token  = os.environ.get("AWS_SESSION_TOKEN", "")
    region = os.environ.get("AWS_REGION", "us-east-1")

    hadoop_conf.set("fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
    hadoop_conf.set("fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    if key and secret:
        hadoop_conf.set("fs.s3a.access.key", key)
        hadoop_conf.set("fs.s3a.secret.key", secret)
    if token:
        hadoop_conf.set("fs.s3a.session.token", token)
    # Normalise s3:// / s3n:// → s3a:// for hadoop-aws compatibility
    hadoop_conf.set("fs.s3.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def _effective_path(path: str) -> str:
    """
    Upgrade legacy ``s3://`` and ``s3n://`` prefixes to ``s3a://`` so
    hadoop-aws handles them, and strip ``file://`` for local paths.
    """
    if path.startswith("s3n://"):
        return "s3a://" + path[6:]
    if path.startswith("s3://") and not path.startswith("s3a://"):
        return "s3a://" + path[5:]
    return _normalise_path(path)

# ── Spark type mapping ────────────────────────────────────────────────────────
_SPARK_TYPE_MAP: dict[str, T.DataType] = {
    "string":    T.StringType(),
    "str":       T.StringType(),
    "int":       T.IntegerType(),
    "integer":   T.IntegerType(),
    "long":      T.LongType(),
    "bigint":    T.LongType(),
    "double":    T.DoubleType(),
    "float":     T.FloatType(),
    "decimal":   T.DecimalType(18, 4),
    "number":    T.DoubleType(),
    "date":      T.DateType(),
    "timestamp": T.TimestampType(),
    "boolean":   T.BooleanType(),
    "bool":      T.BooleanType(),
    "binary":    T.BinaryType(),
    "bytes":     T.BinaryType(),
}


def _spark_type(type_str: str) -> T.DataType:
    """Convert config type string to a Spark DataType."""
    return _SPARK_TYPE_MAP.get((type_str or "string").lower(), T.StringType())


class DataFlowRunner:
    """
    Runs a configuration-driven dataflow step-by-step.
    Each step consumes named dataset(s) from a registry and produces one dataset for the next step.
    """

    def __init__(
        self,
        spark: SparkSession,
        config_path: str | Path,
        base_path: str | Path | None = None,
        use_cobrix: bool = True,
        settings: dict | None = None,
    ):
        self.spark = spark
        self.config_path = Path(config_path)
        self.base_path = Path(base_path) if base_path else self.config_path.parent
        self.use_cobrix = use_cobrix
        self.config = load_config(self.config_path)
        self.interface_name = self.config_path.stem
        self.settings = settings or {}
        self.input_dfs: dict[str, DataFrame] = {}
        self.output_dfs: dict[str, DataFrame] = {}
        self._file_metadata: dict[str, dict] = {}  # header/trailer field values keyed by "{input_name}_header" / "{input_name}_trailer"

    # ─────────────────────────────────────────────────────────────────────────
    # READ INPUTS
    # ─────────────────────────────────────────────────────────────────────────

    def _read_input(self, name: str, cfg: dict) -> DataFrame:
        """
        Read input based on format (cobol/cobrix, parquet, csv, fixed, delimited).

        Protocol routing:
          s3:// / s3a:// / s3n://  – configured for hadoop-aws (S3AFileSystem)
          file://                   – stripped to local path
          relative / absolute       – used as-is by Spark's local FS
        """
        fmt = (cfg.get("format") or "cobol").lower()
        raw_path = get_input_path(
            cfg, str(self.base_path),
            interface_name=self.interface_name, settings=self.settings,
        )
        _configure_s3_if_needed(self.spark, raw_path)
        path = _effective_path(raw_path)
        cobrix_opts = cfg.get("cobrix") or {}

        if fmt == "cobol" and self.use_cobrix:
            try:
                copybook_path = cobrix_opts.get("copybook_path") or cfg.get("copybook") or ""
                copybook_full = (
                    str(self.base_path / copybook_path)
                    if copybook_path and not _is_s3_path(copybook_path)
                                        and not copybook_path.startswith("/")
                    else copybook_path
                )
                reader = (
                    self.spark.read.format("cobol")
                    .option("copybook", copybook_full or path)
                    .option("encoding", cobrix_opts.get("encoding", "cp037"))
                    .option("record_format", cobrix_opts.get("record_format", "F"))
                    .option("file_start_offset", str(cobrix_opts.get("file_start_offset", 0)))
                    .option("file_end_offset", str(cobrix_opts.get("file_end_offset", 0)))
                    .option(
                        "generate_record_id",
                        str(cobrix_opts.get("generate_record_id", False)).lower(),
                    )
                )
                return reader.load(path)
            except ImportError:
                LOG.warning("Cobrix not installed; falling back. Install: pip install spark-cobrix")
                fmt = "parquet"
            except Exception as e:
                LOG.warning("Cobrix read failed for %s: %s; trying parquet/csv", name, e)
                fmt = "parquet"

        if fmt == "parquet":
            return self.spark.read.parquet(path)
        if fmt in ("csv", "delimited"):
            delimiter     = cfg.get("delimiter_char") or cfg.get("delimiter") or ","
            header_count  = int(cfg.get("header_count")  or 0)
            trailer_count = int(cfg.get("trailer_count") or 0)
            if header_count > 0 or trailer_count > 0:
                # Skip leading/trailing non-data lines using DataFrame window functions
                # (no RDD lambda, no cloudpickle serialisation).
                from pyspark.sql.window import Window as _Window
                txt_df = (
                    self.spark.read
                    .option("wholetext", "false")
                    .text(path)
                    .withColumn("value", F.regexp_replace(F.col("value"), r"\r$", ""))
                    .withColumn("_rn", F.row_number().over(
                        _Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id())
                    ) - 1)  # 0-based
                )
                keep_from = header_count
                if trailer_count > 0:
                    total_lines = txt_df.count()
                    keep_to = total_lines - trailer_count
                    LOG.info(
                        "[CSV] %s: total_lines=%d  skip_header=%d  skip_trailer=%d  keep=[%d, %d)",
                        name, total_lines, header_count, trailer_count, keep_from, keep_to,
                    )
                    kept = txt_df.filter(
                        (F.col("_rn") >= keep_from) & (F.col("_rn") < keep_to)
                    )
                else:
                    LOG.info("[CSV] %s: skipping %d header line(s)", name, header_count)
                    kept = txt_df.filter(F.col("_rn") >= keep_from)
                # Collect filtered lines, write to a temp file, re-read as CSV
                import tempfile as _tmp, os as _os
                tmp_csv = _tmp.mktemp(suffix=".csv")
                try:
                    filtered_lines = [row.value for row in kept.orderBy("_rn").select("value").collect()]
                    with open(tmp_csv, "w", encoding="utf-8") as _fh:
                        _fh.write("\n".join(filtered_lines))
                    data_df = (
                        self.spark.read
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .option("sep", delimiter)
                        .csv(tmp_csv)
                    )
                finally:
                    try:
                        _os.unlink(tmp_csv)
                    except Exception:
                        pass
            else:
                data_df = (
                    self.spark.read
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("sep", delimiter)
                    .csv(path)
                )
            normalized = [c.replace("-", "_") for c in data_df.columns]
            if normalized != data_df.columns:
                data_df = data_df.toDF(*normalized)
            # Apply format-aware type casting from field definitions
            data_df = self._apply_field_formats(data_df, cfg.get("fields") or [])
            return data_df
        if fmt == "fixed":
            return self._read_fixed_width(name, cfg, path)

        # Unknown format — try parquet then CSV
        try:
            return self.spark.read.parquet(path)
        except Exception:
            return (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path)
            )

    def _read_fixed_width(self, name: str, cfg: dict, path: str) -> DataFrame:
        """
        Read a fixed-width (positional) flat file and extract named columns.

        Config fields used:
          fields          – list of {name, type, start (1-based), length, nullable, format}
          header_count    – number of header lines to skip at top of file (default 0)
          trailer_count   – number of trailer lines to skip at bottom of file (default 0)
          record_length   – optional expected record width; logs a warning when exceeded
          count_file_path – optional path to a control file whose first line is the
                            expected data-record count; a mismatch triggers a warning.
          control_fields  – optional list of {name, start, length} describing the
                            structured layout of the control file itself.
        """
        fields          = cfg.get("fields") or []
        header_count    = int(cfg.get("header_count") or 0)
        trailer_count   = int(cfg.get("trailer_count") or 0)
        record_length   = cfg.get("record_length")
        raw_ctrl_path   = get_control_file_path(
            cfg, str(self.base_path),
            interface_name=self.interface_name, settings=self.settings,
        )
        count_file_path = _effective_path(raw_ctrl_path) if raw_ctrl_path else ""
        control_fields  = cfg.get("control_fields") or []

        # ── 1. Read all lines as raw text ────────────────────────────────────
        # Stay in DataFrame land — no RDD Python lambda, no JVM↔Python crossing.
        # CR stripping is done with a native Spark function (no cloudpickle overhead).
        raw_df = (
            self.spark.read
            .option("wholetext", "false")
            .text(path)
            .withColumn("value", F.regexp_replace(F.col("value"), r"\r$", ""))
        )

        # ── 2. Skip header / trailer lines ───────────────────────────────────
        # Use pure DataFrame window functions — no RDD, no Python lambda, no
        # cloudpickle serialisation.  monotonically_increasing_id() gives a
        # per-partition row sequence that is stable for single-partition local
        # files (0, 1, 2 …) and preserves block-read order for multi-partition
        # S3/HDFS files when used as a sort key for row_number().
        raw_df_indexed: "DataFrame | None" = None
        total_lines: "int | None" = None
        if header_count > 0 or trailer_count > 0:
            from pyspark.sql.window import Window as _Window
            raw_df_indexed = raw_df.withColumn(
                "_rn",
                F.row_number().over(
                    _Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id())
                ) - 1,
            )  # 0-based row number
            keep_from = header_count
            if trailer_count > 0:
                total_lines = raw_df_indexed.count()
                keep_to = total_lines - trailer_count
                LOG.info(
                    "[FIXED] %s: total_lines=%d  skip_header=%d  skip_trailer=%d  keep=[%d, %d)",
                    name, total_lines, header_count, trailer_count, keep_from, keep_to,
                )
                raw_df = raw_df_indexed.filter(
                    (F.col("_rn") >= keep_from) & (F.col("_rn") < keep_to)
                ).drop("_rn")
            else:
                LOG.info("[FIXED] %s: skipping %d header line(s)", name, header_count)
                raw_df = raw_df_indexed.filter(F.col("_rn") >= keep_from).drop("_rn")

        # ── 2b. Extract header / trailer field values as metadata ────────────
        # Collect via DataFrame.collect() — no RDD lambda, no cloudpickle.
        header_fields  = cfg.get("header_fields") or []
        trailer_fields = cfg.get("trailer_fields") or []

        if header_fields and header_count > 0:
            try:
                if raw_df_indexed is not None:
                    hdr_rows = (
                        raw_df_indexed
                        .filter(F.col("_rn") < header_count)
                        .orderBy("_rn")
                        .select("value")
                        .collect()
                    )
                else:
                    hdr_rows = (
                        self.spark.read
                        .option("wholetext", "false").text(path)
                        .withColumn("value", F.regexp_replace(F.col("value"), r"\r$", ""))
                        .limit(header_count)
                        .select("value").collect()
                    )
                hdr_lines = [row.value for row in hdr_rows]
            except Exception:  # noqa: BLE001
                hdr_lines = []
            header_meta: dict = {}
            for line in hdr_lines:
                for f in header_fields:
                    fname  = (f.get("name") or "").replace("-", "_")
                    start  = int(f.get("start") or 1) - 1
                    length = int(f.get("length") or 1)
                    header_meta[fname] = line[start:start + length].strip() if len(line) >= start + length else ""
            self._file_metadata[name + "_header"] = header_meta
            LOG.info("[FIXED] %s: header metadata extracted: %s", name, header_meta)

        if trailer_fields and trailer_count > 0:
            try:
                if raw_df_indexed is not None and total_lines is not None:
                    trl_rows = (
                        raw_df_indexed
                        .filter(F.col("_rn") >= total_lines - trailer_count)
                        .orderBy("_rn")
                        .select("value")
                        .collect()
                    )
                else:
                    from pyspark.sql.window import Window as _Window
                    tmp_rn = (
                        self.spark.read
                        .option("wholetext", "false").text(path)
                        .withColumn("value", F.regexp_replace(F.col("value"), r"\r$", ""))
                        .withColumn("_rn", F.row_number().over(
                            _Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id())
                        ) - 1)
                    )
                    n_total = tmp_rn.count()
                    trl_rows = (
                        tmp_rn
                        .filter(F.col("_rn") >= n_total - trailer_count)
                        .orderBy("_rn")
                        .select("value")
                        .collect()
                    )
                trl_lines = [row.value for row in trl_rows]
            except Exception:  # noqa: BLE001
                trl_lines = []
            trailer_meta: dict = {}
            for line in trl_lines:
                for f in trailer_fields:
                    fname  = (f.get("name") or "").replace("-", "_")
                    start  = int(f.get("start") or 1) - 1
                    length = int(f.get("length") or 1)
                    trailer_meta[fname] = line[start:start + length].strip() if len(line) >= start + length else ""
            self._file_metadata[name + "_trailer"] = trailer_meta
            LOG.info("[FIXED] %s: trailer metadata extracted: %s", name, trailer_meta)

        # Filter out blank lines (empty records that are not data)
        raw_df = raw_df.filter(F.length(F.trim(F.col("value"))) > 0)

        # ── 3. Optional: validate record width (single aggregation pass) ──────
        if record_length:
            rl = int(record_length)
            # One Spark job to get both counts simultaneously
            stats = raw_df.agg(
                F.count(F.when(F.length(F.col("value")) > rl, True)).alias("too_wide"),
                F.count(F.when(F.length(F.col("value")) < rl, True)).alias("too_short"),
            ).first()
            if stats and stats["too_wide"]:
                LOG.warning(
                    "[FIXED] %s: %d record(s) exceed expected record_length=%d",
                    name, stats["too_wide"], rl,
                )
            if stats and stats["too_short"]:
                LOG.warning(
                    "[FIXED] %s: %d record(s) are shorter than expected record_length=%d — "
                    "trailing fields may be empty",
                    name, stats["too_short"], rl,
                )

        # ── 4. Extract columns by start / length positions ────────────────────
        if fields:
            # ── Multi-record COBOL copybook offset detection ────────────────
            # COBOL copybooks assign ABSOLUTE start positions across all 01-level
            # groups (HEADER at 1-120, DATA at 121-240, TRAILER at 241-360).
            # Physical records are separate lines, each starting at position 1.
            # Detect the mismatch: if min(start) - 1 >= line length, subtract
            # that offset from every field's start so positions become 1-based.
            raw_starts = [int(f.get("start") or 1) for f in fields if f.get("start")]
            if raw_starts:
                min_start = min(raw_starts)
                if min_start > 1:
                    try:
                        sample_len = raw_df.select(
                            F.min(F.length(F.col("value"))).alias("_l")
                        ).collect()[0]["_l"]
                        if sample_len is not None and min_start - 1 >= sample_len:
                            offset = min_start - 1
                            LOG.info(
                                "[FIXED] %s: multi-record copybook detected "
                                "(min_start=%d, line_len=%d); adjusting all starts by -%d.",
                                name, min_start, sample_len, offset,
                            )
                            fields = [
                                {**f, "start": int(f.get("start") or 1) - offset}
                                for f in fields
                            ]
                    except Exception:
                        pass

            # Auto-compute start positions when they are missing (sequential layout)
            auto_start = 1
            select_exprs = []
            for i, f in enumerate(fields):
                col_name = (f.get("name") or f"col_{i}").replace("-", "_")
                length   = int(f.get("length") or 1)
                # Use explicit start if provided and > 0, otherwise auto-advance
                raw_start = f.get("start")
                if raw_start is not None and int(raw_start) > 0:
                    start = int(raw_start)   # 1-based (Spark substring is 1-based)
                else:
                    start = auto_start
                auto_start = start + length   # advance cursor for next field
                select_exprs.append(
                    F.trim(F.substring(F.col("value"), start, length)).alias(col_name)
                )
            df = raw_df.select(*select_exprs)

            # NOTE: All columns are intentionally kept as trimmed strings after
            # extraction.  Fixed-width files are raw text; field 'type' declarations
            # in the config express intent for validation and output formatting,
            # not a guarantee about the actual content.
            #
            # Keeping columns as strings lets the downstream validate step detect
            # invalid data (e.g. letters in a NUMBER field) and route those rows
            # to the error file.  If a numeric field contains "ABCDEFGH" it would
            # silently become null when pre-cast — making the NUMBER validation
            # check a no-op and hiding the data quality issue.
            #
            # _write_fixed_width honours the 'type' declaration for correct
            # padding alignment (right-justify for numbers) at write time.
        else:
            df = raw_df

        # ── 5. Validate record count against control file ────────────────────
        if count_file_path:
            try:
                if control_fields:
                    # Structured control file — extract count from a named field
                    ctrl_df = self._read_fixed_width(
                        name + "_ctrl",
                        {"fields": control_fields},
                        count_file_path,
                    )
                    # Look for a field whose name contains "count" or "cnt"
                    count_col = next(
                        (c for c in ctrl_df.columns if "count" in c.lower() or "cnt" in c.lower()),
                        ctrl_df.columns[0] if ctrl_df.columns else None,
                    )
                    if count_col:
                        first_row = ctrl_df.first()
                        expected = int(str(first_row[count_col]).strip()) if first_row else None
                    else:
                        expected = None
                else:
                    # Simple control file — first line is the count
                    first_row = self.spark.read.text(count_file_path).first()
                    expected = int(first_row["value"].strip()) if first_row else None

                if expected is not None:
                    actual = df.count()
                    if actual != expected:
                        LOG.warning(
                            "[FIXED] %s: count mismatch — count_file=%d  actual_records=%d",
                            name, expected, actual,
                        )
                    else:
                        LOG.info("[FIXED] %s: record count validated (%d)", name, actual)
            except Exception as ce:
                LOG.warning(
                    "[FIXED] %s: could not validate count_file_path '%s': %s",
                    name, count_file_path, ce,
                )

        LOG.info("[FIXED] %s: loaded %d columns from %s", name, len(fields) if fields else 1, path)
        return df

    def _apply_field_formats(self, df: DataFrame, fields: list[dict]) -> DataFrame:
        """
        Apply format-aware type casting based on field definitions.

        For DATE and TIMESTAMP fields with a ``format`` property (e.g.
        ``"yyyy-MM-dd"``), the column is parsed using ``to_date`` /
        ``to_timestamp`` with the specified format.  Other typed fields are
        cast using the standard Spark type mapping.

        Fields not present in the DataFrame are silently skipped.
        """
        if not fields:
            return df
        df_cols = {c.lower(): c for c in df.columns}
        for fld in fields:
            col_name = (fld.get("name") or "").replace("-", "_")
            if not col_name or col_name.lower() not in df_cols:
                continue
            actual_col = df_cols[col_name.lower()]
            ftype = (fld.get("type") or "string").lower()
            fmt = (fld.get("format") or "").strip()
            if ftype == "date" and fmt:
                df = df.withColumn(actual_col, F.to_date(F.col(actual_col), fmt))
            elif ftype == "timestamp" and fmt:
                df = df.withColumn(actual_col, F.to_timestamp(F.col(actual_col), fmt))
            elif ftype not in ("string", "str", ""):
                spark_type = _spark_type(ftype)
                df = df.withColumn(actual_col, F.col(actual_col).cast(spark_type))
        return df

    # ─────────────────────────────────────────────────────────────────────────
    # WRITE OUTPUTS
    # ─────────────────────────────────────────────────────────────────────────

    def _output_columns_from_config(self, cfg: dict, df: DataFrame) -> list[str] | None:
        """Return ordered list of underscore-normalised column names to write.

        Priority:
          1. ``output_columns`` / ``columns`` list in the config — only these fields
             are written to the output file.  The ``fields`` array is intentionally
             ignored when ``output_columns`` is present so that a copybook-derived
             ``fields`` definition (which describes the full fixed-width layout) does
             not accidentally include schema-only columns such as OUT-VALID-FLAG or
             OUT-PROC-TS in the written output.
          2. ``fields`` list — used as a fallback **only** when ``output_columns`` is
             absent from the config entirely (None, not just empty).
          3. None → write all columns that are present in the DataFrame.
        """
        # Use output_columns when it is explicitly configured (even empty list means
        # "write nothing extra" — treat as write-all rather than silently falling
        # through to the full fields array which would include schema-only columns).
        raw_explicit = cfg.get("output_columns")
        if raw_explicit is None:
            raw_explicit = cfg.get("columns")

        if raw_explicit is not None:
            # Key present in config — honour it even if the list is empty
            cols = raw_explicit if isinstance(raw_explicit, list) else [raw_explicit]
            want = [c.replace("-", "_") for c in cols if isinstance(c, str) and c.strip()]
            # Non-empty explicit list → return it; empty list → write all (return None)
            return want if want else None

        # output_columns not present at all → fall back to fields for column ordering
        fields = cfg.get("fields") or []
        if not fields:
            return None
        want = [f.get("name", "").replace("-", "_") for f in fields if f.get("name")]
        return want or None

    def _ensure_output_columns(self, df: DataFrame, want: list[str]) -> DataFrame:
        """Ensure df has all wanted columns; alias from a matching source column if missing."""
        df_cols = set(df.columns)
        for c in want:
            if c in df_cols:
                continue
            # Try the hyphenated equivalent (e.g. TXN_ID → TXN-ID).
            # This handles CSV/test-mode inputs where column names preserve hyphens
            # but out_cols has been normalised to underscores.
            hyphenated = c.replace("_", "-")
            if hyphenated in df_cols:
                df = df.withColumn(c, F.col(hyphenated))
                df_cols.add(c)
                LOG.debug("Output: aliased %s from hyphenated source %s", c, hyphenated)
                continue
            parts = c.split("_")
            for i in range(1, len(parts)):
                suffix = "_".join(parts[i:])
                match = next(
                    (x for x in df.columns if x == suffix or x.endswith("_" + suffix)),
                    None,
                )
                if match:
                    df = df.withColumn(c, F.col(match))
                    df_cols.add(c)
                    LOG.debug("Output: aliased %s from %s", c, match)
                    break
        return df

    def _coalesce_to_named_file(
        self,
        write_fn,           # callable(staging_path: str) -> None
        output_dir: str,    # directory that will contain the target file
        target_file_name: str,
    ) -> None:
        """
        Write a single-file output with a specific name.

        ``write_fn`` should write the (coalesced to 1 partition) data to its
        ``staging_path`` argument.  This helper then promotes the single
        part-file that Spark created into ``output_dir/target_file_name``.

        Works for both local paths and S3/s3a paths.
        """
        import uuid
        staging_dir = output_dir.rstrip("/") + f"/__staging_{uuid.uuid4().hex[:8]}__"
        final_path  = output_dir.rstrip("/") + "/" + target_file_name

        write_fn(staging_dir)

        if _is_s3_path(output_dir):
            # ── S3: use Hadoop FileSystem API via the PySpark JVM bridge ─────
            sc         = self.spark.sparkContext
            jvm        = sc._jvm                          # type: ignore[attr-defined]
            hconf      = sc._jsc.hadoopConfiguration()    # type: ignore[attr-defined]
            HPath      = jvm.org.apache.hadoop.fs.Path
            FileSystem = jvm.org.apache.hadoop.fs.FileSystem
            FileUtil   = jvm.org.apache.hadoop.fs.FileUtil
            URI        = jvm.java.net.URI

            staging_hpath = HPath(staging_dir)
            staging_fs    = FileSystem.get(URI.create(staging_dir), hconf)
            target_hpath  = HPath(final_path)
            target_dir_hp = HPath(output_dir)

            # Find the single part-file in the staging directory
            part_path = None
            for s in staging_fs.listStatus(staging_hpath):
                fname = s.getPath().getName()
                if fname.startswith("part-") and not fname.startswith("."):
                    part_path = s.getPath()
                    break

            if part_path is None:
                staging_fs.delete(staging_hpath, True)
                raise RuntimeError(f"No part file found in staging dir: {staging_dir}")

            target_fs = FileSystem.get(URI.create(output_dir), hconf)
            target_fs.mkdirs(target_dir_hp)
            if target_fs.exists(target_hpath):
                target_fs.delete(target_hpath, False)

            FileUtil.copy(staging_fs, part_path, target_fs, target_hpath, True, hconf)
            staging_fs.delete(staging_hpath, True)

        else:
            # ── Local path: use Python stdlib ────────────────────────────────
            import glob
            import os
            import shutil

            part_files = sorted(glob.glob(os.path.join(staging_dir, "part-*")))
            os.makedirs(output_dir, exist_ok=True)

            if not part_files:
                shutil.rmtree(staging_dir, ignore_errors=True)
                raise RuntimeError(f"No part file found in staging dir: {staging_dir}")

            if os.path.exists(final_path):
                os.remove(final_path)
            shutil.move(part_files[0], final_path)
            shutil.rmtree(staging_dir, ignore_errors=True)

        LOG.info("[TARGET FILE] %s/%s written", output_dir, target_file_name)

    def _eval_hdr_trl_expr(self, df: DataFrame, expr: str, data_count: int) -> str:
        """Evaluate a header/trailer field expression and return its string value."""
        if not expr:
            return ""
        expr_s = expr.strip()
        # String literal: 'value'
        if expr_s.startswith("'") and expr_s.endswith("'") and len(expr_s) >= 2:
            return expr_s[1:-1]
        # Numeric literal
        try:
            return str(int(expr_s))
        except ValueError:
            pass
        try:
            return str(float(expr_s))
        except ValueError:
            pass
        # count(*) — use pre-computed count to avoid a second pass
        if expr_s.lower().replace(" ", "") == "count(*)":
            return str(data_count)
        # Try as aggregate or scalar Spark SQL expression
        try:
            val = df.selectExpr(f"{expr_s} as _hv").collect()[0][0]
            return str(val) if val is not None else ""
        except Exception:  # noqa: BLE001
            pass
        # Fallback: scalar (no table scan needed — e.g. current_date(), literals)
        try:
            val = self.spark.range(1).selectExpr(f"{expr_s} as _hv").collect()[0][0]
            return str(val) if val is not None else ""
        except Exception:  # noqa: BLE001
            pass
        return ""

    def _build_hdr_trl_row(self, fields: list, df: DataFrame, record_length: int, data_count: int) -> str:
        """Build a fixed-width string row from header/trailer field expressions."""
        parts: list[str] = []
        for f in fields:
            val    = self._eval_hdr_trl_expr(df, f.get("expression") or "", data_count)
            length = int(f.get("length") or 1)
            ftype  = (f.get("type") or "string").lower()
            if ftype in ("int", "integer", "long", "bigint", "double", "float", "decimal", "number"):
                piece = val.rjust(length)[:length]
            else:
                piece = val.ljust(length)[:length]
            parts.append(piece)
        result = "".join(parts)
        if record_length > 0:
            result = result[:record_length].ljust(record_length)
        return result

    def _write_fixed_width(self, df: DataFrame, name: str, cfg: dict, path: str, write_mode: str, target_file_name: str = "") -> None:
        """
        Write a DataFrame as a fixed-width flat file.

        Each column is padded (string) or right-justified (numeric) to its declared length.
        The record_length config field is used as the total line width (padded / truncated).
        If control_file_path is set, a companion control file is written with the record count
        (and optional structured fields if control_fields is defined).
        """
        fields          = cfg.get("fields") or []
        record_length   = int(cfg.get("record_length") or 0)
        header_count    = int(cfg.get("header_count") or 0)
        trailer_count   = int(cfg.get("trailer_count") or 0)
        raw_ctrl_path   = cfg.get("control_file_path") or ""
        control_file_path = _effective_path(raw_ctrl_path) if raw_ctrl_path else ""
        control_fields  = cfg.get("control_fields") or []

        if not fields:
            LOG.warning("[FIXED OUT] %s: no fields defined; writing CSV instead", name)
            df.write.mode(write_mode).option("header", "true").csv(path)
            return

        # ── Positional fallback: if NO config field names match ANY DataFrame column,
        #    map by index position so data still flows through (e.g. INP_* → OUT_*).
        data_fields = [f for f in fields
                       if (f.get("record_type") or "DATA").upper() == "DATA"]
        matched_count = sum(
            1 for f in data_fields
            if (f.get("name") or "").replace("-", "_") in df.columns
        )
        if matched_count == 0 and len(df.columns) > 0:
            LOG.info(
                "[FIXED OUT] %s: no field names matched DataFrame columns; "
                "using positional mapping (%d fields, %d df cols)",
                name, len(data_fields), len(df.columns),
            )
            for i, f in enumerate(data_fields):
                if i >= len(df.columns):
                    break
                old_col = df.columns[i]
                new_col = (f.get("name") or "").replace("-", "_")
                if old_col != new_col:
                    df = df.withColumnRenamed(old_col, new_col)

        # ── Build a single fixed-width string column ──────────────────────────
        line_expr = None
        for f in fields:
            col_name = (f.get("name") or "").replace("-", "_")
            length   = int(f.get("length") or 1)
            ftype    = (f.get("type") or "string").lower()
            if col_name not in df.columns:
                LOG.warning("[FIXED OUT] %s: column '%s' not in DataFrame; padding blanks", name, col_name)
                piece = F.lpad(F.lit(""), length, " ")
            elif ftype in ("int", "integer", "long", "bigint", "double", "float", "decimal", "number"):
                # Numeric: right-justify, truncate if too long
                piece = F.rpad(
                    F.substring(F.lpad(F.col(col_name).cast("string"), length, " "), 1, length),
                    length, " ",
                )
            else:
                # String: left-justify, pad / truncate to length
                piece = F.rpad(F.substring(F.col(col_name).cast("string"), 1, length), length, " ")

            line_expr = piece if line_expr is None else F.concat(line_expr, piece)

        if record_length > 0 and line_expr is not None:
            line_expr = F.rpad(F.substring(line_expr, 1, record_length), record_length, " ")

        df_fixed = df.select(line_expr.alias("value"))

        # ── Prepend / append header and trailer lines ─────────────────────────
        if header_count > 0 or trailer_count > 0:
            fixed_rl = record_length or sum(int(f.get("length") or 1) for f in fields)
            header_fields_cfg = cfg.get("header_fields") or []
            trailer_fields_cfg = cfg.get("trailer_fields") or []
            # Pre-compute row count once (used by count(*) expressions)
            data_count = df.count() if (header_fields_cfg or trailer_fields_cfg) else 0
            parts = []
            for _ in range(header_count):
                if header_fields_cfg:
                    row_str = self._build_hdr_trl_row(header_fields_cfg, df, fixed_rl, data_count)
                else:
                    row_str = " " * fixed_rl
                parts.append(self.spark.range(1).select(F.lit(row_str).alias("value")))
            parts.append(df_fixed)
            for _ in range(trailer_count):
                if trailer_fields_cfg:
                    row_str = self._build_hdr_trl_row(trailer_fields_cfg, df, fixed_rl, data_count)
                else:
                    row_str = " " * fixed_rl
                parts.append(self.spark.range(1).select(F.lit(row_str).alias("value")))
            df_fixed = parts[0]
            for p in parts[1:]:
                df_fixed = df_fixed.union(p)

        if target_file_name:
            self._coalesce_to_named_file(
                lambda sp: df_fixed.coalesce(1).write.mode(write_mode).text(sp),
                path, target_file_name,
            )
        else:
            df_fixed.write.mode(write_mode).text(path)
        LOG.info("[FIXED OUT] %s: wrote fixed-width file to %s", name, path)

        # ── Write control file ────────────────────────────────────────────────
        if control_file_path:
            try:
                data_count = df.count()
                if control_fields:
                    # Structured control file — write as fixed-width with control_fields layout
                    ctrl_row = {f.get("name", "").replace("-", "_"): "" for f in control_fields}
                    # Look for a count-like field and set it
                    for f in control_fields:
                        cn = (f.get("name") or "").lower()
                        if "count" in cn or "cnt" in cn:
                            ctrl_row[f.get("name", "").replace("-", "_")] = str(data_count)
                            break
                    ctrl_df = self.spark.createDataFrame([ctrl_row])
                    ctrl_cfg = {"fields": control_fields}
                    self._write_fixed_width(ctrl_df, name + "_ctrl", ctrl_cfg, control_file_path, write_mode)
                else:
                    # Simple control file — write count as single line
                    ctrl_df = self.spark.range(1).select(F.lit(str(data_count)).alias("value"))
                    ctrl_df.write.mode(write_mode).text(control_file_path)
                LOG.info("[FIXED OUT] %s: wrote control file (%d records) to %s", name, data_count, control_file_path)
            except Exception as ce:
                LOG.warning("[FIXED OUT] %s: could not write control file '%s': %s", name, control_file_path, ce)

    def _write_output(self, df: DataFrame, name: str, cfg: dict) -> None:
        """
        Write output based on format.

        Protocol routing:
          s3:// / s3a:// / s3n://  – configured for hadoop-aws (S3AFileSystem)
          file://                   – stripped to local path
          relative / absolute       – used as-is by Spark's local FS

        If output config defines fields/output_columns, only those columns are written.
        Column names in the written file match the schema (with hyphens preserved) so they
        align with config and reconciliation.
        """
        fmt        = (cfg.get("format") or "parquet").lower()
        raw_path   = get_output_path(
            cfg, str(self.base_path),
            interface_name=self.interface_name, settings=self.settings,
        )
        _configure_s3_if_needed(self.spark, raw_path)
        path       = _effective_path(raw_path)
        write_mode = (cfg.get("write_mode") or "overwrite").lower()

        if not path:
            LOG.warning("No output path for %s; skipping write", name)
            return

        # ── Resolve and select output columns ────────────────────────────────
        out_cols = self._output_columns_from_config(cfg, df)
        if out_cols:
            df = self._ensure_output_columns(df, out_cols)
            # Fixed-width output: _write_fixed_width maps columns by normalising
            # field names (hyphens → underscores) and looking them up in df.columns.
            # Keep the underscore-normalised names so the lookup succeeds.
            # For other formats (parquet, csv) alias back to the canonical schema
            # names (with hyphens) so downstream tools see the correct headers.
            if fmt == "fixed":
                df = df.select(*[c for c in out_cols if c in df.columns])
            else:
                fields       = cfg.get("fields") or []
                schema_names = [f.get("name") for f in fields if isinstance(f, dict) and f.get("name")]
                if schema_names and len(schema_names) == len(out_cols):
                    df = df.select(
                        *[F.col(u).alias(h) for u, h in zip(out_cols, schema_names) if u in df.columns]
                    )
                else:
                    df = df.select(*[c for c in out_cols if c in df.columns])
            LOG.debug("Output %s: writing columns %s", name, list(df.columns))
        elif out_cols is not None and not out_cols:
            LOG.warning("Output %s: no configured output columns; writing all columns", name)

        # ── Resolve target_file_name (optional single-file rename) ───────────
        target_file_name = (cfg.get("target_file_name") or cfg.get("dataset_name") or "").strip()
        # Mainframe convention: fixed-width / text output files use .DAT extension
        if target_file_name and fmt in ("fixed", "text") and not target_file_name.upper().endswith(".DAT"):
            target_file_name = target_file_name + ".DAT"

        # ── Write in the requested format ─────────────────────────────────────
        writer = df.write.mode(write_mode)
        if fmt == "parquet":
            if target_file_name:
                self._coalesce_to_named_file(
                    lambda sp: df.coalesce(1).write.mode(write_mode).parquet(sp),
                    path, target_file_name,
                )
            else:
                writer.parquet(path)
        elif fmt in ("csv", "delimited"):
            delimiter     = cfg.get("delimiter_char") or cfg.get("delimiter") or ","
            header_count  = int(cfg.get("header_count")  or 0)
            trailer_count = int(cfg.get("trailer_count") or 0)
            if header_count > 0 or trailer_count > 0:
                # Prepend/append blank lines as header/trailer records.
                # Each blank line is a row of empty delimiter-separated values
                # matching the number of output columns.
                col_count  = len(df.columns)
                blank_line = delimiter.join([""] * col_count)
                blank_row  = self.spark.range(1).select(F.lit(blank_line).alias("value"))
                # Build the CSV body as a single text column (no Spark header
                # since we are manually assembling the file structure).
                import tempfile as _tmp, os as _os
                tmp_csv_dir = _tmp.mkdtemp(prefix="_csv_body_")
                df.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", delimiter).csv(tmp_csv_dir)
                body_df = self.spark.read.text(tmp_csv_dir)
                parts = []
                for _ in range(header_count):
                    parts.append(blank_row)
                parts.append(body_df)
                for _ in range(trailer_count):
                    parts.append(blank_row)
                assembled = parts[0]
                for p in parts[1:]:
                    assembled = assembled.union(p)
                LOG.info(
                    "[CSV OUT] %s: prepending %d header line(s) + appending %d trailer line(s)",
                    name, header_count, trailer_count,
                )
                if target_file_name:
                    self._coalesce_to_named_file(
                        lambda sp: assembled.coalesce(1).write.mode(write_mode).text(sp),
                        path, target_file_name,
                    )
                else:
                    assembled.coalesce(1).write.mode(write_mode).text(path)
                import shutil as _shutil
                _shutil.rmtree(tmp_csv_dir, ignore_errors=True)
            elif target_file_name:
                self._coalesce_to_named_file(
                    lambda sp: df.coalesce(1).write.mode(write_mode)
                                .option("header", "true").option("sep", delimiter).csv(sp),
                    path, target_file_name,
                )
            else:
                writer.option("header", "true").option("sep", delimiter).csv(path)
        elif fmt == "fixed":
            # _write_fixed_width handles single-file rename internally
            self._write_fixed_width(df, name, cfg, path, write_mode, target_file_name)
        elif fmt == "cobol":
            try:
                cobrix_opts = cfg.get("cobrix") or {}
                copybook    = cobrix_opts.get("copybook_path") or cfg.get("copybook") or ""
                writer.format("cobol").option("copybook", copybook).save(path)
            except Exception as e:
                LOG.warning("Cobrix write failed for %s: %s; falling back to parquet", name, e)
                writer.parquet(path)
        else:
            if target_file_name:
                self._coalesce_to_named_file(
                    lambda sp: df.coalesce(1).write.mode(write_mode).parquet(sp),
                    path, target_file_name,
                )
            else:
                writer.parquet(path)

        LOG.info("Wrote %s -> %s/%s", name, path, target_file_name or "(dir)")

    def _check_previous_day_header(self, input_name: str, cfg: dict) -> None:
        """
        Previous-day header check for FIXED-width inputs.

        Reads the previous business day's raw input file, extracts its header
        date field, and verifies it is exactly one business day before today's
        header date.  Raises a RuntimeError (aborting the job) and fires a
        Moogsoft incident when the check fails.

        The check is configured per-input in the config JSON::

            "prev_day_check": {
                "enabled": true,
                "header_date_field": "BATCH_DATE"   // must appear in header_fields
            }

        When ``header_date_field`` is not found in ``header_fields``, or today's
        header metadata was not captured, the check is skipped with a warning
        (defensive — avoids false positives from misconfiguration).
        """
        pdc = cfg.get("prev_day_check") or {}
        if not pdc.get("enabled"):
            return

        date_field_name = (pdc.get("header_date_field") or "").strip()
        if not date_field_name:
            LOG.warning(
                "[PREV-DAY] %s: prev_day_check enabled but header_date_field is not set — skipping.",
                input_name,
            )
            return

        # ── 1. Find field definition in header_fields ──────────────────────
        header_fields = cfg.get("header_fields") or []
        field_def = next(
            (f for f in header_fields if (f.get("name") or "").strip() == date_field_name),
            None,
        )
        if field_def is None:
            LOG.warning(
                "[PREV-DAY] %s: header_date_field '%s' not found in header_fields — skipping check.",
                input_name, date_field_name,
            )
            return

        start  = int(field_def.get("start") or 1) - 1   # convert to 0-based
        length = int(field_def.get("length") or 8)
        spark_fmt = (field_def.get("format") or "yyyyMMdd").strip()
        py_fmt = (
            spark_fmt
            .replace("yyyy", "%Y").replace("yy", "%y")
            .replace("MM", "%m").replace("dd", "%d")
        )

        # ── 2. Read today's header date from already-extracted metadata ─────
        meta = self._file_metadata.get(input_name + "_header") or {}
        norm_field = date_field_name.replace("-", "_")
        today_date_str = meta.get(norm_field) or meta.get(date_field_name) or ""
        if not today_date_str:
            LOG.warning(
                "[PREV-DAY] %s: header date field '%s' not found in extracted metadata — skipping check.",
                input_name, date_field_name,
            )
            return

        try:
            today_header_date: _date = _datetime.strptime(today_date_str, py_fmt).date()
        except (ValueError, TypeError) as exc:
            LOG.warning(
                "[PREV-DAY] %s: cannot parse today's header date '%s' with format '%s' (%s) — skipping check.",
                input_name, today_date_str, py_fmt, exc,
            )
            return

        # ── 3. Compute expected previous business day ───────────────────────
        holidays = self.settings.get("usa_holidays") or []
        prev_biz_day = get_previous_business_day(today_header_date, holidays)
        LOG.info(
            "[PREV-DAY] %s: today_header=%s  expected_prev=%s",
            input_name, today_header_date, prev_biz_day,
        )

        # ── 4. Build path for prev-day file and read its header line ────────
        prev_path = get_input_path_for_date(
            cfg, prev_biz_day,
            interface_name=self.interface_name,
            settings=self.settings,
        )
        # Strip "file://" prefix so Spark can read local files
        spark_prev_path = prev_path
        if spark_prev_path.startswith("file://"):
            spark_prev_path = spark_prev_path[7:]

        pipeline_name = self.interface_name

        try:
            prev_rows = (
                self.spark.read.option("wholetext", "false").text(spark_prev_path)
                .withColumn("value", F.regexp_replace(F.col("value"), r"\r$", ""))
                .limit(1)
                .select("value")
                .collect()
            )
            prev_lines = [row.value for row in prev_rows]
        except Exception as exc:  # noqa: BLE001
            msg = (
                f"[PREV-DAY] Job aborted: cannot read previous business day's file for input "
                f"'{input_name}'. Expected path: '{prev_path}'. Error: {exc}. "
                f"Pipeline: '{pipeline_name}'."
            )
            LOG.error(msg)
            _raise_prev_day_check_incident(
                pipeline_name=pipeline_name,
                input_name=input_name,
                file_path=prev_path,
                expected_date=prev_biz_day,
                actual_date="FILE_MISSING",
            )
            raise RuntimeError(msg)

        if not prev_lines:
            msg = (
                f"[PREV-DAY] Job aborted: previous business day's file for input '{input_name}' "
                f"at '{prev_path}' is empty (no header line found). "
                f"Pipeline: '{pipeline_name}'."
            )
            LOG.error(msg)
            _raise_prev_day_check_incident(
                pipeline_name=pipeline_name,
                input_name=input_name,
                file_path=prev_path,
                expected_date=prev_biz_day,
                actual_date="EMPTY_FILE",
            )
            raise RuntimeError(msg)

        # ── 5. Extract and parse the header date from the prev-day file ─────
        prev_line = prev_lines[0]
        prev_date_str = (
            prev_line[start:start + length].strip()
            if len(prev_line) >= start + length
            else ""
        )
        if not prev_date_str:
            msg = (
                f"[PREV-DAY] Job aborted: cannot extract header date field '{date_field_name}' "
                f"from previous business day's file '{prev_path}' for input '{input_name}'. "
                f"Pipeline: '{pipeline_name}'."
            )
            LOG.error(msg)
            _raise_prev_day_check_incident(
                pipeline_name=pipeline_name,
                input_name=input_name,
                file_path=prev_path,
                expected_date=prev_biz_day,
                actual_date="FIELD_NOT_FOUND",
            )
            raise RuntimeError(msg)

        try:
            prev_header_date: _date = _datetime.strptime(prev_date_str, py_fmt).date()
        except (ValueError, TypeError) as exc:
            msg = (
                f"[PREV-DAY] Job aborted: cannot parse header date '{prev_date_str}' "
                f"in previous business day's file '{prev_path}' for input '{input_name}'. "
                f"Error: {exc}. Pipeline: '{pipeline_name}'."
            )
            LOG.error(msg)
            _raise_prev_day_check_incident(
                pipeline_name=pipeline_name,
                input_name=input_name,
                file_path=prev_path,
                expected_date=prev_biz_day,
                actual_date=prev_date_str,
            )
            raise RuntimeError(msg)

        # ── 6. Compare and pass / fail ──────────────────────────────────────
        if prev_header_date != prev_biz_day:
            msg = (
                f"[PREV-DAY] Job aborted: previous business day's file for input '{input_name}' "
                f"has header date {prev_header_date} but expected {prev_biz_day}. "
                f"File: '{prev_path}'. Pipeline: '{pipeline_name}'."
            )
            LOG.error(msg)
            _raise_prev_day_check_incident(
                pipeline_name=pipeline_name,
                input_name=input_name,
                file_path=prev_path,
                expected_date=prev_biz_day,
                actual_date=prev_header_date,
            )
            raise RuntimeError(msg)

        LOG.info(
            "[PREV-DAY] %s: PASSED — previous business day file header date %s matches expected %s.",
            input_name, prev_header_date, prev_biz_day,
        )

    # ─────────────────────────────────────────────────────────────────────────
    # ORCHESTRATION
    # ─────────────────────────────────────────────────────────────────────────

    def load_inputs(self) -> dict[str, DataFrame]:
        """Load all inputs from config."""
        inputs = self.config.get("Inputs") or {}
        base   = str(self.base_path)
        for name, cfg in inputs.items():
            if not isinstance(cfg, dict):
                continue
            path = get_input_path(cfg, base)
            freq = get_frequency(self.config, name)
            if freq:
                LOG.info("Input '%s' frequency: %s", name, freq)
            try:
                df = self._read_input(name, cfg)
            except Exception as e:
                # ── Test-mode CSV fallback ────────────────────────────────────────
                # test_dataflow.py always writes generated data to
                #   base_path/input/{name}.csv
                # When the original source path doesn't exist (e.g. production file
                # not available during local testing), try that CSV before giving up.
                # This works even when the config's source_path / source_file_name
                # were not cleared in the temp config (e.g. server not yet reloaded).
                test_csv = Path(base) / "input" / f"{name}.csv"
                if test_csv.exists():
                    LOG.info(
                        "Input '%s': original path not found (%s); "
                        "reading test CSV fallback from %s",
                        name, path, test_csv,
                    )
                    try:
                        df = (
                            self.spark.read
                            .option("header", "true")
                            .option("inferSchema", "true")
                            .csv(str(test_csv))
                        )
                        # Normalise column names (strip hyphens/spaces)
                        clean = [c.replace("-", "_").replace(" ", "_") for c in df.columns]
                        if clean != list(df.columns):
                            df = df.toDF(*clean)
                    except Exception as csv_e:
                        LOG.warning(
                            "Could not read test CSV for %s: %s; creating empty from schema",
                            name, csv_e,
                        )
                        df = self._empty_from_schema(cfg)
                else:
                    # ── Production: input file missing → incident + abort ──────────
                    # The input file was not delivered to the expected path.
                    # A ServiceNow incident is raised and the job is aborted so
                    # downstream steps never process an empty/incorrect dataset.
                    # (Test mode uses the parser_test_ base_path and relies on the
                    #  test CSV fallback above; production paths reach here instead.)
                    if "parser_test_" in str(self.base_path):
                        # Safety net: even in test mode, if no test CSV was found,
                        # fall back to empty schema to let the test suite continue.
                        LOG.warning(
                            "[INPUT] Test mode: no source file or test CSV found for "
                            "'%s' at '%s'; creating empty DataFrame from schema.",
                            name, path,
                        )
                        df = self._empty_from_schema(cfg)
                    else:
                        pipeline_name = self.config_path.stem
                        LOG.error(
                            "[INPUT] Required input file '%s' not found at path '%s'. "
                            "Raising ServiceNow incident and aborting job.",
                            name, path,
                        )
                        _raise_input_file_missing_incident(
                            pipeline_name=pipeline_name,
                            input_name=name,
                            file_path=path or str(self.config_path),
                        )
                        raise RuntimeError(
                            f"[INPUT] Job aborted: required input file '{name}' was not "
                            f"found at '{path}'. The file has not been created or "
                            f"delivered. A ServiceNow incident has been raised. "
                            f"Pipeline: '{pipeline_name}'."
                        )
            self.input_dfs[name] = df
            # Previous-day header check (skip in test/parser-test mode)
            if "parser_test_" not in str(self.base_path):
                self._check_previous_day_header(name, cfg)
        return self.input_dfs

    def _empty_from_schema(self, cfg: dict) -> DataFrame:
        """
        Create empty DataFrame with schema from config (for testing when files missing).

        Uses spark.range(0).select(...) instead of createDataFrame([], schema)
        to avoid cloudpickle/RecursionError on Python 3.14 when serialising an
        empty-list RDD with a StructType schema.
        """
        fields = cfg.get("fields") or []
        col_exprs = []
        for i, f in enumerate(fields):
            if not isinstance(f, dict):
                continue
            col_name   = (f.get("name") or f"col_{i}").replace("-", "_")
            spark_type = _spark_type(f.get("type") or "string")
            col_exprs.append(F.lit(None).cast(spark_type).alias(col_name))

        if not col_exprs:
            # Minimal fallback — single nullable id column
            col_exprs = [F.lit(None).cast(T.LongType()).alias("id")]

        # spark.range(0) creates a zero-row DataFrame purely in the JVM (no Python
        # RDD serialisation); the subsequent select() uses Spark Column expressions
        # (also JVM-side) — no cloudpickle, no stack-overflow on Python 3.14.
        return self.spark.range(0).select(*col_exprs)

    def run_transformations(self) -> dict[str, DataFrame]:
        """
        Execute transformation steps one by one.
        Each step reads its source dataset(s) from the registry, applies a Spark DataFrame
        transformation, and stores the result under output_alias so the next step can use it.
        """
        steps    = (self.config.get("Transformations") or {}).get("steps") or []
        datasets : dict[str, DataFrame] = dict(self.input_dfs)
        self.output_dfs.clear()
        for i, step in enumerate(steps):
            step_id   = step.get("id", str(i))
            step_type = (step.get("type") or "select").lower()
            alias     = step.get("output_alias") or step.get("id", "out")
            source_names = step.get("source_inputs") or []
            LOG.info(
                "Step %s: %s (%s) <- %s -> %s",
                i + 1, step_id, step_type, source_names, alias,
            )

            # ── Validate step: inject source input field definitions ──────────
            # This allows _apply_validate() to write validated/error files as
            # true fixed-width flat files (same layout as the input file) rather
            # than pipe-delimited text.
            if step_type == "validate":
                input_cfgs = self.config.get("Inputs") or {}
                step.setdefault("logic", {})
                # Always set the real pipeline name (config file stem) and step ID
                # so ServiceNow incidents carry meaningful identifiers instead of
                # the fallback "unknown" / "validate" placeholders.
                step["logic"]["_pipeline_name"] = self.config_path.stem
                step["logic"]["_step_id"]       = step.get("id") or alias
                step["logic"]["_interface_name"] = self.interface_name
                step["logic"]["_settings"]       = self.settings
                for src_name in source_names:
                    # Case-insensitive lookup so that a source_inputs value like
                    # "HOGAN-INPUT" still matches an Inputs key like "Hogan-Input".
                    src_cfg = input_cfgs.get(src_name)
                    if src_cfg is None:
                        for _k, _v in input_cfgs.items():
                            if _k.upper() == src_name.upper():
                                src_cfg = _v
                                LOG.debug(
                                    "[VALIDATE] Case-insensitive match: "
                                    "source '%s' → input key '%s'",
                                    src_name, _k,
                                )
                                break
                    src_cfg = src_cfg or {}
                    # Inject source input config for previous_day_check derivation
                    step["logic"]["_source_input_config"] = src_cfg
                    # Inject header/trailer metadata for reconciliation and expressions
                    step["logic"]["_file_metadata"] = self._file_metadata
                    src_fields = src_cfg.get("fields") or []
                    if src_fields:
                        step["logic"]["_source_fields"]  = src_fields
                        step["logic"]["_record_length"]  = int(src_cfg.get("record_length") or 0)
                        LOG.debug(
                            "[VALIDATE] Injected %d field definition(s) from input '%s' "
                            "for fixed-width validated/error output.",
                            len(src_fields), src_name,
                        )
                        break

            alias_out, result_df = apply_transformation_step(step, datasets)
            if alias_out and result_df is not None:
                datasets[alias_out]        = result_df
                self.output_dfs[alias_out] = result_df
            else:
                LOG.warning("Step %s produced no result; skipping", step_id)
        return self.output_dfs

    # ── Curated ctrl file ─────────────────────────────────────────────────────

    def _maybe_write_curated_ctrl_file(
        self, df: DataFrame, output_name: str, output_cfg: dict
    ) -> None:
        """
        Write the control file to the curated (output) path when the output's
        source DataFrame comes from a validate step that has ctrl_file_create=True.

        The ctrl file field values (count, date, etc.) are computed from *df*
        (the curated output DataFrame) using the same PySpark expressions
        configured in the validate step's ``ctrl_file_fields``.  This ensures the
        ctrl file in the curated layer reflects exactly the records that were
        written to that layer.
        """
        source_inputs = output_cfg.get("source_inputs") or []
        steps = (self.config.get("Transformations") or {}).get("steps") or []

        # Find a validate step with ctrl_file_create=True.
        # Prefer one whose output_alias appears in this output's source_inputs;
        # fall back to any validate step that ran in this pipeline (present in
        # output_dfs).  This handles the common case where source_inputs points
        # to the original input alias rather than the validate step alias.
        validate_step = None
        for step in steps:
            if (step.get("type") or "").lower() != "validate":
                continue
            logic = step.get("logic") or {}
            if not logic.get("ctrl_file_create"):
                continue
            step_alias = step.get("output_alias") or step.get("id", "")
            if step_alias in source_inputs:
                validate_step = step
                break                   # exact source_inputs match — highest priority
            if step_alias in self.output_dfs:
                validate_step = step    # pipeline ran this step — keep as candidate

        if validate_step is None:
            return

        logic            = validate_step.get("logic") or {}
        ctrl_file_name   = (logic.get("ctrl_file_name")   or "").strip()
        ctrl_file_fields = logic.get("ctrl_file_fields") or []
        ctrl_include_hdr = bool(logic.get("ctrl_include_header", False))
        if not ctrl_file_name or not ctrl_file_fields:
            LOG.debug(
                "[CURATED_CTRL] ctrl_file_name or ctrl_file_fields not "
                "set on validate step '%s'; skipping curated ctrl file.",
                validate_step.get("id"),
            )
            return

        # Resolve the curated output directory path (with freq/date partition)
        raw_path     = get_output_path(output_cfg, str(self.base_path))
        curated_path = _effective_path(raw_path)

        LOG.info(
            "[CURATED_CTRL] Writing ctrl file '%s' to curated path '%s'.",
            ctrl_file_name, curated_path,
        )
        try:
            _create_ctrl_file(
                df,
                ctrl_file_fields,
                curated_path,
                step_id=validate_step.get("id") or "validate",
                ctrl_file_name=ctrl_file_name,
                include_header=ctrl_include_hdr,
            )
        except Exception as exc:
            LOG.warning(
                "[CURATED_CTRL] Failed to write ctrl file '%s' to "
                "curated path '%s': %s",
                ctrl_file_name, curated_path, exc,
            )

    def write_outputs(self) -> None:
        """Write all outputs to configured paths.

        Resolution order:
          1. output_dfs by config name
          2. Configured source_inputs lookup (new: output can declare its source step)
          3. input_dfs by config name (pass-through outputs)
          4. Single-output shortcut when aliases differ
        """
        outputs     = self.config.get("Outputs") or {}
        input_names = set(self.config.get("Inputs") or {})
        for name, cfg in outputs.items():
            if not isinstance(cfg, dict):
                continue
            freq = get_frequency(self.config, name)
            if freq:
                LOG.info("Output '%s' frequency: %s", name, freq)
            df = self.output_dfs.get(name)

            # New: try source_inputs declared on the output node
            if df is None:
                for src in (cfg.get("source_inputs") or []):
                    df = self.output_dfs.get(src)
                    if df is not None:
                        LOG.debug("Output %s: resolved from source_input '%s'", name, src)
                        break

            if df is None:
                df = self.input_dfs.get(name)
            if df is None and len(outputs) == 1 and len(self.output_dfs) == 1:
                only_key = next(iter(self.output_dfs))
                if only_key not in input_names:
                    df = self.output_dfs[only_key]
                    LOG.debug("Mapped single output %s to dataframe %s", name, only_key)
            if df is not None:
                self._write_output(df, name, cfg)
                # Also write the ctrl file to the curated path when the source
                # is a validate step with ctrl_file_create=True.
                self._maybe_write_curated_ctrl_file(df, name, cfg)
            else:
                LOG.warning("No DataFrame for output %s; skipping write", name)

    def run(self) -> dict[str, DataFrame]:
        """Full dataflow: load inputs → run transformations → write outputs."""
        self.load_inputs()
        self.run_transformations()
        self.write_outputs()
        return self.output_dfs
