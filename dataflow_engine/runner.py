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

Schema field properties supported:
  name      – column name (hyphens normalized to underscores internally)
  type      – string | int | long | double | decimal | date | timestamp
  nullable  – bool (default true); used for schema validation / empty-DataFrame creation
  format    – optional format string (e.g. "yyyy-MM-dd" for date columns)
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
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config_loader import load_config, get_input_path, get_control_file_path, get_output_path, get_frequency
from .transformations import (
    apply_transformation_step,
    _is_s3_path,
    _raise_input_file_missing_incident,
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
    ):
        self.spark = spark
        self.config_path = Path(config_path)
        self.base_path = Path(base_path) if base_path else self.config_path.parent
        self.use_cobrix = use_cobrix
        self.config = load_config(self.config_path)
        self.input_dfs: dict[str, DataFrame] = {}
        self.output_dfs: dict[str, DataFrame] = {}

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
        raw_path = get_input_path(cfg, str(self.base_path))
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
            delimiter = cfg.get("delimiter") or ","
            df = (
                self.spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", delimiter)
                .csv(path)
            )
            normalized = [c.replace("-", "_") for c in df.columns]
            if normalized != df.columns:
                df = df.toDF(*normalized)
            return df
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
        raw_ctrl_path   = get_control_file_path(cfg, str(self.base_path))
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
        if header_count > 0 or trailer_count > 0:
            # Extract plain strings (not Row objects) so the lambda only
            # serialises a basic str — avoids cloudpickle Row-class overhead.
            str_rdd  = raw_df.rdd.map(lambda r: r.value)
            indexed  = str_rdd.zipWithIndex()          # RDD[(str, long)]
            keep_from = header_count

            if trailer_count > 0:
                # Need total count to know where trailers start
                total_lines = indexed.count()
                keep_to     = total_lines - trailer_count
                LOG.info(
                    "[FIXED] %s: total_lines=%d  skip_header=%d  skip_trailer=%d  keep=[%d, %d)",
                    name, total_lines, header_count, trailer_count, keep_from, keep_to,
                )
                filtered = (
                    indexed
                    .filter(lambda x: keep_from <= x[1] < keep_to)
                    .map(lambda x: x[0])
                )
            else:
                LOG.info("[FIXED] %s: skipping %d header line(s)", name, header_count)
                filtered = (
                    indexed
                    .filter(lambda x: x[1] >= keep_from)
                    .map(lambda x: x[0])
                )

            raw_df = self.spark.createDataFrame(
                filtered.map(lambda v: (v,)), ["value"]
            )

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
            blank_row = self.spark.createDataFrame([(" " * fixed_rl,)], ["value"])
            parts = []
            for _ in range(header_count):
                parts.append(blank_row)
            parts.append(df_fixed)
            for _ in range(trailer_count):
                parts.append(blank_row)
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
                    ctrl_df = self.spark.createDataFrame([(str(data_count),)], ["value"])
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
        raw_path   = get_output_path(cfg, str(self.base_path))
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
        target_file_name = (cfg.get("target_file_name") or "").strip()

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
            delimiter = cfg.get("delimiter") or ","
            if target_file_name:
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

        # ── Test-mode safety copy ─────────────────────────────────────────────
        # test_dataflow.py's _read_outputs() always looks for parquet data at
        # base_path/output/{name}.  When the original config's source_path was
        # not cleared (e.g. Flask server hasn't reloaded test_dataflow.py yet),
        # the output goes to the production path instead of the temp dir.
        # Write a parquet copy here so _read_outputs() can always find the data.
        # The "parser_test_" prefix is exclusive to our test temp dirs; this guard
        # keeps production runs unaffected.
        if "parser_test_" in str(self.base_path):
            test_out = str(Path(str(self.base_path)) / "output" / name)
            if test_out != path.rstrip("/"):
                try:
                    df.write.mode("overwrite").parquet(test_out)
                    LOG.debug("Test copy written to %s", test_out)
                except Exception as te:
                    LOG.debug("Could not write test copy for %s: %s", name, te)

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

        # Find the validate step whose output_alias is one of our source_inputs
        for src_alias in source_inputs:
            for step in steps:
                if (
                    (step.get("output_alias") or step.get("id", "")) == src_alias
                    and (step.get("type") or "").lower() == "validate"
                ):
                    logic = step.get("logic") or {}
                    if not bool(logic.get("ctrl_file_create", False)):
                        return  # ctrl file creation not enabled for this step

                    ctrl_file_name   = (logic.get("ctrl_file_name")   or "").strip()
                    ctrl_file_fields = logic.get("ctrl_file_fields") or []
                    if not ctrl_file_name or not ctrl_file_fields:
                        LOG.debug(
                            "[CURATED_CTRL] ctrl_file_name or ctrl_file_fields not "
                            "set on validate step '%s'; skipping curated ctrl file.",
                            step.get("id"),
                        )
                        return

                    # Resolve the curated output directory path (with freq/date partition)
                    raw_path = get_output_path(output_cfg, str(self.base_path))
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
                            step_id=step.get("id") or "validate",
                            ctrl_file_name=ctrl_file_name,
                        )
                    except Exception as exc:
                        LOG.warning(
                            "[CURATED_CTRL] Failed to write ctrl file '%s' to "
                            "curated path '%s': %s",
                            ctrl_file_name, curated_path, exc,
                        )
                    return  # done — only one validate step per output

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
