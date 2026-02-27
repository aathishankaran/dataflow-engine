"""
PySpark Dataflow Runner - configuration-driven, step-by-step execution.

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
"""

import logging
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config_loader import load_config, get_input_path, get_output_path
from .transformations import apply_transformation_step

LOG = logging.getLogger(__name__)

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
        """Read input based on format (cobol/cobrix, parquet, csv, fixed, delimited)."""
        fmt = (cfg.get("format") or "cobol").lower()
        path = get_input_path(cfg, str(self.base_path))
        cobrix_opts = cfg.get("cobrix") or {}

        if fmt == "cobol" and self.use_cobrix:
            try:
                copybook_path = cobrix_opts.get("copybook_path") or cfg.get("copybook") or ""
                copybook_full = (
                    str(self.base_path / copybook_path)
                    if copybook_path and not copybook_path.startswith(("/", "s3:"))
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
        count_file_path = cfg.get("count_file_path") or ""
        control_fields  = cfg.get("control_fields") or []

        # ── 1. Read all lines as raw text ────────────────────────────────────
        raw_rdd = self.spark.read.text(path).rdd  # Row(value=str)

        # ── 2. Skip header / trailer lines ───────────────────────────────────
        if header_count > 0 or trailer_count > 0:
            indexed_rdd = raw_rdd.zipWithIndex()
            total_lines = indexed_rdd.count()
            keep_from   = header_count
            keep_to     = total_lines - trailer_count
            LOG.info(
                "[FIXED] %s: total_lines=%d  skip_header=%d  skip_trailer=%d  keep=[%d, %d)",
                name, total_lines, header_count, trailer_count, keep_from, keep_to,
            )
            filtered_rdd = (
                indexed_rdd
                .filter(lambda xi: keep_from <= xi[1] < keep_to)
                .map(lambda xi: xi[0])
            )
        else:
            filtered_rdd = raw_rdd

        raw_df = self.spark.createDataFrame(filtered_rdd, ["value"])

        # ── 3. Optional: validate record width ───────────────────────────────
        if record_length:
            rl = int(record_length)
            too_wide = raw_df.filter(F.length(F.col("value")) > rl).count()
            if too_wide:
                LOG.warning(
                    "[FIXED] %s: %d record(s) exceed expected record_length=%d",
                    name, too_wide, rl,
                )

        # ── 4. Extract columns by start / length positions ────────────────────
        if fields:
            select_exprs = []
            cast_map: dict[str, tuple[str, str | None]] = {}  # col_name -> (type, format)
            for i, f in enumerate(fields):
                col_name = (f.get("name") or f"col_{i}").replace("-", "_")
                start    = int(f.get("start") or 1)   # 1-based (Spark substring is 1-based)
                length   = int(f.get("length") or 1)
                ftype    = (f.get("type") or "string").lower()
                ffmt     = f.get("format") or None
                select_exprs.append(
                    F.trim(F.substring(F.col("value"), start, length)).alias(col_name)
                )
                if ftype != "string":
                    cast_map[col_name] = (ftype, ffmt)
            df = raw_df.select(*select_exprs)

            # ── 4a. Cast non-string columns ───────────────────────────────────
            for col_name, (ftype, ffmt) in cast_map.items():
                try:
                    if ftype == "date" and ffmt:
                        df = df.withColumn(col_name, F.to_date(F.col(col_name), ffmt))
                    elif ftype == "timestamp" and ffmt:
                        df = df.withColumn(col_name, F.to_timestamp(F.col(col_name), ffmt))
                    else:
                        spark_type = _spark_type(ftype)
                        df = df.withColumn(col_name, F.col(col_name).cast(spark_type))
                except Exception as ce:
                    LOG.warning("[FIXED] %s: cast failed for column %s (%s): %s", name, col_name, ftype, ce)
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
        Returns None when no fields or output_columns are configured (write all)."""
        explicit = cfg.get("output_columns") or cfg.get("columns")
        if explicit:
            want = [
                c.replace("-", "_")
                for c in (explicit if isinstance(explicit, list) else [explicit])
            ]
            return want or None

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

    def _write_fixed_width(self, df: DataFrame, name: str, cfg: dict, path: str, write_mode: str) -> None:
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
        control_file_path = cfg.get("control_file_path") or ""
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
        """Write output based on format.

        If output config defines fields/output_columns, only those columns are written.
        Column names in the written file match the schema (with hyphens preserved) so they
        align with config and reconciliation.
        """
        fmt        = (cfg.get("format") or "parquet").lower()
        path       = get_output_path(cfg, str(self.base_path))
        write_mode = (cfg.get("write_mode") or "overwrite").lower()

        if not path:
            LOG.warning("No output path for %s; skipping write", name)
            return

        # ── Resolve and select output columns ────────────────────────────────
        out_cols = self._output_columns_from_config(cfg, df)
        if out_cols:
            df = self._ensure_output_columns(df, out_cols)
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

        # ── Write in the requested format ─────────────────────────────────────
        writer = df.write.mode(write_mode)
        if fmt == "parquet":
            writer.parquet(path)
        elif fmt in ("csv", "delimited"):
            delimiter = cfg.get("delimiter") or ","
            writer.option("header", "true").option("sep", delimiter).csv(path)
        elif fmt == "fixed":
            self._write_fixed_width(df, name, cfg, path, write_mode)
            return   # _write_fixed_width handles the write itself
        elif fmt == "cobol":
            try:
                cobrix_opts = cfg.get("cobrix") or {}
                copybook    = cobrix_opts.get("copybook_path") or cfg.get("copybook") or ""
                writer.format("cobol").option("copybook", copybook).save(path)
            except Exception as e:
                LOG.warning("Cobrix write failed for %s: %s; falling back to parquet", name, e)
                writer.parquet(path)
        else:
            writer.parquet(path)

        LOG.info("Wrote %s -> %s", name, path)

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
            try:
                df = self._read_input(name, cfg)
            except Exception as e:
                LOG.warning(
                    "Could not read input %s from %s: %s; creating empty from schema",
                    name, path, e,
                )
                df = self._empty_from_schema(cfg)
            self.input_dfs[name] = df
        return self.input_dfs

    def _empty_from_schema(self, cfg: dict) -> DataFrame:
        """Create empty DataFrame with schema from config (for testing when files missing)."""
        fields = cfg.get("fields") or []
        struct_fields = []
        for i, f in enumerate(fields):
            if not isinstance(f, dict):
                continue
            col_name  = (f.get("name") or f"col_{i}").replace("-", "_")
            spark_type = _spark_type(f.get("type") or "string")
            nullable   = f.get("nullable", True)
            struct_fields.append(T.StructField(col_name, spark_type, bool(nullable)))
        schema = (
            T.StructType(struct_fields)
            if struct_fields
            else T.StructType([T.StructField("id", T.LongType(), True)])
        )
        return self.spark.createDataFrame([], schema)

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
            alias_out, result_df = apply_transformation_step(step, datasets)
            if alias_out and result_df is not None:
                datasets[alias_out]        = result_df
                self.output_dfs[alias_out] = result_df
            else:
                LOG.warning("Step %s produced no result; skipping", step_id)
        return self.output_dfs

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
            else:
                LOG.warning("No DataFrame for output %s; skipping write", name)

    def run(self) -> dict[str, DataFrame]:
        """Full dataflow: load inputs → run transformations → write outputs."""
        self.load_inputs()
        self.run_transformations()
        self.write_outputs()
        return self.output_dfs
