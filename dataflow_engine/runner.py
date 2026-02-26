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
"""

import logging
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config_loader import load_config, get_input_path, get_output_path
from .transformations import apply_transformation_step

LOG = logging.getLogger(__name__)


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

    def _read_input(self, name: str, cfg: dict) -> DataFrame:
        """Read input based on format (cobol/cobrix, parquet, csv, fixed)."""
        fmt = (cfg.get("format") or "cobol").lower()
        path = get_input_path(cfg, str(self.base_path))
        cobrix_opts = cfg.get("cobrix") or {}

        if fmt == "cobol" and self.use_cobrix:
            try:
                copybook_path = cobrix_opts.get("copybook_path") or cfg.get("copybook") or ""
                copybook_full = str(self.base_path / copybook_path) if copybook_path and not copybook_path.startswith(("/", "s3:")) else copybook_path
                reader = (
                    self.spark.read.format("cobol")
                    .option("copybook", copybook_full or path)
                    .option("encoding", cobrix_opts.get("encoding", "cp037"))
                    .option("record_format", cobrix_opts.get("record_format", "F"))
                    .option("file_start_offset", str(cobrix_opts.get("file_start_offset", 0)))
                    .option("file_end_offset", str(cobrix_opts.get("file_end_offset", 0)))
                    .option("generate_record_id", str(cobrix_opts.get("generate_record_id", False)).lower())
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
        if fmt == "csv":
            # Read CSV using file header so copybook-style names (TXN-AMT) are accepted,
            # then normalize column names to underscores (TXN_AMT) to match config schema.
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)
            normalized = [c.replace("-", "_") for c in df.columns]
            if normalized != df.columns:
                df = df.toDF(*normalized)
            return df
        if fmt == "fixed":
            from pyspark.sql.types import StructType, StructField, StringType
            fields = cfg.get("fields") or []
            schema = StructType([StructField(f.get("name", f"col_{i}").replace("-", "_"), StringType(), True) for i, f in enumerate(fields)]) if fields else StructType([StructField("value", StringType(), True)])
            return self.spark.read.schema(schema).text(path)

        try:
            return self.spark.read.parquet(path)
        except Exception:
            return self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)

    def _output_columns_from_config(self, cfg: dict, df: DataFrame) -> list[str] | None:
        """Return list of column names to write, from output config fields (copybook). Skips group-level names like SUMMARY_REC."""
        fields = cfg.get("fields") or []
        if not fields:
            return None
        # Prefer explicit output columns if set
        explicit = cfg.get("output_columns") or cfg.get("columns")
        if explicit:
            want = [c.replace("-", "_") for c in (explicit if isinstance(explicit, list) else [explicit])]
        else:
            want = [f.get("name", "").replace("-", "_") for f in fields if f.get("name")]
        if not want:
            return None
        return want

    def _ensure_output_columns(self, df: DataFrame, want: list[str]) -> DataFrame:
        """Ensure df has all wanted columns; alias from a matching source column (e.g. SUM_CUST_ID from ACCT_CUST_ID) if missing."""
        df_cols = set(df.columns)
        for c in want:
            if c in df_cols:
                continue
            # Match by suffix: e.g. SUM_CUST_ID -> find column ending with _CUST_ID
            parts = c.split("_")
            for i in range(1, len(parts)):
                suffix = "_".join(parts[i:])
                match = next((x for x in df.columns if x == suffix or x.endswith("_" + suffix)), None)
                if match:
                    df = df.withColumn(c, F.col(match))
                    df_cols.add(c)
                    LOG.debug("Output: aliased %s from %s", c, match)
                    break
        return df

    def _write_output(self, df: DataFrame, name: str, cfg: dict) -> None:
        """Write output based on format. If output config defines fields/columns, only those columns are written.
        Column names in the written file match the schema (with hyphens) so they align with config and reconciliation."""
        fmt = (cfg.get("format") or "parquet").lower()
        path = get_output_path(cfg, str(self.base_path))
        write_mode = (cfg.get("write_mode") or "overwrite").lower()

        if not path:
            LOG.warning("No output path for %s; skipping write", name)
            return

        out_cols = self._output_columns_from_config(cfg, df)
        if out_cols:
            df = self._ensure_output_columns(df, out_cols)
            # Schema names (with hyphens) for written output so generated matches config/reconciliation
            fields = cfg.get("fields") or []
            schema_names = [f.get("name") for f in fields if isinstance(f, dict) and f.get("name")]
            if schema_names and len(schema_names) == len(out_cols):
                df = df.select(*[F.col(u).alias(h) for u, h in zip(out_cols, schema_names) if u in df.columns])
            else:
                df = df.select(*[c for c in out_cols if c in df.columns])
            LOG.debug("Output %s: writing columns %s", name, list(df.columns))
        elif out_cols is not None and not out_cols:
            LOG.warning("Output %s: no configured output columns; writing all columns", name)

        writer = df.write.mode(write_mode)
        if fmt == "parquet":
            writer.parquet(path)
        elif fmt == "csv":
            writer.option("header", "true").csv(path)
        elif fmt == "cobol":
            try:
                cobrix_opts = cfg.get("cobrix") or {}
                copybook = cobrix_opts.get("copybook_path") or cfg.get("copybook") or ""
                writer.format("cobol").option("copybook", copybook).save(path)
            except Exception as e:
                LOG.warning("Cobrix write failed for %s: %s; falling back to parquet", name, e)
                writer.parquet(path)
        else:
            writer.parquet(path)

        LOG.info("Wrote %s -> %s", name, path)

    def load_inputs(self) -> dict[str, DataFrame]:
        """Load all inputs from config."""
        inputs = self.config.get("Inputs") or {}
        base = str(self.base_path)
        for name, cfg in inputs.items():
            if not isinstance(cfg, dict):
                continue
            path = get_input_path(cfg, base)
            try:
                df = self._read_input(name, cfg)
            except Exception as e:
                LOG.warning("Could not read input %s from %s: %s; creating empty from schema", name, path, e)
                df = self._empty_from_schema(cfg)
            self.input_dfs[name] = df
        return self.input_dfs

    def _empty_from_schema(self, cfg: dict) -> DataFrame:
        """Create empty DataFrame with schema from config (for testing when files missing)."""
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
        type_map = {
            "string": StringType(),
            "long": LongType(),
            "int": LongType(),
            "integer": LongType(),
            "double": DoubleType(),
            "float": DoubleType(),
            "number": DoubleType(),
        }
        fields = cfg.get("fields") or []
        struct_fields = [
            StructField(
                (f.get("name") or f"col_{i}").replace("-", "_"),
                type_map.get((f.get("type") or "string").lower(), StringType()),
                True,
            )
            for i, f in enumerate(fields) if isinstance(f, dict)
        ]
        schema = StructType(struct_fields) if struct_fields else StructType([StructField("id", LongType(), True)])
        return self.spark.createDataFrame([], schema)

    def run_transformations(self) -> dict[str, DataFrame]:
        """
        Execute transformation steps one by one.
        Each step reads its source dataset(s) from the registry, applies a Spark DataFrame
        transformation, and stores the result under output_alias so the next step can use it.
        """
        steps = (self.config.get("Transformations") or {}).get("steps") or []
        # Registry of named DataFrames: inputs first, then each step output feeds the next
        datasets: dict[str, DataFrame] = dict(self.input_dfs)
        self.output_dfs.clear()
        for i, step in enumerate(steps):
            step_id = step.get("id", str(i))
            step_type = (step.get("type") or "select").lower()
            alias = step.get("output_alias") or step.get("id", "out")
            source_names = step.get("source_inputs") or []
            LOG.info("Step %s: %s (%s) <- %s -> %s", i + 1, step_id, step_type, source_names, alias)
            alias_out, result_df = apply_transformation_step(step, datasets)
            if alias_out and result_df is not None:
                datasets[alias_out] = result_df  # output of this step feeds next step
                self.output_dfs[alias_out] = result_df
            else:
                LOG.warning("Step %s produced no result; skipping", step_id)
        return self.output_dfs

    def write_outputs(self) -> None:
        """Write all outputs to configured paths. Resolves output by config name; if no match, uses single output when there is only one."""
        outputs = self.config.get("Outputs") or {}
        input_names = set(self.config.get("Inputs") or {})
        for name, cfg in outputs.items():
            if not isinstance(cfg, dict):
                continue
            df = self.output_dfs.get(name)
            if df is None:
                df = self.input_dfs.get(name)
            if df is None and len(outputs) == 1 and len(self.output_dfs) == 1:
                # Single output config and single produced df (alias may differ from config key)
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
