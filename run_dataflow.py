#!/usr/bin/env python3
"""
Run mainframe dataflow from generated config JSON.

Usage:
    python run_dataflow.py samples/config.json
    python run_dataflow.py samples/config.json --base-path .
    python run_dataflow.py samples/config.json --dry-run

Windows: Set JAVA_HOME to your JDK and (recommended) HADOOP_HOME to a folder
containing bin/winutils.exe to avoid ExitCodeException -1073741515. See WINDOWS.md.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from dataflow_engine.runner import DataFlowRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run mainframe dataflow from config JSON",
    )
    parser.add_argument(
        "config",
        type=Path,
        default=Path("samples/config.json"),
        nargs="?",
        help="Path to config JSON file",
    )
    parser.add_argument(
        "--base-path",
        type=Path,
        default=None,
        help="Base path for relative file paths (default: project root)",
    )
    parser.add_argument(
        "--no-cobrix",
        action="store_true",
        help="Disable Cobrix; use parquet/csv for inputs",
    )
    parser.add_argument(
        "--master",
        type=str,
        default="local[*]",
        help="Spark master URL",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load inputs and run transformations only; do not write outputs",
    )
    args = parser.parse_args()

    config_path = args.config
    if not config_path.exists():
        raise SystemExit(f"Config file not found: {config_path}")

    # Default base_path: project root (parent of script)
    base_path = args.base_path or Path(__file__).resolve().parent

    builder = (
        SparkSession.builder
        .appName("mainframe_migration")
        .master(args.master)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )

    # Windows: avoid ExitCodeException -1073741515 (missing winutils / Hadoop native)
    if sys.platform == "win32":
        hadoop_home = os.environ.get("HADOOP_HOME", "").strip()
        if hadoop_home:
            # Must be an absolute path. Fix "C:hadoop" (invalid) -> "C:\hadoop"
            hadoop_path = Path(hadoop_home)
            if not hadoop_path.is_absolute():
                # Windows: "C:hadoop" means drive C + path; make it "C:\hadoop"
                if len(hadoop_home) >= 2 and hadoop_home[1] == ":" and (len(hadoop_home) == 2 or hadoop_home[2] != "\\"):
                    hadoop_home = hadoop_home[0:2] + "\\" + hadoop_home[2:]
                    hadoop_path = Path(hadoop_home)
                else:
                    hadoop_path = hadoop_path.resolve()
                hadoop_home = str(hadoop_path)
            # Escape backslashes for JVM so it receives C:\hadoop correctly
            hadoop_home_java = hadoop_home.replace("\\", "\\\\")
            builder = builder.config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={hadoop_home_java}")
        else:
            logging.warning(
                "Windows detected and HADOOP_HOME is not set. If you see ExitCodeException -1073741515, "
                "set HADOOP_HOME to a folder containing bin/winutils.exe. See dataflow-engine/WINDOWS.md"
            )
        # Use a local temp dir to reduce path/permission issues
        local_dir = os.environ.get("SPARK_LOCAL_DIR") or os.environ.get("TEMP") or os.environ.get("TMP") or "."
        if local_dir:
            builder = builder.config("spark.local.dir", local_dir)

    spark = builder.getOrCreate()

    runner = DataFlowRunner(
        spark=spark,
        config_path=config_path,
        base_path=base_path,
        use_cobrix=not args.no_cobrix,
    )

    runner.load_inputs()
    runner.run_transformations()

    if args.dry_run:
        print("Dry run: skipping output writes.")
        for name, df in runner.output_dfs.items():
            count = df.count()
            print(f"  {name}: {count} rows")
    else:
        runner.write_outputs()
        print("Dataflow completed. Outputs written to samples/output/")


if __name__ == "__main__":
    main()
