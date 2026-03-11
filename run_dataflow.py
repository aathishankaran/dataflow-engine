#!/usr/bin/env python3
"""
Run mainframe dataflow from generated config JSON.
Python 3.12+ compatible.  Runs on Linux, macOS, and Windows.

Usage:
    python run_dataflow.py samples/config.json
    python run_dataflow.py samples/config.json --base-path /data/pipelines
    python run_dataflow.py samples/config.json --master spark://host:7077
    python run_dataflow.py samples/config.json --dry-run

The runner auto-detects the host OS (Linux / macOS / Windows) and applies
the appropriate PySpark / Hadoop / Java configuration at startup.

Environment variables (optional):
    JAVA_HOME           – path to JDK (required for Spark)
    HADOOP_HOME         – path to Hadoop / winutils.exe directory (Windows only)
    SPARK_LOCAL_DIRS    – override Spark local scratch directory
    AWS_ACCESS_KEY_ID   – explicit AWS key (use IAM roles instead in production)
    AWS_SECRET_ACCESS_KEY
    AWS_REGION          – default us-east-1
"""

import argparse
import logging
import os
import platform
import sys
from pathlib import Path

# ── import runner FIRST so _configure_spark_for_os() runs before SparkSession
from dataflow_engine.runner import DataFlowRunner, _IS_WINDOWS, _IS_MACOS, _IS_LINUX

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOG = logging.getLogger(__name__)


def _build_spark_session(master: str) -> SparkSession:
    """
    Create a SparkSession with settings appropriate for the host OS and
    the current Python 3.12 runtime.
    """
    LOG.info("Building SparkSession: OS=%s, master=%s, Python=%s",
             platform.system(), master, sys.version.split()[0])

    builder = (
        SparkSession.builder
        .appName("dataflow_engine")
        .master(master)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Ensure PySpark workers use the same Python interpreter
        .config("spark.pyspark.python",        sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )

    if _IS_WINDOWS:
        # ── Windows-specific settings ──────────────────────────────────────
        hadoop_home = os.environ.get("HADOOP_HOME", "").strip()
        if hadoop_home:
            # Normalise path (fix "C:hadoop" → "C:\hadoop")
            hpath = Path(hadoop_home)
            if not hpath.is_absolute():
                if len(hadoop_home) >= 2 and hadoop_home[1] == ":" and \
                   (len(hadoop_home) == 2 or hadoop_home[2] != "\\"):
                    hadoop_home = hadoop_home[:2] + "\\" + hadoop_home[2:]
                    hpath = Path(hadoop_home)
                else:
                    hpath = hpath.resolve()
                hadoop_home = str(hpath)
            # Pass HADOOP_HOME to the JVM via driver Java options
            hadoop_home_jvm = hadoop_home.replace("\\", "\\\\")
            builder = builder.config(
                "spark.driver.extraJavaOptions",
                f"-Dhadoop.home.dir={hadoop_home_jvm}"
            )
            LOG.info("Windows: HADOOP_HOME=%s", hadoop_home)
        else:
            LOG.warning(
                "Windows: HADOOP_HOME not set.  If you encounter ExitCodeException "
                "-1073741515, set HADOOP_HOME to a folder containing bin/winutils.exe."
            )

        # Use a Windows-friendly local scratch directory
        local_dir = (os.environ.get("SPARK_LOCAL_DIRS") or
                     os.environ.get("TEMP") or
                     os.environ.get("TMP") or
                     "C:\\Temp")
        scratch = str(Path(local_dir) / "spark_tmp")
        Path(scratch).mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.local.dir", scratch)
        LOG.debug("Windows: spark.local.dir=%s", scratch)

    elif _IS_MACOS:
        # ── macOS-specific settings ────────────────────────────────────────
        # On Apple Silicon (arm64) some Spark native libs may not be present;
        # falling back to Java implementation avoids native-lib errors.
        builder = builder.config("spark.io.compression.codec", "snappy")
        LOG.debug("macOS: applied macOS-specific Spark configuration")

    else:
        # ── Linux (default production environment) ─────────────────────────
        LOG.debug("Linux: using default Spark configuration")

    return builder.getOrCreate()


def _error_messages(exc: BaseException) -> list[str]:
    """Walk the exception chain and return clean message strings (no traceback)."""
    messages: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        msg = str(current).strip()
        if msg:
            messages.append(msg)
        # Follow explicit __cause__ first, then implicit __context__
        next_exc = current.__cause__
        if next_exc is None and not current.__suppress_context__:
            next_exc = current.__context__
        current = next_exc
    return messages


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run mainframe dataflow from config JSON (Python 3.12 / cross-platform)",
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
        help="Base path for relative file paths (default: parent of config file)",
    )
    parser.add_argument(
        "--no-cobrix",
        action="store_true",
        help="Disable Cobrix COBOL reader; use Parquet/CSV for inputs",
    )
    parser.add_argument(
        "--master",
        type=str,
        default="local[*]",
        help="Spark master URL (default: local[*])",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load inputs and run transformations only; do not write outputs",
    )
    parser.add_argument(
        "--settings",
        type=Path,
        default=None,
        help="Path to settings.json (bucket prefixes for path derivation)",
    )
    args = parser.parse_args()

    config_path = args.config
    if not config_path.exists():
        LOG.error("Config file not found: %s", config_path)
        sys.exit(1)

    try:
        base_path = args.base_path or Path(__file__).resolve().parent

        spark = _build_spark_session(master=args.master)

        import json as _json
        settings = {}
        settings_path = args.settings or Path(__file__).resolve().parent / "settings.json"
        if settings_path.exists():
            settings = _json.loads(settings_path.read_text(encoding="utf-8"))
            LOG.info("Loaded settings from %s", settings_path)

        runner = DataFlowRunner(
            spark=spark,
            config_path=config_path,
            base_path=base_path,
            use_cobrix=not args.no_cobrix,
            settings=settings,
        )

        runner.load_inputs()
        runner.run_transformations()

        if args.dry_run:
            LOG.info("Dry run — skipping output writes.")
            for name, df in runner.output_dfs.items():
                count = df.count()
                LOG.info("  %s: %d rows", name, count)
        else:
            runner.write_outputs()
            LOG.info("Dataflow completed. Outputs written.")

    except KeyboardInterrupt:
        LOG.error("Dataflow interrupted by user.")
        sys.exit(130)
    except Exception as exc:
        sep = "─" * 70
        LOG.error(sep)
        LOG.error("DATAFLOW FAILED")
        LOG.error(sep)
        for msg in _error_messages(exc):
            LOG.error("  %s", msg)
        LOG.error(sep)
        sys.exit(1)


if __name__ == "__main__":
    main()
