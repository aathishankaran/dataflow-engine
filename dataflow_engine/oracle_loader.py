"""
Oracle SQL*Loader integration for the Dataflow Engine.

Workflow
--------
1. Fetch Oracle credentials from HashiCorp Vault at runtime.
2. Coalesce the Spark DataFrame into a single CSV partition (local temp file).
3. Generate a SQL*Loader control (.ctl) file from the DataFrame's schema.
4. Execute ``sqlldr`` via subprocess to load the CSV into the Oracle table.
5. Clean up temporary files.

Supported load modes
--------------------
INSERT          — table must be empty; aborts if any rows exist.
APPEND          — adds rows to existing data (default).
TRUNCATE_INSERT — truncates the table before loading.
REPLACE         — drops and recreates the table contents (DELETE then INSERT).

SQL*Loader control file field-type mapping
------------------------------------------
Spark type      → SQL*Loader type
-----------       ------------------
string          → CHAR
integer/long    → INTEGER EXTERNAL
double/float    → DECIMAL EXTERNAL
decimal(p,s)    → DECIMAL EXTERNAL
boolean         → CHAR
date            → DATE "YYYY-MM-DD"
timestamp       → TIMESTAMP "YYYY-MM-DD HH24:MI:SS"
binary          → RAW

Environment / prerequisites
---------------------------
- ``sqlldr`` must be on the PATH (part of Oracle Instant Client or Oracle DB Client).
- The ``VAULT_ADDR`` / ``VAULT_TOKEN`` (or AppRole) env vars must be set.
"""

import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

from pyspark.sql import DataFrame

from .vault import get_oracle_credentials

LOG = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# SQL*Loader constants
# ─────────────────────────────────────────────────────────────────────────────

_LOAD_MODE_MAP: Dict[str, str] = {
    "INSERT":          "INSERT",
    "APPEND":          "APPEND",
    "TRUNCATE_INSERT": "TRUNCATE",
    "REPLACE":         "REPLACE",
}

# Spark simple-string type → SQL*Loader field directive
_SPARK_TO_SQLLDR: Dict[str, str] = {
    "string":    "CHAR",
    "integer":   "INTEGER EXTERNAL",
    "int":       "INTEGER EXTERNAL",
    "long":      "INTEGER EXTERNAL",
    "bigint":    "INTEGER EXTERNAL",
    "short":     "INTEGER EXTERNAL",
    "byte":      "INTEGER EXTERNAL",
    "double":    "DECIMAL EXTERNAL",
    "float":     "DECIMAL EXTERNAL",
    "decimal":   "DECIMAL EXTERNAL",
    "boolean":   "CHAR",
    "date":      'DATE "YYYY-MM-DD"',
    "timestamp": 'TIMESTAMP "YYYY-MM-DD HH24:MI:SS"',
    "binary":    "RAW",
}


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────

def write_df_to_oracle(
    df: DataFrame,
    logic: dict,
    pipeline_name: str = "",
    step_id: str = "",
) -> None:
    """Write a Spark DataFrame to Oracle using SQL*Loader.

    Parameters
    ----------
    df            : Spark DataFrame whose rows will be loaded into Oracle.
    logic         : ``oracle_write`` step logic dict from the config JSON.
    pipeline_name : Pipeline name (used only in log messages).
    step_id       : Step ID (used in log messages and temp file names).

    Raises
    ------
    RuntimeError  : on missing config, Vault errors, or sqlldr failure.
    """
    cfg = _parse_logic(logic, step_id)

    # ── 1. Fetch credentials from Vault ─────────────────────────────────────
    LOG.info("[oracle_write:%s] Fetching credentials from Vault path '%s'", step_id, cfg["vault_path"])
    username, password = get_oracle_credentials(
        vault_path=cfg["vault_path"],
        username_key=cfg["vault_username_key"],
        password_key=cfg["vault_password_key"],
    )

    # ── 2. Create temp workspace ─────────────────────────────────────────────
    safe_id = (step_id or "oracle_write").replace("/", "_").replace(" ", "_")
    tmp_dir = tempfile.mkdtemp(prefix=f"df_{safe_id}_")
    LOG.debug("[oracle_write:%s] Temp directory: %s", step_id, tmp_dir)

    try:
        # ── 3. Write DataFrame to CSV ────────────────────────────────────────
        data_file = _write_df_to_csv(df, tmp_dir, step_id)

        # ── 4. Generate SQL*Loader control file ──────────────────────────────
        ctl_path = _write_ctl_file(df, cfg, tmp_dir, step_id)

        # ── 5. Ensure output directories exist ───────────────────────────────
        for out_path in [cfg["bad_file_path"], cfg["log_file_path"]]:
            _ensure_dir(out_path)
        if cfg.get("discard_file_path"):
            _ensure_dir(cfg["discard_file_path"])

        # ── 6. Build connect string & run sqlldr ─────────────────────────────
        connect_str = _build_connect_string(
            host=cfg["host"],
            port=cfg["port"],
            service_name=cfg["service_name"],
            username=username,
            password=password,
        )
        LOG.info(
            "[oracle_write:%s] Loading into %s.%s on %s:%d/%s (mode=%s)",
            step_id, cfg["schema"], cfg["table"],
            cfg["host"], cfg["port"], cfg["service_name"],
            cfg["load_mode"],
        )
        _run_sqlldr(
            ctl_file=ctl_path,
            data_file=data_file,
            connect_string=connect_str,
            log_file=cfg["log_file_path"],
            bad_file=cfg["bad_file_path"],
            discard_file=cfg.get("discard_file_path"),
        )
        LOG.info("[oracle_write:%s] Load complete.", step_id)

    finally:
        # ── 7. Clean up temp files ───────────────────────────────────────────
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            LOG.debug("[oracle_write:%s] Removed temp directory: %s", step_id, tmp_dir)
        except Exception as cleanup_err:
            LOG.warning("[oracle_write:%s] Could not remove temp dir %s: %s", step_id, tmp_dir, cleanup_err)


# ─────────────────────────────────────────────────────────────────────────────
# Control-file generation (public — can be used in tests)
# ─────────────────────────────────────────────────────────────────────────────

def generate_ctl_content(
    table_name: str,
    schema: str,
    columns: List[str],
    spark_types: List[str],
    load_mode: str = "APPEND",
    bad_file: str = "/tmp/sqlldr/output.bad",
    log_file: str = "/tmp/sqlldr/output.log",
    discard_file: Optional[str] = None,
    batch_size: int = 10000,
) -> str:
    """Return the text content of a SQL*Loader .ctl file.

    Parameters
    ----------
    table_name   : Oracle table name (no schema prefix).
    schema       : Oracle schema / owner name.  Pass ``""`` to omit.
    columns      : List of column names (must match DataFrame column order).
    spark_types  : Corresponding Spark ``simpleString`` types.
    load_mode    : One of INSERT, APPEND, TRUNCATE_INSERT, REPLACE.
    bad_file     : Filesystem path for the ``BADFILE`` directive.
    log_file     : Filesystem path for the ``LOGFILE`` directive (informational).
    discard_file : Optional path for the ``DISCARDFILE`` directive.
    batch_size   : Value of the ``ROWS`` parameter (rows per array insert).

    Returns
    -------
    str : complete .ctl file content.
    """
    sqlldr_method = _LOAD_MODE_MAP.get(load_mode.upper(), "APPEND")
    full_table = f"{schema.upper()}.{table_name.upper()}" if schema else table_name.upper()

    discard_line = f"\nDISCARDFILE '{discard_file}'" if discard_file else ""

    field_lines: List[str] = []
    for col, stype in zip(columns, spark_types):
        base_type = stype.lower().split("(")[0].strip()   # "decimal(18,4)" → "decimal"
        sqlldr_type = _SPARK_TO_SQLLDR.get(base_type, "CHAR")
        field_lines.append(f"  {col.upper()} {sqlldr_type}")

    fields_str = ",\n".join(field_lines)

    return (
        f"-- SQL*Loader Control File — generated by Dataflow Engine\n"
        f"-- Table: {full_table}  Mode: {sqlldr_method}\n"
        f"{sqlldr_method}\n"
        f"INTO TABLE {full_table}\n"
        f"  FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'\n"
        f"  TRAILING NULLCOLS\n"
        f"  ROWS={batch_size}\n"
        f"  BADFILE '{bad_file}'"
        f"{discard_line}\n"
        f"(\n"
        f"{fields_str}\n"
        f")\n"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Private helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_logic(logic: dict, step_id: str) -> dict:
    """Validate and normalise the oracle_write logic dict."""
    host         = (logic.get("host") or "").strip()
    port         = int(logic.get("port") or 1521)
    service_name = (logic.get("service_name") or "").strip()
    schema       = (logic.get("schema") or "").strip()
    table        = (logic.get("table") or "").strip()
    vault_path   = (logic.get("vault_path") or "").strip()

    if not host:
        raise RuntimeError(f"[oracle_write:{step_id}] 'host' is required in oracle_write logic.")
    if not service_name:
        raise RuntimeError(f"[oracle_write:{step_id}] 'service_name' is required in oracle_write logic.")
    if not table:
        raise RuntimeError(f"[oracle_write:{step_id}] 'table' is required in oracle_write logic.")
    if not vault_path:
        raise RuntimeError(f"[oracle_write:{step_id}] 'vault_path' is required for Vault credential lookup.")

    return {
        "host":             host,
        "port":             port,
        "service_name":     service_name,
        "schema":           schema,
        "table":            table,
        "load_mode":        (logic.get("load_mode") or "APPEND").upper(),
        "bad_file_path":    (logic.get("bad_file_path") or "/tmp/sqlldr/output.bad").strip(),
        "log_file_path":    (logic.get("log_file_path") or "/tmp/sqlldr/output.log").strip(),
        "discard_file_path": (logic.get("discard_file_path") or "").strip() or None,
        "batch_size":       int(logic.get("batch_size") or 10000),
        "vault_path":       vault_path,
        "vault_username_key": (logic.get("vault_username_key") or "username").strip() or "username",
        "vault_password_key": (logic.get("vault_password_key") or "password").strip() or "password",
    }


def _write_df_to_csv(df: DataFrame, tmp_dir: str, step_id: str) -> str:
    """Coalesce the DataFrame to one partition and write as CSV; return the part file path."""
    csv_dir = os.path.join(tmp_dir, "data")
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "false")
        .option("quoteAll", "true")
        .csv(csv_dir)
    )
    part_files = [f for f in os.listdir(csv_dir) if f.startswith("part-") and f.endswith(".csv")]
    if not part_files:
        raise RuntimeError(
            f"[oracle_write:{step_id}] No CSV part file generated in {csv_dir}. "
            "The DataFrame may be empty."
        )
    data_file = os.path.join(csv_dir, part_files[0])
    row_count = df.count()
    LOG.info("[oracle_write:%s] DataFrame written to CSV: %s (%d rows)", step_id, data_file, row_count)
    return data_file


def _write_ctl_file(df: DataFrame, cfg: dict, tmp_dir: str, step_id: str) -> str:
    """Generate the SQL*Loader .ctl file and return its path."""
    columns    = df.columns
    spark_types = [str(f.dataType.simpleString()) for f in df.schema.fields]

    ctl_content = generate_ctl_content(
        table_name=cfg["table"],
        schema=cfg["schema"],
        columns=columns,
        spark_types=spark_types,
        load_mode=cfg["load_mode"],
        bad_file=cfg["bad_file_path"],
        log_file=cfg["log_file_path"],
        discard_file=cfg.get("discard_file_path"),
        batch_size=cfg["batch_size"],
    )

    safe_id = (step_id or "sqlldr").replace("/", "_").replace(" ", "_")
    ctl_path = os.path.join(tmp_dir, f"{safe_id}.ctl")
    Path(ctl_path).write_text(ctl_content, encoding="utf-8")

    LOG.info("[oracle_write:%s] Control file: %s", step_id, ctl_path)
    LOG.debug("Control file content:\n%s", ctl_content)
    return ctl_path


def _build_connect_string(
    host: str,
    port: int,
    service_name: str,
    username: str,
    password: str,
) -> str:
    """Return an Oracle EZConnect string: ``user/pass@//host:port/service``."""
    return f"{username}/{password}@//{host}:{port}/{service_name}"


def _ensure_dir(file_path: str) -> None:
    """Create parent directory of *file_path* if it does not exist."""
    parent = os.path.dirname(file_path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _run_sqlldr(
    ctl_file: str,
    data_file: str,
    connect_string: str,
    log_file: str,
    bad_file: str,
    discard_file: Optional[str] = None,
) -> None:
    """Execute SQL*Loader as a subprocess.

    Exit codes:
      0 — success (all rows loaded)
      1 — warnings (some rows rejected — written to bad_file)
      2 — fatal error

    Raises
    ------
    RuntimeError : if sqlldr is not on PATH or exits with code 2+.
    """
    sqlldr_bin = shutil.which("sqlldr") or shutil.which("sqlldr.exe")
    if not sqlldr_bin:
        raise RuntimeError(
            "SQL*Loader ('sqlldr') not found on PATH. "
            "Install Oracle Instant Client tools and ensure 'sqlldr' is accessible."
        )

    cmd = [
        sqlldr_bin,
        connect_string,          # user/pass@//host:port/service
        f"CONTROL={ctl_file}",
        f"DATA={data_file}",
        f"LOG={log_file}",
        f"BAD={bad_file}",
    ]
    if discard_file:
        cmd.append(f"DISCARD={discard_file}")

    # Build a sanitised command for logging (password masked)
    safe_connect = _mask_password(connect_string)
    safe_cmd = [sqlldr_bin, safe_connect] + cmd[2:]
    LOG.info("Running: %s", " ".join(safe_cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,           # 1-hour hard ceiling
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            "SQL*Loader process timed out after 1 hour. "
            f"Check the log file for partial progress: {log_file}"
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Failed to launch SQL*Loader: {exc}") from exc

    if result.stdout:
        LOG.info("sqlldr stdout:\n%s", result.stdout.strip())
    if result.stderr:
        LOG.warning("sqlldr stderr:\n%s", result.stderr.strip())

    if result.returncode == 0:
        LOG.info("SQL*Loader: all rows loaded successfully.")
    elif result.returncode == 1:
        LOG.warning(
            "SQL*Loader: completed with warnings — some rows were rejected. "
            "Check bad file: %s", bad_file
        )
    else:
        raise RuntimeError(
            f"SQL*Loader failed (exit code {result.returncode}). "
            f"Review log: {log_file}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )


def _mask_password(connect_string: str) -> str:
    """Replace the password in a connect string with **** for safe logging."""
    # Format:  user/password@//host:port/service
    at_idx = connect_string.find("@")
    slash_idx = connect_string.find("/")
    if slash_idx < 0 or slash_idx >= at_idx:
        return connect_string   # unexpected format — return as-is
    user = connect_string[:slash_idx]
    rest = connect_string[at_idx:]      # e.g.  @//host:port/service
    return f"{user}/****{rest}"
