"""
Load and validate dataflow config JSON for PySpark execution.
"""

import json
from pathlib import Path
from typing import Any


def load_config(path: str | Path) -> dict[str, Any]:
    """
    Load dataflow configuration from JSON file.

    Expected structure:
        {
            "Inputs": { "DD_NAME": { name, dataset, format, copybook, cobrix, fields, s3_path, ... } },
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


def get_input_path(config_input: dict, base_path: str | None = None) -> str:
    """
    Resolve input file path from config (path, s3_path, dataset, or cobrix.copybook_path).
    Relative paths are resolved against base_path so --base-path works correctly.

    Args:
        config_input: Single input config dict.
        base_path: Optional base path for relative paths.

    Returns:
        Resolved path (S3 or local).
    """
    path = config_input.get("path")
    if path:
        return _resolve_path(path, base_path) if base_path else path
    s3 = config_input.get("s3_path")
    if s3:
        return _resolve_path(s3, base_path) if base_path else s3
    cobrix = config_input.get("cobrix") or {}
    cpy = cobrix.get("copybook_path") or config_input.get("copybook")
    if cpy and base_path and not _is_absolute_path(cpy):
        return str(Path(base_path) / cpy)
    if cpy:
        return cpy
    return config_input.get("dataset", "") or ""


def get_output_path(config_output: dict, base_path: str | None = None) -> str:
    """
    Resolve output path from config (path, s3_path, or dataset).
    Relative paths are resolved against base_path.
    """
    path = config_output.get("path") or config_output.get("s3_path") or config_output.get("dataset", "")
    if not path:
        return ""
    return _resolve_path(path, base_path) if base_path and not _is_absolute_path(path) else path
