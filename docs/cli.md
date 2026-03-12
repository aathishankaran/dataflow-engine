# CLI Reference

The dataflow engine is invoked via `run_dataflow.py`, a command-line interface that parses arguments, builds a SparkSession, and runs the pipeline.

---

## Usage

```bash
python run_dataflow.py [config] [options]
```

---

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `config` | `samples/config.json` | Path to the pipeline configuration JSON file |
| `--base-path` | Parent directory of config | Base directory for resolving relative file paths |
| `--no-cobrix` | `False` | Disable Cobrix JAR loading (for environments without COBOL support) |
| `--master` | `local[*]` | Spark master URL (e.g., `local[*]`, `yarn`, `spark://host:7077`) |
| `--dry-run` | `False` | Parse config and load inputs but skip writing outputs |
| `--settings` | `settings.json` | Path to the runtime settings JSON file |

---

## Examples

### Basic execution

```bash
python run_dataflow.py configs/my_pipeline.json
```

### Custom base path

```bash
python run_dataflow.py configs/my_pipeline.json --base-path /data/pipelines
```

### Dry run (no output writes)

```bash
python run_dataflow.py configs/my_pipeline.json --dry-run
```

### Disable Cobrix

```bash
python run_dataflow.py configs/my_pipeline.json --no-cobrix
```

### Run on YARN cluster

```bash
python run_dataflow.py configs/my_pipeline.json --master yarn
```

### Custom settings file

```bash
python run_dataflow.py configs/my_pipeline.json --settings /etc/dataflow/settings.json
```

### Combined options

```bash
python run_dataflow.py configs/my_pipeline.json \
    --base-path /data/pipelines \
    --master local[4] \
    --settings custom_settings.json \
    --no-cobrix
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Pipeline completed successfully |
| `1` | Pipeline failed with an error (validation failure, missing file, etc.) |
| `130` | Pipeline interrupted by user (Ctrl+C / SIGINT) |

---

## SparkSession Configuration

The `_build_spark_session()` function creates a SparkSession with OS-aware defaults:

- **App name**: Set from the config file name
- **Master**: Configurable via `--master` (default: `local[*]`)
- **Cobrix**: Optionally loads the Cobrix JAR for COBOL copybook reading
- **Platform detection**: Adjusts Spark configuration for macOS, Linux, and Windows

!!! note "Local development"
    For local development, the default `local[*]` master uses all available CPU cores.
    Use `local[N]` to limit parallelism (e.g., `local[4]` for 4 cores).

---

## Config File Format

The config JSON file defines the entire pipeline. See [Pipeline Flow](pipeline.md) for the execution model and [Transformation Types](transformations-guide.md) for step configuration.

```json
{
  "name": "my_pipeline",
  "inputs": [ ... ],
  "steps": [ ... ],
  "outputs": [ ... ]
}
```
