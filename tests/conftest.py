import json
import os
import sys
import tempfile
import shutil
import types
import pytest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ---------------------------------------------------------------------------
# Mock heavy dependencies (PySpark, hvac, requests) so unit tests run without
# them installed.  These stubs satisfy import-time attribute access only.
# ---------------------------------------------------------------------------


class _StubModule(types.ModuleType):
    """Module stub that returns a callable placeholder for any attribute."""
    def __init__(self, name):
        super(_StubModule, self).__init__(name)
        self.__path__ = []
        self.__all__ = []
        self.__file__ = "<stub>"

    def __getattr__(self, name):
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        # Return a class-like callable that also supports attribute access
        return type(name, (), {
            "__init__": lambda self, *a, **kw: None,
            "__getattr__": lambda self, n: type(n, (), {}),
            "__call__": lambda self, *a, **kw: self,
        })


_STUB_ROOTS = [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "pyspark.sql.window", "pyspark.sql.utils", "pyspark.sql.dataframe",
    "hvac",
]

for _name in _STUB_ROOTS:
    if _name not in sys.modules:
        _stub = _StubModule(_name)
        sys.modules[_name] = _stub
        # Wire parent→child
        parts = _name.split(".")
        if len(parts) > 1:
            parent = ".".join(parts[:-1])
            if parent in sys.modules:
                setattr(sys.modules[parent], parts[-1], _stub)


@pytest.fixture
def temp_dir():
    """Create a temporary directory, clean up after test."""
    d = tempfile.mkdtemp()
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def sample_config_dict():
    """Return a minimal valid config dict."""
    return {
        "Inputs": {
            "TEST-INPUT": {
                "name": "TEST-INPUT",
                "format": "CSV",
                "fields": [
                    {"name": "ID", "type": "string"},
                    {"name": "AMOUNT", "type": "long"},
                    {"name": "STATUS", "type": "string"},
                ],
                "path": "/tmp/test_input.csv",
            }
        },
        "Outputs": {
            "TEST-OUTPUT": {
                "name": "TEST-OUTPUT",
                "format": "CSV",
                "fields": [
                    {"name": "ID", "type": "string"},
                    {"name": "AMOUNT", "type": "long"},
                ],
                "path": "/tmp/test_output",
            }
        },
        "Transformations": {
            "steps": [
                {
                    "id": "filter_step",
                    "type": "filter",
                    "source_inputs": ["TEST-INPUT"],
                    "logic": {
                        "conditions": [{"field": "AMOUNT", "operation": ">", "value": 0}]
                    },
                    "output_alias": "TEST-OUTPUT",
                }
            ]
        },
    }


@pytest.fixture
def sample_config_file(temp_dir, sample_config_dict):
    """Write sample config to a temp file and return path."""
    path = os.path.join(temp_dir, "test_config.json")
    with open(path, "w") as f:
        json.dump(sample_config_dict, f)
    return path
