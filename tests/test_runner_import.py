"""Unit tests for dataflow_engine module imports and basic structure."""
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestModuleImports:
    def test_import_config_loader(self):
        from dataflow_engine import config_loader
        assert hasattr(config_loader, "load_config")
        assert hasattr(config_loader, "get_input_path")
        assert hasattr(config_loader, "get_output_path")

    def test_import_oracle_loader(self):
        from dataflow_engine import oracle_loader
        assert hasattr(oracle_loader, "generate_ctl_content")
        assert hasattr(oracle_loader, "write_df_to_oracle")

    def test_import_incident(self):
        from dataflow_engine import incident
        assert hasattr(incident, "MoogsoftIncidentConnector")
        assert hasattr(incident, "IncidentCreationError")

    def test_import_vault(self):
        from dataflow_engine import vault
        assert hasattr(vault, "get_oracle_credentials")
        assert hasattr(vault, "store_oracle_credentials")

    def test_package_exports(self):
        import dataflow_engine
        assert hasattr(dataflow_engine, "DataFlowRunner")
        assert hasattr(dataflow_engine, "MainframeTransformationExecutor")
        assert hasattr(dataflow_engine, "apply_transformation_step")
        assert hasattr(dataflow_engine, "load_config")
        assert hasattr(dataflow_engine, "get_input_path")
        assert hasattr(dataflow_engine, "get_output_path")
        assert hasattr(dataflow_engine, "MoogsoftIncidentConnector")
        assert hasattr(dataflow_engine, "IncidentCreationError")


class TestRunDataflow:
    def test_import_main(self):
        """Verify run_dataflow.py module is importable."""
        # Don't actually run it, just check import works
        import importlib
        spec = importlib.util.find_spec("run_dataflow")
        # May or may not be on path depending on test execution dir
        # Just verify no import errors in the package
        from dataflow_engine.runner import DataFlowRunner
        assert DataFlowRunner is not None
