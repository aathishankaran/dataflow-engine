"""Unit tests for dataflow_engine/incident.py."""
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dataflow_engine.incident import (
    MoogsoftIncidentConnector,
    IncidentCreationError,
    SEVERITY_CRITICAL,
    SEVERITY_HIGH,
    SEVERITY_MEDIUM,
    SEVERITY_LOW,
    SEVERITY_INFO,
)


class TestMoogsoftIncidentConnector:
    def test_init_with_params(self):
        conn = MoogsoftIncidentConnector(
            endpoint="https://moogsoft.example.com/events",
            api_key="test-key-123",
        )
        assert conn.endpoint == "https://moogsoft.example.com/events"
        assert conn.api_key == "test-key-123"
        assert conn.timeout == 30
        assert conn.default_source == "dataflow-engine"

    def test_init_missing_endpoint(self):
        with pytest.raises(ValueError, match="endpoint"):
            MoogsoftIncidentConnector(api_key="key")

    def test_init_missing_api_key(self):
        with pytest.raises(ValueError, match="API key"):
            MoogsoftIncidentConnector(endpoint="https://example.com")

    def test_invalid_severity(self):
        conn = MoogsoftIncidentConnector(
            endpoint="https://example.com",
            api_key="key",
        )
        with pytest.raises(ValueError, match="Invalid severity"):
            conn.create_incident(summary="test", severity="INVALID")

    def test_build_payload(self):
        payload = MoogsoftIncidentConnector._build_payload(
            summary="Test incident",
            description="Test description",
            severity="HIGH",
            source="test-source",
            alert_key="test-key",
            tags={"env": "test"},
            service="dataflow",
            additional_fields={},
        )
        assert payload["description"] == "Test incident"
        assert payload["severity"] == 4  # HIGH = 4
        assert payload["source"] == "test-source"
        assert payload["custom_info"]["env"] == "test"

    def test_convenience_methods_exist(self):
        conn = MoogsoftIncidentConnector(
            endpoint="https://example.com",
            api_key="key",
        )
        assert hasattr(conn, "create_validation_incident")
        assert hasattr(conn, "create_last_run_file_missing_incident")
        assert hasattr(conn, "create_input_file_missing_incident")
        assert hasattr(conn, "create_prev_day_check_incident")
        assert hasattr(conn, "create_record_count_check_incident")

    def test_severity_constants(self):
        assert SEVERITY_CRITICAL == "CRITICAL"
        assert SEVERITY_HIGH == "HIGH"
        assert SEVERITY_MEDIUM == "MEDIUM"
        assert SEVERITY_LOW == "LOW"
        assert SEVERITY_INFO == "INFO"


class TestIncidentCreationError:
    def test_is_runtime_error(self):
        err = IncidentCreationError("test error")
        assert isinstance(err, RuntimeError)
        assert str(err) == "test error"
