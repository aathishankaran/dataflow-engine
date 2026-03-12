"""Unit tests for dataflow_engine/config_loader.py."""
import json
import os
import pytest
import sys
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dataflow_engine.config_loader import (
    load_config,
    get_input_path,
    get_output_path,
    get_previous_business_day,
    get_control_file_path,
    has_count_file,
    get_frequency,
)


class TestLoadConfig:
    def test_load_valid_config(self, sample_config_file):
        config = load_config(sample_config_file)
        assert "Inputs" in config
        assert "Outputs" in config
        assert "Transformations" in config

    def test_load_nonexistent_file(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.json")

    def test_load_invalid_json(self, temp_dir):
        path = os.path.join(temp_dir, "bad.json")
        with open(path, "w") as f:
            f.write("not valid json{{{")
        with pytest.raises(Exception):
            load_config(path)

    def test_load_missing_inputs(self, temp_dir):
        path = os.path.join(temp_dir, "no_inputs.json")
        with open(path, "w") as f:
            json.dump({"other": "data"}, f)
        with pytest.raises(ValueError):
            load_config(path)

    def test_load_lowercase_keys(self, temp_dir):
        path = os.path.join(temp_dir, "lower.json")
        with open(path, "w") as f:
            json.dump({
                "inputs": {"IN": {"name": "IN"}},
                "outputs": {"OUT": {"name": "OUT"}},
                "transformations": {"steps": []},
            }, f)
        config = load_config(path)
        assert "Inputs" in config
        assert "IN" in config["Inputs"]


class TestGetInputPath:
    def test_legacy_path(self):
        inp = {"path": "/data/input.csv"}
        assert get_input_path(inp) == "/data/input.csv"

    def test_s3_path(self):
        inp = {"s3_path": "s3://bucket/data/input"}
        assert get_input_path(inp) == "s3://bucket/data/input"

    def test_source_path_with_file(self):
        inp = {
            "source_path": "s3://raw",
            "source_file_name": "data.dat",
        }
        result = get_input_path(inp)
        assert "data.dat" in result

    def test_v2_dataset_name(self):
        inp = {"dataset_name": "TX.DAT"}
        settings = {"raw_bucket_prefix": "s3://raw"}
        result = get_input_path(inp, settings=settings)
        assert "TX.DAT" in result

    def test_with_base_path(self):
        inp = {"path": "relative/input.csv"}
        result = get_input_path(inp, base_path="/base")
        assert result.startswith("/base")

    def test_empty_config(self):
        result = get_input_path({})
        assert result == ""

    def test_dataset_fallback(self):
        inp = {"dataset": "PROD.DATA.FILE"}
        assert get_input_path(inp) == "PROD.DATA.FILE"


class TestGetOutputPath:
    def test_legacy_path(self):
        out = {"path": "/data/output"}
        assert get_output_path(out) == "/data/output"

    def test_s3_path(self):
        out = {"s3_path": "s3://bucket/output"}
        assert get_output_path(out) == "s3://bucket/output"

    def test_source_path_with_frequency(self):
        out = {
            "source_path": "s3://curated",
            "frequency": "DAILY",
        }
        result = get_output_path(out)
        assert "DAILY" in result

    def test_empty_config(self):
        assert get_output_path({}) == ""


class TestGetPreviousBusinessDay:
    def test_normal_day(self):
        # Tuesday March 10 → Monday March 9
        result = get_previous_business_day(date(2026, 3, 10), [])
        assert result == date(2026, 3, 9)

    def test_skip_holiday(self):
        # Day after a holiday should skip the holiday
        result = get_previous_business_day(date(2026, 1, 2), ["2026-01-01"])
        assert result == date(2025, 12, 31)

    def test_multiple_holidays(self):
        holidays = ["2026-03-09", "2026-03-08"]
        result = get_previous_business_day(date(2026, 3, 10), holidays)
        assert result == date(2026, 3, 7)

    def test_weekend_not_skipped(self):
        # This system does NOT skip weekends
        # Monday March 9, 2026 → Sunday March 8
        result = get_previous_business_day(date(2026, 3, 9), [])
        assert result == date(2026, 3, 8)

    def test_structured_holidays(self):
        holidays = [
            {"active": True, "name": "New Year", "date": "2026-01-01"},
            {"active": False, "name": "Inactive", "date": "2026-01-02"},
        ]
        result = get_previous_business_day(date(2026, 1, 2), holidays)
        assert result == date(2025, 12, 31)  # Skips active Jan 1

    def test_inactive_holiday_not_skipped(self):
        holidays = [
            {"active": False, "name": "Inactive", "date": "2026-03-09"},
        ]
        result = get_previous_business_day(date(2026, 3, 10), holidays)
        assert result == date(2026, 3, 9)  # Not skipped because inactive


class TestGetControlFilePath:
    def test_with_control_file_name(self):
        inp = {
            "source_path": "s3://raw",
            "control_file_name": "count.ctl",
        }
        result = get_control_file_path(inp)
        assert "count.ctl" in result

    def test_no_control_file(self):
        assert get_control_file_path({}) == ""

    def test_legacy_count_file_path(self):
        inp = {"count_file_path": "/data/counts/ctl.dat"}
        result = get_control_file_path(inp)
        assert result == "/data/counts/ctl.dat"


class TestHasCountFile:
    def test_explicit_flag(self):
        assert has_count_file({"has_count_file": True}) is True

    def test_implicit_from_name(self):
        assert has_count_file({"control_file_name": "count.ctl"}) is True

    def test_no_count_file(self):
        assert has_count_file({}) is False


class TestGetFrequency:
    def test_found_in_inputs(self):
        config = {"Inputs": {"IN1": {"frequency": "DAILY"}}}
        assert get_frequency(config, "IN1") == "DAILY"

    def test_found_in_outputs(self):
        config = {"Outputs": {"OUT1": {"frequency": "monthly"}}}
        assert get_frequency(config, "OUT1") == "MONTHLY"

    def test_not_found(self):
        config = {"Inputs": {}, "Outputs": {}}
        assert get_frequency(config, "MISSING") is None
