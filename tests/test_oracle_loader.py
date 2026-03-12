"""Unit tests for dataflow_engine/oracle_loader.py — CTL generation."""
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dataflow_engine.oracle_loader import generate_ctl_content


class TestGenerateCtlContent:
    def test_basic_generation(self):
        ctl = generate_ctl_content(
            table_name="TEST_TBL",
            schema="MYSCHEMA",
            columns=["ID", "NAME", "AMOUNT"],
            spark_types=["string", "string", "decimal"],
        )
        assert "MYSCHEMA.TEST_TBL" in ctl
        assert "ID CHAR" in ctl
        assert "NAME CHAR" in ctl
        assert "AMOUNT DECIMAL EXTERNAL" in ctl

    def test_all_type_mappings(self):
        cols = ["C1", "C2", "C3", "C4", "C5", "C6", "C7"]
        types = ["string", "long", "double", "date", "timestamp", "boolean", "binary"]
        ctl = generate_ctl_content(
            table_name="TBL",
            schema="S",
            columns=cols,
            spark_types=types,
        )
        assert "C1 CHAR" in ctl
        assert "C2 INTEGER EXTERNAL" in ctl
        assert "C3 DECIMAL EXTERNAL" in ctl
        assert 'C4 DATE "YYYY-MM-DD"' in ctl
        assert 'C5 TIMESTAMP "YYYY-MM-DD HH24:MI:SS"' in ctl
        assert "C6 CHAR" in ctl  # boolean -> CHAR
        assert "C7 RAW" in ctl

    def test_insert_mode(self):
        ctl = generate_ctl_content(
            table_name="T", schema="S",
            columns=["X"], spark_types=["string"],
            load_mode="INSERT",
        )
        assert "INSERT" in ctl

    def test_append_mode(self):
        ctl = generate_ctl_content(
            table_name="T", schema="S",
            columns=["X"], spark_types=["string"],
            load_mode="APPEND",
        )
        assert "APPEND" in ctl

    def test_truncate_mode(self):
        ctl = generate_ctl_content(
            table_name="T", schema="S",
            columns=["X"], spark_types=["string"],
            load_mode="TRUNCATE_INSERT",
        )
        assert "TRUNCATE" in ctl

    def test_replace_mode(self):
        ctl = generate_ctl_content(
            table_name="T", schema="S",
            columns=["X"], spark_types=["string"],
            load_mode="REPLACE",
        )
        assert "REPLACE" in ctl

    def test_empty_schema(self):
        ctl = generate_ctl_content(
            table_name="MY_TABLE",
            schema="",
            columns=["X"], spark_types=["string"],
        )
        assert "MY_TABLE" in ctl
        assert ".MY_TABLE" not in ctl  # No schema.prefix

    def test_discard_file(self):
        ctl = generate_ctl_content(
            table_name="T", schema="S",
            columns=["X"], spark_types=["string"],
            discard_file="/tmp/discard.dsc",
        )
        assert "DISCARDFILE" in ctl
        assert "/tmp/discard.dsc" in ctl

    def test_no_discard_file(self):
        ctl = generate_ctl_content(
            table_name="T", schema="S",
            columns=["X"], spark_types=["string"],
        )
        assert "DISCARDFILE" not in ctl

    def test_unknown_type_defaults_char(self):
        ctl = generate_ctl_content(
            table_name="T", schema="S",
            columns=["X"], spark_types=["unknown_type"],
        )
        assert "X CHAR" in ctl

    def test_decimal_with_precision(self):
        ctl = generate_ctl_content(
            table_name="T", schema="S",
            columns=["AMT"], spark_types=["decimal(18,4)"],
        )
        assert "AMT DECIMAL EXTERNAL" in ctl

    def test_integer_types(self):
        for t in ["int", "short", "byte", "bigint"]:
            ctl = generate_ctl_content(
                table_name="T", schema="S",
                columns=["C"], spark_types=[t],
            )
            assert "INTEGER EXTERNAL" in ctl

    def test_bad_file_in_output(self):
        ctl = generate_ctl_content(
            table_name="T", schema="S",
            columns=["X"], spark_types=["string"],
            bad_file="/custom/path.bad",
        )
        assert "/custom/path.bad" in ctl
