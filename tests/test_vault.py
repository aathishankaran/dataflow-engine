"""Unit tests for dataflow_engine/vault.py — mocked Vault client."""
import pytest
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dataflow_engine.vault import _split_kv_path


class TestSplitKvPath:
    def test_kv_v2_path(self):
        mount, path = _split_kv_path("secret/data/oracle/prod")
        assert mount == "secret"
        assert path == "oracle/prod"

    def test_kv_v1_path(self):
        mount, path = _split_kv_path("secret/oracle/prod")
        assert mount == "secret"
        assert path == "oracle/prod"

    def test_custom_mount(self):
        mount, path = _split_kv_path("kv/data/mydb")
        assert mount == "kv"
        assert path == "mydb"

    def test_leading_slash(self):
        mount, path = _split_kv_path("/secret/data/oracle/prod")
        assert mount == "secret"
        assert path == "oracle/prod"

    def test_deep_path(self):
        mount, path = _split_kv_path("secret/data/team/project/oracle/prod")
        assert mount == "secret"
        assert path == "team/project/oracle/prod"
