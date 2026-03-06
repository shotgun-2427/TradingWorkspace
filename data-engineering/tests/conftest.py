import importlib

import pandas as pd
import pytest

from pipeline.common.models import GCPConfig


# ---------- Fakes ----------
class FakeBigQueryClient:
    def __init__(self, config):
        self.config = config
        self.load_fields_called = False
        self.insert_records_called_with = None
        self.queries = []

    def load_fields(self):
        self.load_fields_called = True
        return {"dummy": "mapping"}

    def execute_query(self, query: str):
        # Provide hawk_id -> ticker mapping
        self.queries.append(query)
        return pd.DataFrame({"hawk_id": [101, 202], "value": ["TICK1", "TICK2"]})

    def insert_records(self, data, table):
        self.insert_records_called_with = (data.copy(), table)


class FakeLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(msg)


def fake_read_gcp_config(env):
    return GCPConfig(project="proj", dataset="ds")


def fake_normalize_data(data, fields_mapping, column_mapping):
    return data  # passthrough


# ---------- Patch BaseSource dependencies ----------
@pytest.fixture(autouse=True)
def patch_base_source(monkeypatch):
    base_mod = importlib.import_module("pipeline.base_source")
    monkeypatch.setattr(base_mod, "read_gcp_config", fake_read_gcp_config)
    monkeypatch.setattr(base_mod, "normalize_data", fake_normalize_data)
    monkeypatch.setattr(base_mod, "BigQueryClient", FakeBigQueryClient)
    fake_logger = FakeLogger()
    monkeypatch.setattr(base_mod, "logger", fake_logger)
    return fake_logger


# ---------- Environment for FactSet ----------
@pytest.fixture(autouse=True)
def factset_env(monkeypatch):
    monkeypatch.setenv("FACTSET_USERNAME", "user")
    monkeypatch.setenv("FACTSET_API_KEY", "apikey")
