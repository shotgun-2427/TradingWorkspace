import importlib
import json

import pandas as pd
import pytest

from pipeline.common.enums import Environment
from pipeline.common.models import GCPConfig


# ---------- Fakes ----------
class FakeBigQueryClient:
    def __init__(self, config):
        self.config = config
        self.insert_records_called_with = None
        self.queries = []

    def execute_query(self, query: str):
        self.queries.append(query)
        # Return mock series metadata
        return pd.DataFrame({
            "series_id": ["WCESTUS1", "WCRNTUS2", "WCRFPUS2"],
            "name": ["U.S. Crude Oil Stocks", "U.S. Net Imports", "U.S. Production"],
            "description": ["Weekly stocks", "Weekly imports", "Weekly production"],
            "frequency": ["weekly", "weekly", "weekly"],
            "unit": ["Thousand Barrels", "Thousand Barrels per Day", "Thousand Barrels per Day"]
        })

    def insert_records(self, data, table):
        self.insert_records_called_with = (data.copy(), table)


class FakeLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(("info", msg))

    def warning(self, msg):
        self.messages.append(("warning", msg))

    def error(self, msg):
        self.messages.append(("error", msg))


def fake_read_gcp_config(env):
    return GCPConfig(project="proj", dataset="ds")


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def json(self):
        return self._data

    def raise_for_status(self):
        if self.status_code != 200:
            raise Exception(f"HTTP Error: {self.status_code}")


# ---------- Patch BaseMacroSource dependencies ----------
@pytest.fixture(autouse=True)
def patch_base_macro_source(monkeypatch):
    base_mod = importlib.import_module("pipeline.base_macro_source")
    monkeypatch.setattr(base_mod, "read_gcp_config", fake_read_gcp_config)
    monkeypatch.setattr(base_mod, "BigQueryClient", FakeBigQueryClient)
    fake_logger = FakeLogger()
    monkeypatch.setattr(base_mod, "logger", fake_logger)
    return fake_logger


@pytest.fixture(autouse=True)
def eia_env(monkeypatch):
    monkeypatch.setenv("EIA_API_KEY", "test_api_key")


@pytest.fixture
def mock_eia_response():
    """Mock EIA API response for a single series."""
    return {
        "response": {
            "data": [
                {"period": "2024-01-05", "value": 420000, "units": "Thousand Barrels"},
                {"period": "2024-01-12", "value": 418500, "units": "Thousand Barrels"},
            ]
        }
    }


@pytest.fixture
def eia_petroleum_class(monkeypatch, mock_eia_response):
    from pipeline.sources.eia_petroleum import EIAPetroleum, requests
    
    def fake_get(url, params=None):
        return FakeResponse(mock_eia_response)
    
    monkeypatch.setattr(requests, "get", fake_get)
    return EIAPetroleum


@pytest.fixture
def source(eia_petroleum_class):
    return eia_petroleum_class(environment=Environment.DEVELOPMENT)


# ---- Init -------------------------------------------------------------------

def test_init_missing_env(monkeypatch, patch_base_macro_source):
    monkeypatch.delenv("EIA_API_KEY", raising=False)
    from pipeline.sources.eia_petroleum import EIAPetroleum
    with pytest.raises(ValueError):
        EIAPetroleum(environment=Environment.DEVELOPMENT)


def test_init_sets_source(source):
    assert source.source == "eia_petroleum"


def test_init_loads_api_key(source):
    assert source.api_key == "test_api_key"


# ---- Fetch ------------------------------------------------------------------

def test_fetch_returns_dataframe(source):
    df = source.fetch("2024-01-01", "2024-01-15", ["WCESTUS1"])
    assert isinstance(df, pd.DataFrame)
    assert "series_id" in df.columns
    assert "period" in df.columns
    assert "value" in df.columns


def test_fetch_all_series(source):
    df = source.fetch("2024-01-01", "2024-01-15")
    # Should fetch all 3 series
    assert len(df["series_id"].unique()) == 3


def test_fetch_filters_series(source):
    df = source.fetch("2024-01-01", "2024-01-15", ["WCESTUS1"])
    assert set(df["series_id"].unique()) == {"WCESTUS1"}


def test_fetch_unknown_series_skipped(source, patch_base_macro_source):
    df = source.fetch("2024-01-01", "2024-01-15", ["UNKNOWN_SERIES"])
    assert df.empty


# ---- Transform --------------------------------------------------------------

def test_transform_adds_source_column(source):
    raw = source.fetch("2024-01-01", "2024-01-15", ["WCESTUS1"])
    transformed = source.transform(raw)
    assert "source" in transformed.columns
    assert all(transformed["source"] == "eia_petroleum")


def test_transform_converts_timestamp(source):
    raw = source.fetch("2024-01-01", "2024-01-15", ["WCESTUS1"])
    transformed = source.transform(raw)
    assert transformed["record_timestamp"].dtype == "datetime64[ns]"


def test_transform_converts_value_to_float(source):
    raw = source.fetch("2024-01-01", "2024-01-15", ["WCESTUS1"])
    transformed = source.transform(raw)
    assert str(transformed["value"].dtype) == "Float64"


def test_transform_drops_null_values(source, monkeypatch):
    from pipeline.sources.eia_petroleum import EIAPetroleum, requests
    
    response_with_null = {
        "response": {
            "data": [
                {"period": "2024-01-05", "value": 420000, "units": "Thousand Barrels"},
                {"period": "2024-01-12", "value": None, "units": "Thousand Barrels"},
            ]
        }
    }
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: FakeResponse(response_with_null))
    
    source_with_nulls = EIAPetroleum(environment=Environment.DEVELOPMENT)
    raw = source_with_nulls.fetch("2024-01-01", "2024-01-15", ["WCESTUS1"])
    transformed = source_with_nulls.transform(raw)
    
    # Should only have 1 row (the null value row is dropped)
    assert len(transformed) == 1


def test_transform_empty_input(source):
    empty_df = pd.DataFrame(columns=["series_id", "period", "value", "unit"])
    transformed = source.transform(empty_df)
    assert transformed.empty


# ---- Validate ---------------------------------------------------------------

def test_validate_success(source):
    raw = source.fetch("2024-01-01", "2024-01-15", ["WCESTUS1"])
    transformed = source.transform(raw)
    errors = source.validate(transformed)
    assert errors == []


def test_validate_empty_dataset(source):
    empty_df = pd.DataFrame(columns=["source", "series_id", "record_timestamp", "value"])
    errors = source.validate(empty_df)
    assert "Dataset is empty" in errors


def test_validate_missing_columns(source):
    df = pd.DataFrame({
        "source": ["eia_petroleum"],
        "series_id": ["WCESTUS1"],
        # Missing: record_timestamp, value
    })
    errors = source.validate(df)
    assert any("Missing columns" in e for e in errors)


def test_validate_duplicates(source):
    df = pd.DataFrame({
        "source": ["eia_petroleum", "eia_petroleum"],
        "series_id": ["WCESTUS1", "WCESTUS1"],
        "record_timestamp": [pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-05")],
        "value": [420000.0, 421000.0],
    })
    errors = source.validate(df)
    assert any("duplicate" in e.lower() for e in errors)


def test_validate_nan_values(source):
    df = pd.DataFrame({
        "source": ["eia_petroleum"],
        "series_id": ["WCESTUS1"],
        "record_timestamp": [pd.Timestamp("2024-01-05")],
        "value": [None],
    })
    errors = source.validate(df)
    assert any("NaN" in e for e in errors)


def test_validate_unknown_series(source):
    df = pd.DataFrame({
        "source": ["eia_petroleum"],
        "series_id": ["UNKNOWN_SERIES"],
        "record_timestamp": [pd.Timestamp("2024-01-05")],
        "value": [100.0],
    })
    errors = source.validate(df)
    assert any("Unknown series_ids" in e for e in errors)


# ---- Normalize --------------------------------------------------------------

def test_normalize_adds_char_value_column(source):
    raw = source.fetch("2024-01-01", "2024-01-15", ["WCESTUS1"])
    transformed = source.transform(raw)
    normalized = source.normalize(transformed)
    assert "char_value" in normalized.columns


def test_normalize_column_order(source):
    raw = source.fetch("2024-01-01", "2024-01-15", ["WCESTUS1"])
    transformed = source.transform(raw)
    normalized = source.normalize(transformed)
    expected_cols = ["source", "series_id", "record_timestamp", "value", "char_value"]
    assert list(normalized.columns) == expected_cols


def test_normalize_missing_required_columns(source):
    df = pd.DataFrame({"source": ["eia_petroleum"]})
    with pytest.raises(ValueError, match="Missing required columns"):
        source.normalize(df)
