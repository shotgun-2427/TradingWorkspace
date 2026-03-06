import pandas as pd
import pytest

from pipeline.common.enums import Environment


@pytest.fixture
def snapshot_class(monkeypatch):
    from pipeline.sources.factset_equities_ohlcv_snapshot import (
        FactsetEquitiesOhlcvSnapshotSource,
        TimeSeriesApi,
    )

    def fake_get_time_series_data_for_list(self, ts_request):
        formulas = ts_request.data.formulas
        # Single snapshot row
        record = {
            "request_id": ts_request.data.ids[0],
            "date": "2024-01-05",
        }
        # open, high, low, close, volume
        record[formulas[0]] = 10.0
        record[formulas[1]] = 12.0
        record[formulas[2]] = 9.0
        record[formulas[3]] = 11.0
        record[formulas[4]] = 1000.0

        class Wrapper:
            def get_response_200(self_inner):
                class Response:
                    def to_dict(self_innermost):
                        return {"data": [record]}

                return Response()

        return Wrapper()

    monkeypatch.setattr(TimeSeriesApi, "get_time_series_data_for_list", fake_get_time_series_data_for_list)
    return FactsetEquitiesOhlcvSnapshotSource


@pytest.fixture
def source(snapshot_class):
    return snapshot_class(environment=Environment.DEVELOPMENT)


# ---- Init -------------------------------------------------------------------

def test_snapshot_init_missing_env(monkeypatch, snapshot_class):
    monkeypatch.delenv("FACTSET_USERNAME", raising=False)
    monkeypatch.delenv("FACTSET_API_KEY", raising=False)
    with pytest.raises(ValueError):
        snapshot_class(environment=Environment.DEVELOPMENT)


def test_snapshot_init(source):
    # BigQuery mapping from conftest patched client
    assert source.hawk_id_to_ticker_map == {101: "TICK1", 202: "TICK2"}


# ---- Fetch ------------------------------------------------------------------

def test_snapshot_fetch_builds_dataframe(source):
    df = source.fetch([101])
    for col in ["ticker", "record_timestamp", "open", "high", "low", "close", "volume", "hawk_id"]:
        assert col in df.columns
    assert set(df["hawk_id"]) == {101}


# ---- Transform --------------------------------------------------------------

def test_snapshot_transform_types(source):
    raw = source.fetch([101])
    transformed = source.transform(raw)
    assert len(transformed) == 1
    # Dtypes
    assert str(transformed.dtypes["hawk_id"]) == "Int64"
    assert str(transformed.dtypes["open"]) in {"Float64", "float64"}
    assert str(transformed.dtypes["volume"]) == "Int64"


def test_snapshot_transform_empty_after_dropna(source):
    raw = pd.DataFrame(
        {
            "ticker": ["TICK1"],
            "record_timestamp": ["2024-01-05"],
            "open": [None],
            "high": [None],
            "low": [None],
            "close": [None],
            "volume": [None],
            "hawk_id": [101],
        }
    )
    transformed = source.transform(raw)
    assert transformed.empty


# ---- Validate ---------------------------------------------------------------

def test_snapshot_validate_success(source):
    raw = source.fetch([101])
    transformed = source.transform(raw)
    assert source.validate(transformed) == []


def test_snapshot_validate_empty(source):
    assert source.validate(pd.DataFrame()) == ["Dataset is empty"]


def test_snapshot_validate_anomalies(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101, 101],
            "record_timestamp": [pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-05")],
            "open": [10.0, 10.0],
            "high": [8.0, 10.0],  # first row high < open/low/close
            "low": [9.0, 11.0],  # first row low > high; second row low > open/close
            "close": [9.5, 10.5],
            "volume": [100, -5],  # negative volume
        }
    )
    errors = source.validate(df)
    assert any("high < low" in e for e in errors)
    assert any("high < open" in e for e in errors)
    assert any("low > open" in e for e in errors)
    assert any("low > close" in e for e in errors)
    assert any("negative volume" in e for e in errors)
    assert any("duplicate" in e.lower() for e in errors)
