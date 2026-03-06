from datetime import datetime

import pandas as pd
import pytest

from pipeline.common.enums import Environment


@pytest.fixture
def factset_class(monkeypatch):
    from pipeline.sources.factset_equities_ohlcv import (
        FactsetEquitiesOhlcvSource,
        TimeSeriesApi,
    )

    def fake_get_time_series_data_for_list(self, ts_request):
        formulas = ts_request.data.formulas
        # Jan 5 (Fri) and Jan 6 (Sat) to test weekend filtering
        records = []
        for date_str in ["2024-01-05", "2024-01-06"]:
            rec = {"request_id": ts_request.data.ids[0], "date": date_str}
            rec[formulas[0]] = 10.0
            rec[formulas[1]] = 12.0
            rec[formulas[2]] = 9.0
            rec[formulas[3]] = 11.0
            rec[formulas[4]] = 1000.0
            records.append(rec)

        class Wrapper:
            def get_response_200(self_inner):
                class Response:
                    def to_dict(self_innermost):
                        return {"data": records}

                return Response()

        return Wrapper()

    monkeypatch.setattr(TimeSeriesApi, "get_time_series_data_for_list", fake_get_time_series_data_for_list)
    return FactsetEquitiesOhlcvSource


@pytest.fixture
def source(factset_class):
    return factset_class(environment=Environment.DEVELOPMENT, interval="1d")


def test_init_missing_env(monkeypatch, factset_class):
    monkeypatch.delenv("FACTSET_USERNAME", raising=False)
    monkeypatch.delenv("FACTSET_API_KEY", raising=False)
    with pytest.raises(ValueError):
        factset_class(environment=Environment.DEVELOPMENT, interval="1d")


def test_interval_supported(source):
    assert source.interval == "1d"
    assert source.hawk_id_to_ticker_map == {101: "TICK1", 202: "TICK2"}


def test_fetch_invalid_interval(factset_class):
    with pytest.raises(ValueError):
        factset_class(environment=Environment.DEVELOPMENT, interval="1h")


def test_fetch_builds_dataframe(source):
    df = source.fetch("2024-01-05", "2024-01-06", [101])
    for col in ["ticker", "record_timestamp", "open", "high", "low", "close", "volume", "hawk_id"]:
        assert col in df.columns
    assert set(df["hawk_id"]) == {101}


def test_transform_filters_weekends(source):
    raw = source.fetch("2024-01-05", "2024-01-06", [101])
    transformed = source.transform(raw)
    assert len(transformed) == 1
    assert pd.Timestamp(transformed["record_timestamp"].iloc[0]).date() == datetime(2024, 1, 5).date()
    assert str(transformed.dtypes["volume"]) == "Int64"


def test_transform_empty_after_dropna(source):
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


def test_validate_success(source):
    raw = source.fetch("2024-01-05", "2024-01-06", [101])
    transformed = source.transform(raw)
    assert source.validate(transformed) == []


def test_validate_empty(source):
    assert source.validate(pd.DataFrame()) == ["Dataset is empty"]


def test_validate_anomalies(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101, 101],
            "record_timestamp": [pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-05")],
            "open": [10.0, 10.0],
            "high": [8.0, 10.0],
            "low": [9.0, 11.0],
            "close": [9.5, 10.5],
            "volume": [100, -5],
        }
    )
    errors = source.validate(df)
    assert any("high < low" in e for e in errors)
    assert any("high < open" in e for e in errors)
    assert any("low > open" in e for e in errors)
    assert any("low > close" in e for e in errors)
    assert any("negative volume" in e for e in errors)
    assert any("duplicate" in e.lower() for e in errors)
