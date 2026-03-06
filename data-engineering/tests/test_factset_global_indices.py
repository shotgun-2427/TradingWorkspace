import pandas as pd
import pytest

from pipeline.common.enums import Environment


@pytest.fixture
def global_indices_class(monkeypatch):
    from pipeline.sources.factset_global_indices import (
        FactsetGlobalIndices,
        TimeSeriesApi,
    )

    def fake_get_time_series_data_for_list(self, ts_request):
        formulas = ts_request.data.formulas
        records = []
        for date_str in ["2024-01-05", "2024-01-08"]:
            rec = {
                "request_id": ts_request.data.ids[0],
                "date": date_str,
            }
            # price
            rec[formulas[0]] = 4500.25
            records.append(rec)

        class Wrapper:
            def get_response_200(self_inner):
                class Response:
                    def to_dict(self_innermost):
                        return {"data": records}

                return Response()

        return Wrapper()

    monkeypatch.setattr(TimeSeriesApi, "get_time_series_data_for_list", fake_get_time_series_data_for_list)
    return FactsetGlobalIndices


@pytest.fixture
def source(global_indices_class):
    return global_indices_class(environment=Environment.DEVELOPMENT, interval="1d")


# ---- Init -------------------------------------------------------------------

def test_init_missing_env(monkeypatch, global_indices_class):
    monkeypatch.delenv("FACTSET_USERNAME", raising=False)
    monkeypatch.delenv("FACTSET_API_KEY", raising=False)
    with pytest.raises(ValueError):
        global_indices_class(environment=Environment.DEVELOPMENT, interval="1d")


def test_init_interval_supported(source):
    assert source.interval == "1d"
    assert source.hawk_id_to_ticker_map == {101: "TICK1", 202: "TICK2"}


def test_init_invalid_interval(global_indices_class):
    with pytest.raises(ValueError):
        global_indices_class(environment=Environment.DEVELOPMENT, interval="1h")


# ---- Fetch ------------------------------------------------------------------

def test_fetch_builds_dataframe(source):
    df = source.fetch("2024-01-05", "2024-01-08", [101])
    expected_cols = ["ticker", "record_timestamp", "price", "hawk_id"]
    for col in expected_cols:
        assert col in df.columns
    assert set(df["hawk_id"]) == {101}


# ---- Transform --------------------------------------------------------------

def test_transform_types(source):
    raw = source.fetch("2024-01-05", "2024-01-08", [101])
    transformed = source.transform(raw)
    assert len(transformed) == 2
    # Check dtypes
    assert str(transformed.dtypes["hawk_id"]) == "Int64"
    assert str(transformed.dtypes["price"]) == "Float64"


def test_transform_empty_after_dropna(source):
    raw = pd.DataFrame(
        {
            "ticker": ["TICK1"],
            "record_timestamp": ["2024-01-05"],
            "price": [None],
            "hawk_id": [101],
        }
    )
    transformed = source.transform(raw)
    assert transformed.empty


def test_transform_keeps_complete_rows(source):
    raw = pd.DataFrame(
        {
            "ticker": ["TICK1", "TICK1"],
            "record_timestamp": ["2024-01-05", "2024-01-08"],
            "price": [4500.25, None],
            "hawk_id": [101, 101],
        }
    )
    transformed = source.transform(raw)
    # Only first row should remain (second has None in price)
    assert len(transformed) == 1
    assert transformed["price"].iloc[0] == 4500.25


# ---- Validate ---------------------------------------------------------------

def test_validate_success(source):
    raw = source.fetch("2024-01-05", "2024-01-08", [101])
    transformed = source.transform(raw)
    errors = source.validate(transformed)
    assert errors == []


def test_validate_empty(source):
    errors = source.validate(pd.DataFrame())
    assert errors == ["Dataset is empty"]


def test_validate_missing_columns(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101],
            "record_timestamp": [pd.Timestamp("2024-01-05")],
            # Missing: price
        }
    )
    errors = source.validate(df)
    assert any("Missing columns" in e for e in errors)


def test_validate_duplicates(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101, 101],
            "record_timestamp": [pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-05")],
            "price": [4500.25, 4510.50],
        }
    )
    errors = source.validate(df)
    assert any("Duplicate" in e for e in errors)


def test_validate_nan_in_price(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101],
            "record_timestamp": [pd.Timestamp("2024-01-05")],
            "price": [None],
        }
    )
    errors = source.validate(df)
    assert any("NaN" in e for e in errors)


def test_validate_zero_price(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101],
            "record_timestamp": [pd.Timestamp("2024-01-05")],
            "price": [0.0],
        }
    )
    errors = source.validate(df)
    assert any("zero or negative" in e for e in errors)


def test_validate_negative_price(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101],
            "record_timestamp": [pd.Timestamp("2024-01-05")],
            "price": [-100.0],
        }
    )
    errors = source.validate(df)
    assert any("zero or negative" in e for e in errors)


def test_validate_multiple_errors(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101, 101, 202],
            "record_timestamp": [
                pd.Timestamp("2024-01-05"),
                pd.Timestamp("2024-01-05"),
                pd.Timestamp("2024-01-08"),
            ],
            "price": [4500.25, -50.0, None],
        }
    )
    errors = source.validate(df)
    # Should catch duplicates, negative price, and NaN
    assert any("Duplicate" in e for e in errors)
    assert any("zero or negative" in e for e in errors)
    assert any("NaN" in e for e in errors)
