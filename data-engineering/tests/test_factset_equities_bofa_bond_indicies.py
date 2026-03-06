import pandas as pd
import pytest

from pipeline.common.enums import Environment


@pytest.fixture
def bofa_class(monkeypatch):
    from pipeline.sources.factset_equities_bofa_bond_indicies import (
        FactsetEquitiesBofABond,
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
            # total_return, oas, duration_modified, duration_effective, convexity
            rec[formulas[0]] = 1.25
            rec[formulas[1]] = 150.0
            rec[formulas[2]] = 5.5
            rec[formulas[3]] = 5.3
            rec[formulas[4]] = 0.45
            records.append(rec)

        class Wrapper:
            def get_response_200(self_inner):
                class Response:
                    def to_dict(self_innermost):
                        return {"data": records}

                return Response()

        return Wrapper()

    monkeypatch.setattr(TimeSeriesApi, "get_time_series_data_for_list", fake_get_time_series_data_for_list)
    return FactsetEquitiesBofABond


@pytest.fixture
def source(bofa_class):
    return bofa_class(environment=Environment.DEVELOPMENT, interval="1d")


# ---- Init -------------------------------------------------------------------

def test_init_missing_env(monkeypatch, bofa_class):
    monkeypatch.delenv("FACTSET_USERNAME", raising=False)
    monkeypatch.delenv("FACTSET_API_KEY", raising=False)
    with pytest.raises(ValueError):
        bofa_class(environment=Environment.DEVELOPMENT, interval="1d")


def test_init_interval_supported(source):
    assert source.interval == "1d"
    assert source.hawk_id_to_ticker_map == {101: "TICK1", 202: "TICK2"}


def test_init_invalid_interval(bofa_class):
    with pytest.raises(ValueError):
        bofa_class(environment=Environment.DEVELOPMENT, interval="1h")


# ---- Fetch ------------------------------------------------------------------

def test_fetch_builds_dataframe(source):
    df = source.fetch("2024-01-05", "2024-01-08", [101])
    expected_cols = [
        "ticker",
        "record_timestamp",
        "total_return",
        "oas",
        "duration_modified",
        "duration_effective",
        "convexity",
        "hawk_id",
    ]
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
    assert str(transformed.dtypes["total_return"]) == "Float64"
    assert str(transformed.dtypes["oas"]) == "Float64"
    assert str(transformed.dtypes["duration_modified"]) == "Float64"
    assert str(transformed.dtypes["duration_effective"]) == "Float64"
    assert str(transformed.dtypes["convexity"]) == "Float64"


def test_transform_empty_after_dropna(source):
    raw = pd.DataFrame(
        {
            "ticker": ["TICK1"],
            "record_timestamp": ["2024-01-05"],
            "total_return": [None],
            "oas": [None],
            "duration_modified": [None],
            "duration_effective": [None],
            "convexity": [None],
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
            "total_return": [1.25, None],
            "oas": [150.0, 160.0],
            "duration_modified": [5.5, 5.6],
            "duration_effective": [5.3, 5.4],
            "convexity": [0.45, 0.46],
            "hawk_id": [101, 101],
        }
    )
    transformed = source.transform(raw)
    # Only first row should remain (second has None in total_return)
    assert len(transformed) == 1
    assert transformed["total_return"].iloc[0] == 1.25


# ---- Validate ---------------------------------------------------------------

def test_validate_success(source):
    raw = source.fetch("2024-01-05", "2024-01-08", [101])
    transformed = source.transform(raw)
    errors = source.validate(transformed)
    assert errors == []


def test_validate_empty(source):
    errors = source.validate(pd.DataFrame())
    assert errors == ["Dataset is empty"]


def test_validate_duplicates(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101, 101],
            "record_timestamp": [pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-05")],
            "total_return": [1.25, 1.30],
            "oas": [150.0, 155.0],
            "duration_modified": [5.5, 5.6],
            "duration_effective": [5.3, 5.4],
            "convexity": [0.45, 0.46],
        }
    )
    errors = source.validate(df)
    assert any("Duplicate" in e for e in errors)


def test_validate_nan_in_metrics(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101],
            "record_timestamp": [pd.Timestamp("2024-01-05")],
            "total_return": [None],
            "oas": [150.0],
            "duration_modified": [5.5],
            "duration_effective": [5.3],
            "convexity": [0.45],
        }
    )
    errors = source.validate(df)
    assert any("NaNs found in total_return" in e for e in errors)


def test_validate_nan_in_multiple_metrics(source):
    df = pd.DataFrame(
        {
            "hawk_id": [101],
            "record_timestamp": [pd.Timestamp("2024-01-05")],
            "total_return": [1.25],
            "oas": [None],
            "duration_modified": [None],
            "duration_effective": [5.3],
            "convexity": [0.45],
        }
    )
    errors = source.validate(df)
    assert any("NaNs found in oas" in e for e in errors)
    assert any("NaNs found in duration_modified" in e for e in errors)
