import pandas as pd
import pytest

from pipeline.base_source import BaseSource
from pipeline.common.enums import Environment, WriteMode


class DummySource(BaseSource):
    def fetch(self, *_, **__): return pd.DataFrame()

    def transform(self, *_, **__): return pd.DataFrame()

    def validate(self, *_, **__): return []


@pytest.fixture
def source():
    return DummySource(environment=Environment.DEVELOPMENT, source_type="dummy")


def test_init_loads_fields_and_tickers(source):
    assert source.bq_client.load_fields_called
    assert len(source.bq_client.queries) == 1
    assert "TICKER" in source.bq_client.queries[0]
    assert source.hawk_id_to_ticker_map == {101: "TICK1", 202: "TICK2"}


def test_load_hawk_figi(source):
    mapping = source._load_hawk_figi()
    assert mapping == {101: "TICK1", 202: "TICK2"}
    assert "FIGI" in source.bq_client.queries[-1]


def test_get_ticker_success(source):
    assert source._get_ticker(101) == "TICK1"


def test_get_ticker_missing(source):
    with pytest.raises(ValueError):
        source._get_ticker(999)


def test_normalize_enforces_dtypes(source):
    df = pd.DataFrame(
        {
            "hawk_id": [1],
            "record_timestamp": [pd.Timestamp("2024-01-01")],
            "field_id": [5],
            "char_value": ["abc"],
            "int_value": [7],
            "double_value": [1.5],
        }
    )
    out = source.normalize(df)
    assert str(out.dtypes["hawk_id"]) == "Int64"
    assert str(out.dtypes["field_id"]) == "Int64"
    assert str(out.dtypes["int_value"]) == "Int64"
    assert str(out.dtypes["char_value"]) == "string"
    assert str(out.dtypes["double_value"]) in {"Float64", "float64"}
    assert pd.api.types.is_datetime64_ns_dtype(out["record_timestamp"])


def test_write_bigquery(source, patch_base_source):
    df = pd.DataFrame(
        {
            "hawk_id": pd.Series([1], dtype="Int64"),
            "record_timestamp": [pd.Timestamp("2024-01-01")],
            "field_id": pd.Series([5], dtype="Int64"),
            "char_value": pd.Series(["a"], dtype="string"),
            "int_value": pd.Series([1], dtype="Int64"),
            "double_value": pd.Series([1.0], dtype="Float64"),
        }
    )
    source.write(df, WriteMode.BIGQUERY)
    inserted, table = source.bq_client.insert_records_called_with
    assert not inserted.empty
    assert table
    assert any("BigQuery" in m for m in patch_base_source.messages)


def test_write_csv(tmp_path, source, patch_base_source):
    df = pd.DataFrame(
        {
            "hawk_id": pd.Series([1], dtype="Int64"),
            "record_timestamp": [pd.Timestamp("2024-01-01")],
            "field_id": pd.Series([5], dtype="Int64"),
            "char_value": pd.Series(["a"], dtype="string"),
            "int_value": pd.Series([1], dtype="Int64"),
            "double_value": pd.Series([1.0], dtype="Float64"),
        }
    )
    out_path = tmp_path / "out.csv"
    source.write(df, WriteMode.CSV, output_path=str(out_path))
    assert out_path.exists()
    assert any("written to" in m for m in patch_base_source.messages)


def test_write_log(capsys, source, patch_base_source):
    df = pd.DataFrame(
        {
            "hawk_id": pd.Series([1], dtype="Int64"),
            "record_timestamp": [pd.Timestamp("2024-01-01")],
            "field_id": pd.Series([5], dtype="Int64"),
            "char_value": pd.Series(["a"], dtype="string"),
            "int_value": pd.Series([1], dtype="Int64"),
            "double_value": pd.Series([1.0], dtype="Float64"),
        }
    )
    source.write(df, WriteMode.LOG)
    captured = capsys.readouterr()
    assert "hawk_id" in captured.out
    assert any("written to log" in m for m in patch_base_source.messages)


def test_write_invalid_mode(source):
    with pytest.raises(ValueError):
        source.write(pd.DataFrame(), object())
