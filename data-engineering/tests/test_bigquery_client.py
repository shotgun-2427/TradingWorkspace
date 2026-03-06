import json
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from pipeline.common.bigquery_client import BigQueryClient


@dataclass
class DummyConfig:
    project: str = "test-project"
    dataset: str = "test_dataset"


@pytest.fixture
def config():
    return DummyConfig()


@pytest.fixture
def fake_client():
    """Return a MagicMock standing in for google.cloud.bigquery.Client."""
    c = MagicMock(name="BigQueryInnerClient")
    return c


def test_init_adc_success(monkeypatch, config, fake_client):
    """ADC path succeeds on first attempt."""
    monkeypatch.setenv("IRRELEVANT", "1")  # ensure other branches aren't taken
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("SERVICE_ACCOUNT_JSON", raising=False)

    # First call succeeds
    monkeypatch.setattr(
        "pipeline.common.bigquery_client.bigquery.Client",
        lambda project: fake_client,
    )

    client = BigQueryClient(config)
    assert client.client is fake_client


def test_init_fallback_to_google_application_credentials(monkeypatch, config, fake_client):
    """ADC fails, fallback to GOOGLE_APPLICATION_CREDENTIALS succeeds."""
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/fake.json")
    monkeypatch.delenv("SERVICE_ACCOUNT_JSON", raising=False)

    calls = []

    def client_factory(project):
        # First call: raise (ADC). Second call: succeed.
        if not calls:
            calls.append("first")
            raise RuntimeError("ADC failure")
        calls.append("second")
        return fake_client

    monkeypatch.setattr(
        "pipeline.common.bigquery_client.bigquery.Client",
        client_factory,
    )

    client = BigQueryClient(config)
    assert client.client is fake_client
    assert calls == ["first", "second"]


def test_init_fallback_to_service_account_json(monkeypatch, config, fake_client):
    """ADC fails, GOOGLE_APPLICATION_CREDENTIALS missing, SERVICE_ACCOUNT_JSON used."""
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    service_json = {"type": "service_account", "project_id": "x"}
    monkeypatch.setenv("SERVICE_ACCOUNT_JSON", json.dumps(service_json))

    # ADC fails first
    calls = []

    def client_factory(*args, **kwargs):
        if not calls:
            calls.append("adc")
            raise RuntimeError("ADC failure")
        calls.append("sa_json")
        return fake_client

    monkeypatch.setattr(
        "pipeline.common.bigquery_client.bigquery.Client",
        client_factory,
    )
    monkeypatch.setattr(
        "pipeline.common.bigquery_client.service_account.Credentials.from_service_account_info",
        lambda info: SimpleNamespace(name="creds", info=info),
    )

    client = BigQueryClient(config)
    assert client.client is fake_client
    assert calls == ["adc", "sa_json"]


def test_init_missing_credentials_raises(monkeypatch, config):
    """ADC fails and no env vars => raises ConnectionRefusedError."""
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("SERVICE_ACCOUNT_JSON", raising=False)

    def failing_client(*_args, **_kwargs):
        raise RuntimeError("ADC failure")

    monkeypatch.setattr(
        "pipeline.common.bigquery_client.bigquery.Client",
        failing_client,
    )

    with pytest.raises(ConnectionRefusedError):
        BigQueryClient(config)


def test_insert_records_success(monkeypatch, config, fake_client, capsys):
    client = BigQueryClient.__new__(BigQueryClient)  # bypass __init__
    client.config = config
    client.client = fake_client

    job = MagicMock()
    fake_client.load_table_from_dataframe.return_value = job

    df = pd.DataFrame({"a": [1, 2]})
    client.insert_records(df, "target_table")

    fake_client.load_table_from_dataframe.assert_called_once()
    job.result.assert_called_once()
    out = capsys.readouterr().out
    assert "Loaded 2 rows" in out


def test_insert_records_failure(monkeypatch, config, fake_client, capsys):
    client = BigQueryClient.__new__(BigQueryClient)
    client.config = config
    client.client = fake_client

    job = MagicMock()
    job.result.side_effect = RuntimeError("BQ failure")
    fake_client.load_table_from_dataframe.return_value = job

    df = pd.DataFrame({"a": [1]})
    client.insert_records(df, "target_table")

    out = capsys.readouterr().out
    assert "Failed to load data" in out


def test_execute_query_no_params(monkeypatch, config, fake_client):
    client = BigQueryClient.__new__(BigQueryClient)
    client.config = config
    client.client = fake_client

    # Mock query job chain
    query_job = MagicMock()
    result = MagicMock()
    result.to_dataframe.return_value = pd.DataFrame({"x": [1]})
    query_job.result.return_value = result
    fake_client.query.return_value = query_job

    df = client.execute_query("SELECT 1")
    fake_client.query.assert_called_once()
    assert list(df.columns) == ["x"]


def test_execute_query_with_params(monkeypatch, config, fake_client):
    client = BigQueryClient.__new__(BigQueryClient)
    client.config = config
    client.client = fake_client

    created_params = []

    class DummyParam:
        def __init__(self, name, typ, value):
            self.name = name
            self.type_ = typ
            self.value = value

    def param_factory(name, typ, value):
        p = DummyParam(name, typ, value)
        created_params.append(p)
        return p

    monkeypatch.setattr(
        "pipeline.common.bigquery_client.bigquery.ScalarQueryParameter",
        param_factory,
    )

    # Intercept QueryJobConfig to just store the params object
    class DummyJobConfig:
        def __init__(self, query_parameters):
            self.query_parameters = query_parameters

    monkeypatch.setattr(
        "pipeline.common.bigquery_client.QueryJobConfig",
        DummyJobConfig,
    )

    query_job = MagicMock()
    result = MagicMock()
    result.to_dataframe.return_value = pd.DataFrame({"y": [5]})
    query_job.result.return_value = result
    fake_client.query.return_value = query_job

    params = {"p1": ("abc", "STRING")}
    df = client.execute_query("SELECT @p1", params=params)

    assert df.iloc[0, 0] == 5
    # Ensure our param object was created correctly
    assert len(created_params) == 1
    assert created_params[0].name == "p1"
    assert created_params[0].type_ == "STRING"
    assert created_params[0].value == "abc"

    # Ensure job_config was passed with our params
    passed_job_config = fake_client.query.call_args.kwargs["job_config"]
    assert isinstance(passed_job_config, DummyJobConfig)
    assert passed_job_config.query_parameters[0] is created_params[0]


def test_load_fields(monkeypatch, config):
    client = BigQueryClient.__new__(BigQueryClient)
    client.config = config

    # Mock execute_query to return dataframe
    df = pd.DataFrame(
        {
            "field_id": [1, 2],
            "field_name": ["price", "volume"],
            "field_type": ["FLOAT", "INTEGER"],
        }
    )
    monkeypatch.setattr(client, "execute_query", lambda q: df)

    out = client.load_fields()
    assert out == {
        "price": (1, "FLOAT"),
        "volume": (2, "INTEGER"),
    }
    # Ensure keys are ints
    assert isinstance(out["price"][0], int)


def test_load_categorized_hawk_ids(monkeypatch, config):
    client = BigQueryClient.__new__(BigQueryClient)
    client.config = config

    df = pd.DataFrame(
        {
            "hawk_id": [9999, 10000, 15000, 20000, 25000, 30000, 31000, 32000],
            "asset_class": [
                "other",
                "equities",
                "equities",
                "futures",
                "futures",
                "ice_bofa_bond_indices",
                "global_indices",
                "unknown_class",
            ],
        }
    )
    monkeypatch.setattr(client, "execute_query", lambda q: df)

    out = client.load_categorized_hawk_ids()
    assert out == {
        "futures": [20000, 25000],
        "equities": [10000, 15000],
        "ice_bofa_bond_indices": [30000],
        "global_indices": [31000],
        "other": [9999, 32000],
    }
