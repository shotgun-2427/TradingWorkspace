# tests/test_utils.py
import yaml
import pandas as pd
import pytest
from pathlib import Path

from pipeline.common.enums import Environment
from pipeline.common.utils import read_gcp_config, normalize_data
from pipeline.common.models import GCPConfig


def test_read_gcp_config(monkeypatch, tmp_path):
    # Create fake YAML configs
    prod_cfg = {"project": "prod_proj", "dataset": "prod_ds"}
    dev_cfg = {"project": "dev_proj", "dataset": "dev_ds"}

    prod_file = tmp_path / "prod.yaml"
    dev_file = tmp_path / "dev.yaml"
    prod_file.write_text(yaml.dump(prod_cfg))
    dev_file.write_text(yaml.dump(dev_cfg))

    # Patch the file paths inside utils module
    import pipeline.common.utils as utils_mod
    monkeypatch.setattr(utils_mod, "production_gcp_config", str(prod_file))
    monkeypatch.setattr(utils_mod, "development_gcp_config", str(dev_file))

    prod = read_gcp_config(Environment.PRODUCTION)
    dev = read_gcp_config(Environment.DEVELOPMENT)

    assert isinstance(prod, GCPConfig)
    assert prod.project == "prod_proj" and prod.dataset == "prod_ds"
    assert dev.project == "dev_proj" and dev.dataset == "dev_ds"


def test_read_gcp_config_invalid_env():
    with pytest.raises(Exception):
        read_gcp_config("STAGING")  # not a supported Environment


def test_normalize_data():
    # fields_mapping maps internal column names -> (field_id, field_type)
    fields_mapping = {
        "open_mapped": (1, "double"),
        "volume_mapped": (2, "int"),
        "char_field_mapped": (3, "char"),
    }
    # column_mapping maps user columns -> internal names
    column_mapping = {
        "open": "open_mapped",
        "volume": "volume_mapped",
        "char_field": "char_field_mapped",
    }

    df = pd.DataFrame(
        {
            "record_timestamp": [pd.Timestamp("2024-01-01")],
            "hawk_id": [101],
            "open": [10.5],
            "volume": [123],
            "char_field": ["abc"],
        }
    )

    out = normalize_data(df, fields_mapping=fields_mapping, column_mapping=column_mapping)

    # Expected columns
    assert list(out.columns) == [
        "hawk_id",
        "record_timestamp",
        "field_id",
        "char_value",
        "int_value",
        "double_value",
    ]
    # Three rows (one per original field)
    assert len(out) == 3

    # Check each field_id and value landed in correct typed column
    row_open = out[out.field_id == 1].iloc[0]
    assert row_open.double_value == 10.5 and pd.isna(row_open.int_value) and pd.isna(row_open.char_value)

    row_volume = out[out.field_id == 2].iloc[0]
    assert row_volume.int_value == 123 and pd.isna(row_volume.double_value) and pd.isna(row_volume.char_value)

    row_char = out[out.field_id == 3].iloc[0]
    assert row_char.char_value == "abc" and pd.isna(row_char.double_value) and pd.isna(row_char.int_value)
