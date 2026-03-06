"""
@description: Utility functions used throughout pipeline.
@author: Rithwik Babu
"""
import yaml
from pandas import DataFrame

from pipeline.common.enums import Environment
from pipeline.common.gData import production_gcp_config, development_gcp_config
from pipeline.common.models import GCPConfig


def read_gcp_config(environment: Environment) -> GCPConfig:
    """Load a GCP config given an environment."""
    if environment == Environment.PRODUCTION:
        yaml_config = production_gcp_config
    elif environment == Environment.DEVELOPMENT:
        yaml_config = development_gcp_config
    else:
        raise Exception(f"Environment not supported! {environment}")

    with open(yaml_config) as fp:
        config_dict = yaml.load(fp, yaml.FullLoader)

    return GCPConfig(**config_dict)


def normalize_data(data: DataFrame, fields_mapping: dict, column_mapping: dict) -> DataFrame:
    """
    Expects a DataFrame with columns for each field and normalizes it into a format suitable for BigQuery.

    :param data: Final input data to be transformed.
    :param fields_mapping: Fields mapping generated from table.
    :param column_mapping: Mapping of column fields to BigQuery column names.
    :return: Normalized DataFrame with columns for each field type.
    """
    field_ids = {field_name: fields_mapping.get(mapped_name, (None, None))[0]
                 for field_name, mapped_name in column_mapping.items()}
    field_types = {field_name: fields_mapping.get(mapped_name, (None, None))[1]
                   for field_name, mapped_name in column_mapping.items()}

    melted = data.melt(
        id_vars=["record_timestamp", "hawk_id"],
        value_vars=list(column_mapping.keys()),
        var_name="field_name",
        value_name="value"
    )

    melted['field_id'] = melted['field_name'].map(field_ids)
    melted['char_value'] = None
    melted['int_value'] = None
    melted['double_value'] = None

    type_series = melted['field_name'].map(field_types)

    double_mask = type_series == 'double'
    int_mask = type_series == 'int'
    char_mask = type_series == 'char'

    melted.loc[double_mask, 'double_value'] = melted.loc[double_mask, 'value'].astype(float)
    melted.loc[int_mask, 'int_value'] = melted.loc[int_mask, 'value'].astype(int)
    melted.loc[char_mask, 'char_value'] = melted.loc[char_mask, 'value'].astype(str)

    melted = melted.drop(columns=['value', 'field_name'])

    return melted[['hawk_id', 'record_timestamp', 'field_id', 'char_value', 'int_value', 'double_value']]
