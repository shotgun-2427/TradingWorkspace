"""
@description: Module to source Global Index price data from FactSet's Formula API
@author: Rithwik Babu
"""
import os
from datetime import datetime
from typing import List

import pandas as pd
from fds.sdk.Formula import Configuration, ApiClient
from fds.sdk.Formula.api.time_series_api import TimeSeriesApi
from fds.sdk.Formula.model.time_series_request import TimeSeriesRequest
from fds.sdk.Formula.model.time_series_request_data import TimeSeriesRequestData
from pandas import DataFrame, Int64Dtype as Integer, Float64Dtype as Float

from pipeline.base_source import BaseSource
from pipeline.common.enums import Environment


class FactsetGlobalIndices(BaseSource):
    """
    Pull FactSet Global Index price via the FactSet Formula API.
    Uses embedded dates in formulas to bypass Batch entitlement restrictions.

    Example formula: FG_PRICE(1/5/2026,1/16/2026,,USD)
    """

    def __init__(self, environment: Environment, interval: str):
        super().__init__(
            environment=environment,
            source_type='global_indices',
            column_mapping={
                "price": f"price_{interval}",
            }
        )
        self.interval = interval

        if self.interval != '1d':
            raise ValueError("FactsetGlobalIndices only supports daily data (1d).")

        self.username = os.environ.get('FACTSET_USERNAME')
        self.api_key = os.environ.get('FACTSET_API_KEY')

        if not self.username or not self.api_key:
            raise ValueError("FACTSET_USERNAME and FACTSET_API_KEY must be set in environment variables")

    def fetch(self, start_date: str, end_date: str, securities: list[int]) -> pd.DataFrame:
        """
        Fetch global index price data from FactSet for the given securities.

        :param start_date: Start date for the data (YYYY-MM-DD).
        :param end_date: End date for the data (YYYY-MM-DD).
        :param securities: List of hawk_ids for which to fetch data.
        :return: Raw data from FactSet
        """
        fs_ids = [self._get_ticker(hawk_id) for hawk_id in securities]

        # Convert to FactSet's M/D/YYYY style (no leading zeros)
        sd_dt = datetime.strptime(start_date, "%Y-%m-%d")
        ed_dt = datetime.strptime(end_date, "%Y-%m-%d")
        sd = f"{sd_dt.month}/{sd_dt.day}/{sd_dt.year}"
        ed = f"{ed_dt.month}/{ed_dt.day}/{ed_dt.year}"

        # Build TimeSeries formula for FG_PRICE
        # FG_PRICE(start_date, end_date, frequency, currency)
        formulas = [
            f'FG_PRICE({sd},{ed},D,USD)',
        ]
        field_names = ["price"]

        configuration = Configuration(verify_ssl=False)
        configuration.username = self.username
        configuration.password = self.api_key

        with ApiClient(configuration) as api_client:
            api = TimeSeriesApi(api_client)
            req = TimeSeriesRequest(
                data=TimeSeriesRequestData(
                    ids=fs_ids,
                    formulas=formulas,
                    flatten="Y",
                )
            )

            wrapper = api.get_time_series_data_for_list(req)
            resp = wrapper.get_response_200()
            data = resp.to_dict()["data"]
            df = pd.DataFrame(data)

        # Map columns from the formula strings to human names
        colmap = {"request_id": "ticker", "date": "record_timestamp"}
        for f, name in zip(formulas, field_names):
            colmap[f] = name
        df = df.rename(columns=colmap)

        # Attach hawk_id
        ticker_to_hawk = {v: k for k, v in self.hawk_id_to_ticker_map.items()}
        df["hawk_id"] = df["ticker"].map(ticker_to_hawk)

        return df

    def transform(self, data: DataFrame) -> DataFrame:
        """
        Transforms global index price data to ensure data integrity.

        :param data: Input DataFrame to be transformed.
        :return: Transformed DataFrame with price data.
        """
        cols = ["hawk_id", "record_timestamp", "price"]
        df = data.loc[:, cols].copy()

        df = df.astype({
            "hawk_id": Integer(),
            "record_timestamp": "datetime64[ns]",
            "price": Float(),
        })

        # Drop rows with missing values
        df = df.dropna()

        return df

    def validate(self, data: DataFrame) -> List[str]:
        """
        Validates the DataFrame containing global index price data.

        :param data: The price data to validate.
        :return: List of validation error messages. Empty list if no errors.
        """
        errors: List[str] = []

        if data.empty:
            errors.append("Dataset is empty")
            return errors

        required_cols = {"hawk_id", "record_timestamp", "price"}
        missing = required_cols - set(data.columns)
        if missing:
            errors.append(f"Missing columns: {sorted(missing)}")
            return errors

        # Check for duplicates in (hawk_id, record_timestamp)
        if data.duplicated(subset=["hawk_id", "record_timestamp"]).any():
            errors.append("Duplicate rows for (hawk_id, record_timestamp)")

        # Check for NaN values in price
        if data["price"].isna().any():
            nan_count = data["price"].isna().sum()
            errors.append(f"Found {nan_count} NaN values in price column")

        # Check for zero or negative prices
        non_positive = data[data["price"] <= 0]
        if not non_positive.empty:
            errors.append(f"Found {len(non_positive)} records with zero or negative prices")

        return errors