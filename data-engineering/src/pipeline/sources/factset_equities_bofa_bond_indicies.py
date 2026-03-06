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


class FactsetEquitiesBofABond(BaseSource):
    def __init__(self, environment: Environment, interval: str):
        super().__init__(
            environment=environment,
            source_type='bofa_bond_indices',
            column_mapping={
                "total_return": f"total_return_{interval}",
                "oas": f"oas_{interval}",
                "duration_modified": f"duration_modified_{interval}",
                "duration_effective": f"duration_effective_{interval}",
                "convexity": f"convexity_{interval}",
            }
        )
        self.interval = interval

        if self.interval != '1d':
            raise ValueError("This source only supports daily index series (1d).")

        self.username = os.environ.get('FACTSET_USERNAME')
        self.api_key = os.environ.get('FACTSET_API_KEY')

        if not self.username or not self.api_key:
            raise ValueError("FACTSET_USERNAME and FACTSET_API_KEY must be set in environment variables")

    def fetch(self, start_date: str, end_date: str, securities: list[int]) -> pd.DataFrame:
        """
        Pull BofA/ICE bond index series from FactSet TimeSeries API.

        :param start_date: YYYY-MM-DD
        :param end_date:   YYYY-MM-DD
        :param securities: list of hawk_ids mapped to index IDs (e.g., BofA index tickers)
        :return: DataFrame with hawk_id, record_timestamp, total_return, oas, duration_modified,
                 duration_effective, convexity
        """
        fs_ids = [self._get_ticker(hawk_id) for hawk_id in securities]

        # Convert to FactSet's M/D/YYYY style (no leading zeros)
        sd_dt = datetime.strptime(start_date, "%Y-%m-%d")
        ed_dt = datetime.strptime(end_date, "%Y-%m-%d")
        sd = f"{sd_dt.month}/{sd_dt.day}/{sd_dt.year}"
        ed = f"{ed_dt.month}/{ed_dt.day}/{ed_dt.year}"

        # Build TimeSeries formulas using the passed-in dates.
        # No spaces so column names match exactly what the API returns.
        formulas = [
            f'ML_TOT_RET({sd},{ed},D,"FIVEDAY","LOC","NO_TYPE","TC")',
            f'ML_OAS({sd},{ed},D,"FIVEDAYEOM","FINAL","CLOSE")',
            f'ML_DUR_MOD({sd},{ed},D,"FIVEDAY","FINAL")',
            f'ML_DUR_EFF({sd},{ed},D,"FIVEDAY","FINAL")',
            f'ML_CONVEX({sd},{ed},D,"FIVEDAY","FINAL")',
        ]
        field_names = [
            "total_return",
            "oas",
            "duration_modified",
            "duration_effective",
            "convexity",
        ]

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

        # attach hawk_id
        ticker_to_hawk = {v: k for k, v in self.hawk_id_to_ticker_map.items()}
        df["hawk_id"] = df["ticker"].map(ticker_to_hawk)

        return df

    def validate(self, data: DataFrame) -> List[str]:
        """
        Basic sanity checks for BofA bond index data.
        Returns a list of error messages (empty list == ok).
        """
        errors: list[str] = []

        if data.empty:
            errors.append("Dataset is empty")
            return errors

        required_cols = {
            "hawk_id",
            "record_timestamp",
            "total_return",
            "oas",
            "duration_modified",
            "duration_effective",
            "convexity",
        }
        missing = required_cols - set(data.columns)
        if missing:
            errors.append(f"Missing columns: {sorted(missing)}")

        # Duplicates in (hawk_id, record_timestamp)
        if data.duplicated(subset=["hawk_id", "record_timestamp"]).any():
            errors.append("Duplicate rows for (hawk_id, record_timestamp)")

        # NaN checks on metric columns
        for col in [
            "total_return",
            "oas",
            "duration_modified",
            "duration_effective",
            "convexity",
        ]:
            if data[col].isna().any():
                errors.append(f"NaNs found in {col}")

        return errors

    def transform(self, data: DataFrame) -> DataFrame:
        """
        Casts types and drops missing. No NYSE calendar filtering (these are index series).
        """
        cols = [
            "hawk_id",
            "record_timestamp",
            "total_return",
            "oas",
            "duration_modified",
            "duration_effective",
            "convexity",
        ]
        df = data.loc[:, cols].copy()

        df = df.astype({
            "hawk_id": Integer(),
            "record_timestamp": "datetime64[ns]",
            "total_return": Float(),
            "oas": Float(),
            "duration_modified": Float(),
            "duration_effective": Float(),
            "convexity": Float(),
        })

        df = df.dropna()
        return df
