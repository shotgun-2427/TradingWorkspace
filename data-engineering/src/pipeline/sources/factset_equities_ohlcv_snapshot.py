"""
@description: Module to source Equity OHLCV snapshot data from FactSet's Formula API
@author: Rithwik Babu
"""
import os

import pandas as pd
from fds.sdk.Formula import Configuration, ApiClient
from fds.sdk.Formula.api.time_series_api import TimeSeriesApi
from fds.sdk.Formula.model.time_series_request import TimeSeriesRequest
from fds.sdk.Formula.model.time_series_request_data import TimeSeriesRequestData
from pandas import DataFrame, Int64Dtype as Integer, Float64Dtype as Float

from pipeline.base_source import BaseSource
from pipeline.common.enums import Environment


class FactsetEquitiesOhlcvSnapshotSource(BaseSource):
    def __init__(self, environment: Environment):
        super().__init__(
            environment=environment,
            source_type='equities_ohlcv_snapshot',
            column_mapping={
                "open": "adjusted_open_snapshot",
                "high": "adjusted_high_snapshot",
                "low": "adjusted_low_snapshot",
                "close": "adjusted_close_snapshot",
                "volume": "volume_snapshot",
            }
        )

        self.username = os.environ.get('FACTSET_USERNAME')
        self.api_key = os.environ.get('FACTSET_API_KEY')

        if not self.username or not self.api_key:
            raise ValueError("FACTSET_USERNAME and FACTSET_API_KEY must be set in environment variables")

    def fetch(self, securities: list[int]) -> pd.DataFrame:
        """
        Fetch OHLCV snapshot data from FactSet for the given securities and process the results.

        :param securities: List of hawk_ids for which to fetch data.
        :return: Raw data from FactSet
        """
        # construct a list of tickers from securities
        futures_ids = [self._get_ticker(hawk_id) for hawk_id in securities]

        data_fields = {
            'open': 'P_PRICE_OPEN',
            'high': 'P_PRICE_HIGH',
            'low': 'P_PRICE_LOW',
            'close': 'P_PRICE',
            'volume': 'P_VOLUME',
        }

        # construct formulas for snapshot data
        formulas = [
            'P_PRICE_OPEN(NOW,NOW,,USD,,4)',
            'P_PRICE_HIGH(NOW,NOW,,,4,"PRICE","INTRA")',
            'P_PRICE_LOW(NOW,NOW,USD,,4,"PRICE","INTRA")',
            'P_PRICE(NOW,NOW,D,USD,,4)',
            'P_VOLUME(NOW,NOW,,,0)',
        ]

        # create factset sdk configuration object
        configuration = Configuration(verify_ssl=False)
        configuration.username = self.username
        configuration.password = self.api_key

        with ApiClient(configuration) as api_client:
            api_instance = TimeSeriesApi(api_client)

            # create request object
            time_series_request = TimeSeriesRequest(
                data=TimeSeriesRequestData(
                    ids=futures_ids,
                    formulas=formulas,
                    flatten="Y"
                ),
            )

            time_series_response_wrapper = api_instance.get_time_series_data_for_list(time_series_request)
            time_series_response = time_series_response_wrapper.get_response_200()
            time_series_data = time_series_response.to_dict()['data']
            time_series_df = pd.DataFrame(time_series_data)

            # create column mapping dynamically
            column_mapping = {'request_id': 'ticker', 'date': 'record_timestamp'}
            for formula, field_name in zip(formulas, data_fields.keys()):
                column_mapping[formula] = field_name

            time_series_df = time_series_df.rename(columns=column_mapping)

            # create hawk_id column
            ticker_to_hawk_id_map = {v: k for k, v in self.hawk_id_to_ticker_map.items()}
            time_series_df['hawk_id'] = time_series_df['ticker'].map(ticker_to_hawk_id_map)

            return time_series_df

    def transform(self, data: DataFrame) -> DataFrame:
        """Transforms OHLCV snapshot data to ensure data integrity.

        :param data: Final input DataFrame to be transformed.
        :return: Transformed DataFrame OHLCV snapshot data.
        """
        cols = ['hawk_id', 'record_timestamp', 'open', 'high', 'low', 'close', 'volume']
        df = data.loc[:, cols].copy()

        df = df.astype({
            'hawk_id': Integer(),
            'record_timestamp': 'datetime64[ns]',
            'open': Float(),
            'high': Float(),
            'low': Float(),
            'close': Float(),
            'volume': Integer(),
        })

        df = df.dropna()

        return df

    def validate(self, data: DataFrame) -> list[str]:
        """
        Validates the DataFrame containing OHLCV snapshot data and returns a list of validation errors.

        :param data: The OHLCV snapshot data to validate.
        :return: List of validation error messages. Empty list if no errors.
        """
        validation_errors = []

        # Check if dataframe is empty
        if data.empty:
            validation_errors.append("Dataset is empty")
            return validation_errors

        # Check for missing values
        missing_values = data.isnull().sum()
        if missing_values.sum() > 0:
            for column, count in missing_values.items():
                if count > 0:
                    validation_errors.append(f"Column '{column}' has {count} missing values")

        # Check for price anomalies
        # High should be >= Low
        invalid_high_low = data[data['high'] < data['low']]
        if not invalid_high_low.empty:
            validation_errors.append(f"Found {len(invalid_high_low)} records where high < low")

        # High should be >= Open and Close
        invalid_high_open = data[data['high'] < data['open']]
        if not invalid_high_open.empty:
            validation_errors.append(f"Found {len(invalid_high_open)} records where high < open")

        invalid_high_close = data[data['high'] < data['close']]
        if not invalid_high_close.empty:
            validation_errors.append(f"Found {len(invalid_high_close)} records where high < close")

        # Low should be <= Open and Close
        invalid_low_open = data[data['low'] > data['open']]
        if not invalid_low_open.empty:
            validation_errors.append(f"Found {len(invalid_low_open)} records where low > open")

        invalid_low_close = data[data['low'] > data['close']]
        if not invalid_low_close.empty:
            validation_errors.append(f"Found {len(invalid_low_close)} records where low > close")

        # Check for zero or negative prices
        for column in ['open', 'high', 'low', 'close']:
            zero_or_negative = data[data[column] <= 0]
            if not zero_or_negative.empty:
                validation_errors.append(f"Found {len(zero_or_negative)} records with zero or negative {column} prices")

        # Check for negative volume
        negative_volume = data[data['volume'] < 0]
        if not negative_volume.empty:
            validation_errors.append(f"Found {len(negative_volume)} records with negative volume")

        # Check for duplicate records
        duplicates = data.duplicated(subset=['hawk_id', 'record_timestamp']).sum()
        if duplicates > 0:
            validation_errors.append(f"Found {duplicates} duplicate records (same hawk_id and timestamp)")

        return validation_errors
