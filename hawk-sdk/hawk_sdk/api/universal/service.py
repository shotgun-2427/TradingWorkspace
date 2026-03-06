"""
@description: Service layer for processing and normalizing Universal data.
@author: Rithwik Babu
"""
from typing import List, Iterator

import pandas as pd

from hawk_sdk.api.universal.repository import UniversalRepository


class UniversalService:
    """Service class for Universal data business logic."""

    def __init__(self, repository: UniversalRepository) -> None:
        """Initializes the service with a repository.

        :param repository: An instance of UniversalRepository for data access.
        """
        self.repository = repository

    def get_data(
        self,
        hawk_ids: List[int],
        field_ids: List[int],
        start_date: str,
        end_date: str,
        interval: str
    ) -> pd.DataFrame:
        """Fetches and normalizes universal data into a pandas DataFrame.

        The output DataFrame has columns: date, hawk_id, ticker, and one column
        per field_id/field_name. If a hawk_id doesn't have a value for a field
        on a given date, the value will be empty (NaN).

        :param hawk_ids: A list of hawk_ids to fetch data for.
        :param field_ids: A list of field_ids to fetch data for.
        :param start_date: The start date for the data query (YYYY-MM-DD). Ignored for snapshot.
        :param end_date: The end date (YYYY-MM-DD) or timestamp (YYYY-MM-DD HH:MM:SS) for snapshot.
        :param interval: The interval for the data query. Use 'snapshot' for point-in-time data.
        :return: A pandas DataFrame containing the normalized data.
        """
        if interval == "snapshot":
            raw_data = self.repository.fetch_snapshot(hawk_ids, field_ids, end_date)
        else:
            raw_data = self.repository.fetch_data(hawk_ids, field_ids, start_date, end_date, interval)
        return self._pivot_data(raw_data)

    def get_latest_snapshot(
        self,
        hawk_ids: List[int],
        field_ids: List[int]
    ) -> pd.DataFrame:
        """Fetches the most recent data available for the given hawk_ids and field_ids.

        :param hawk_ids: A list of hawk_ids to fetch data for.
        :param field_ids: A list of field_ids to fetch data for.
        :return: A pandas DataFrame containing the normalized data.
        """
        raw_data = self.repository.fetch_latest_snapshot(hawk_ids, field_ids)
        return self._pivot_data(raw_data)

    def get_field_ids(self, field_names: List[str]) -> pd.DataFrame:
        """Fetches field_ids for the given field names.

        :param field_names: A list of field name strings to lookup.
        :return: A pandas DataFrame containing field_id and field_name.
        """
        raw_data = self.repository.fetch_field_ids_by_name(field_names)
        return self._normalize_data(raw_data)

    def get_all_fields(self) -> pd.DataFrame:
        """Fetches all available fields.

        :return: A pandas DataFrame containing all field_id and field_name pairs.
        """
        raw_data = self.repository.fetch_all_fields()
        return self._normalize_data(raw_data)

    @staticmethod
    def _normalize_data(data: Iterator[dict]) -> pd.DataFrame:
        """Converts raw data into a normalized pandas DataFrame.

        :param data: An iterator over raw data rows.
        :return: A pandas DataFrame containing normalized data.
        """
        return pd.DataFrame([dict(row) for row in data])

    @staticmethod
    def _pivot_data(data: Iterator[dict]) -> pd.DataFrame:
        """Converts raw long-format data into a wide-format DataFrame.

        Takes data with rows like (date, hawk_id, ticker, field_name, value)
        and pivots it so each field becomes a column.

        :param data: An iterator over raw data rows.
        :return: A pandas DataFrame in wide format with field names as columns.
        """
        df = pd.DataFrame([dict(row) for row in data])

        if df.empty:
            return df

        # Coalesce the value columns into a single 'value' column
        # Prefer double_value, then int_value, then char_value
        df['value'] = df['double_value'].combine_first(
            df['int_value'].astype(float)
        ).combine_first(
            df['char_value']
        )

        # Pivot the DataFrame so each field_name becomes a column
        pivoted = df.pivot_table(
            index=['date', 'hawk_id', 'ticker'],
            columns='field_name',
            values='value',
            aggfunc='first'
        ).reset_index()

        # Flatten column names
        pivoted.columns.name = None

        return pivoted
