"""
@description: Service layer for processing and normalizing Universal Supplemental data.
@author: Rithwik Babu
"""
from typing import List, Iterator, Optional

import pandas as pd

from hawk_sdk.api.universal_supplemental.repository import UniversalSupplementalRepository


class UniversalSupplementalService:
    """Service class for Universal Supplemental data business logic."""

    def __init__(self, repository: UniversalSupplementalRepository) -> None:
        """Initializes the service with a repository.

        :param repository: An instance of UniversalSupplementalRepository for data access.
        """
        self.repository = repository

    def get_data(
        self,
        sources: List[str],
        series_ids: List[str],
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Fetches and normalizes supplemental data into a pandas DataFrame.

        :param sources: A list of data source identifiers.
        :param series_ids: A list of series codes within the source.
        :param start_date: The start date for the data query (YYYY-MM-DD).
        :param end_date: The end date for the data query (YYYY-MM-DD).
        :return: A pandas DataFrame containing the normalized data.
        """
        raw_data = self.repository.fetch_data(sources, series_ids, start_date, end_date)
        return self._normalize_data(raw_data)

    def get_data_by_source(
        self,
        sources: List[str],
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """Fetches all data for given sources into a pandas DataFrame.

        :param sources: A list of data source identifiers.
        :param start_date: The start date for the data query (YYYY-MM-DD).
        :param end_date: The end date for the data query (YYYY-MM-DD).
        :return: A pandas DataFrame containing the normalized data.
        """
        raw_data = self.repository.fetch_data_by_source(sources, start_date, end_date)
        return self._normalize_data(raw_data)

    def get_latest_data(
        self,
        sources: List[str],
        series_ids: List[str]
    ) -> pd.DataFrame:
        """Fetches the most recent data for each series.

        :param sources: A list of data source identifiers.
        :param series_ids: A list of series codes within the source.
        :return: A pandas DataFrame containing the latest data for each series.
        """
        raw_data = self.repository.fetch_latest_data(sources, series_ids)
        return self._normalize_data(raw_data)

    def get_all_series(self, source: Optional[str] = None) -> pd.DataFrame:
        """Fetches all available series metadata.

        :param source: Optional source to filter series by.
        :return: A pandas DataFrame containing series metadata.
        """
        raw_data = self.repository.fetch_all_series(source)
        return self._normalize_data(raw_data)

    def get_available_sources(self) -> pd.DataFrame:
        """Fetches all available data sources.

        :return: A pandas DataFrame containing unique source identifiers.
        """
        raw_data = self.repository.fetch_available_sources()
        return self._normalize_data(raw_data)

    @staticmethod
    def _normalize_data(data: Iterator[dict]) -> pd.DataFrame:
        """Converts raw data into a normalized pandas DataFrame.

        :param data: An iterator over raw data rows.
        :return: A pandas DataFrame containing normalized data.
        """
        return pd.DataFrame([dict(row) for row in data])
