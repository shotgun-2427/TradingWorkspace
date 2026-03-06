"""
@description: Service layer for processing and normalizing System data.
@author: Rithwik Babu
"""

from typing import List, Iterator

import pandas as pd

from hawk_sdk.api.system.repository import SystemRepository


class SystemService:
    """Service class for System business logic."""

    def __init__(self, repository: SystemRepository) -> None:
        """Initializes the service with a repository.

        :param repository: An instance of SystemRepository for data access.
        """
        self.repository = repository

    def get_hawk_ids(self, tickers: List[str]) -> pd.DataFrame:
        """Fetches and normalizes hawk IDs into a pandas DataFrame.

        :param tickers: A list of specific tickers to filter by.
        :return: A pandas DataFrame containing the normalized hawk ID data.
        """
        raw_data = self.repository.fetch_hawk_ids(tickers)
        return self._normalize_data(raw_data)

    @staticmethod
    def _normalize_data(data: Iterator[dict]) -> pd.DataFrame:
        """Converts raw data into a normalized pandas DataFrame.

        :param data: An iterator over raw data rows.
        :return: A pandas DataFrame containing normalized data.
        """
        return pd.DataFrame([dict(row) for row in data])
