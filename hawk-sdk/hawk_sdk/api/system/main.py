"""
@description: Datasource API for Hawk System data access and export functions.
@author: Rithwik Babu
"""
from typing import List

from hawk_sdk.api.system.repository import SystemRepository
from hawk_sdk.api.system.service import SystemService
from hawk_sdk.core.common.data_object import DataObject


class System:
    """Datasource API for fetching System data."""

    def __init__(self, environment="production") -> None:
        """Initializes the System datasource with required configurations."""
        self.repository = SystemRepository(environment=environment)
        self.service = SystemService(self.repository)

    def get_hawk_ids(self, tickers: List[str]) -> DataObject:
        """Fetch hawk_ids for the given list of tickers.

        :param tickers: A list of specific tickers to filter by.
        :return: A hawk DataObject containing the hawk ID data.
        """
        return DataObject(
            name="system_hawk_id_mappings",
            data=self.service.get_hawk_ids(tickers)
        )
