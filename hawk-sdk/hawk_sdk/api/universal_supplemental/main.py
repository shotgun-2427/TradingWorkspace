"""
@description: Datasource API for Universal Supplemental data access and export functions.
@author: Rithwik Babu
"""
from typing import List, Optional

from hawk_sdk.api.universal_supplemental.repository import UniversalSupplementalRepository
from hawk_sdk.api.universal_supplemental.service import UniversalSupplementalService
from hawk_sdk.core.common.data_object import DataObject


class UniversalSupplemental:
    """Datasource API for fetching supplemental data via source and series_id.
    
    Supplemental data is not tied to a hawk_id and is identified by a 
    combination of source (e.g., 'eia_petroleum', 'fred') and series_id 
    (e.g., 'WCESTUS1').
    """

    def __init__(self, environment="production") -> None:
        """Initializes the Universal Supplemental datasource with required configurations."""
        self.repository = UniversalSupplementalRepository(environment=environment)
        self.service = UniversalSupplementalService(self.repository)

    def get_data(
        self,
        sources: List[str],
        series_ids: List[str],
        start_date: str,
        end_date: str
    ) -> DataObject:
        """Fetch supplemental data for specific sources and series_ids.

        Returns a DataFrame with columns: source, series_id, series_name,
        record_timestamp, value, char_value.

        :param sources: A list of data source identifiers (e.g., ['eia_petroleum']).
        :param series_ids: A list of series codes (e.g., ['WCESTUS1', 'WCRFPUS2']).
        :param start_date: The start date (YYYY-MM-DD).
        :param end_date: The end date (YYYY-MM-DD).
        :return: A hawk DataObject containing the data.
        """
        return DataObject(
            name="supplemental_data",
            data=self.service.get_data(sources, series_ids, start_date, end_date)
        )

    def get_data_by_source(
        self,
        sources: List[str],
        start_date: str,
        end_date: str
    ) -> DataObject:
        """Fetch all supplemental data for the given sources.

        Useful when you want all series from a particular source without
        specifying individual series_ids.

        :param sources: A list of data source identifiers (e.g., ['eia_petroleum']).
        :param start_date: The start date (YYYY-MM-DD).
        :param end_date: The end date (YYYY-MM-DD).
        :return: A hawk DataObject containing the data.
        """
        return DataObject(
            name="supplemental_data_by_source",
            data=self.service.get_data_by_source(sources, start_date, end_date)
        )

    def get_latest_data(
        self,
        sources: List[str],
        series_ids: List[str]
    ) -> DataObject:
        """Fetch the most recent data point for each specified series.

        :param sources: A list of data source identifiers.
        :param series_ids: A list of series codes.
        :return: A hawk DataObject containing the latest data for each series.
        """
        return DataObject(
            name="supplemental_latest_data",
            data=self.service.get_latest_data(sources, series_ids)
        )

    def get_all_series(self, source: Optional[str] = None) -> DataObject:
        """Get all available series metadata.

        Returns a DataFrame with columns: source, series_id, name, 
        description, frequency, unit.

        :param source: Optional source to filter series by (e.g., 'eia_petroleum').
        :return: A hawk DataObject containing series metadata.
        """
        return DataObject(
            name="supplemental_series",
            data=self.service.get_all_series(source)
        )

    def get_available_sources(self) -> DataObject:
        """Get all available data sources.

        Useful for discovering what supplemental data sources are available.

        :return: A hawk DataObject containing unique source identifiers.
        """
        return DataObject(
            name="supplemental_sources",
            data=self.service.get_available_sources()
        )
