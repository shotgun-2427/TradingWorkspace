"""
@description: Module to source EIA petroleum data from the EIA API.
@author: Rithwik Babu
"""
import os
from typing import List, Optional

import pandas as pd
import requests
from pandas import DataFrame, Float64Dtype as Float

from pipeline.base_macro_source import BaseMacroSource
from pipeline.common.enums import Environment
from pipeline.common.setup_logger import logger


class EIAPetroleum(BaseMacroSource):
    """
    Fetch EIA petroleum data (crude oil stocks, imports, production) from the EIA API.
    
    This source fetches weekly petroleum data for the following series:
    - WCESTUS1: U.S. Ending Stocks excl. SPR of Crude Oil (Thousand Barrels)
    - WCRNTUS2: U.S. Net Imports of Crude Oil (Thousand Barrels per Day)
    - WCRFPUS2: U.S. Field Production of Crude Oil (Thousand Barrels per Day)
    """

    BASE_URL = "https://api.eia.gov/v2"
    
    # Series configuration: series_id -> API endpoint path
    SERIES_CONFIG = {
        "WCESTUS1": {
            "endpoint": "petroleum/stoc/wstk/data/",
            "description": "Weekly U.S. Ending Stocks excluding SPR of Crude Oil"
        },
        "WCRNTUS2": {
            "endpoint": "petroleum/move/wkly/data/",
            "description": "Weekly U.S. Net Imports of Crude Oil"
        },
        "WCRFPUS2": {
            "endpoint": "petroleum/sum/sndw/data/",
            "description": "Weekly U.S. Field Production of Crude Oil"
        }
    }

    def __init__(self, environment: Environment):
        super().__init__(environment=environment, source="eia_petroleum")
        
        self.api_key = os.environ.get('EIA_API_KEY')
        if not self.api_key:
            raise ValueError("EIA_API_KEY must be set in environment variables")

    def fetch(self, start_date: str, end_date: str, series_ids: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Fetch EIA petroleum data for the specified date range and series.
        
        :param start_date: Start date for the data (YYYY-MM-DD)
        :param end_date: End date for the data (YYYY-MM-DD)
        :param series_ids: Optional list of series IDs to fetch. If None, fetches all configured series.
        :return: Raw data from EIA API
        """
        if series_ids is None:
            series_ids = list(self.SERIES_CONFIG.keys())
        
        all_data = []
        
        for series_id in series_ids:
            if series_id not in self.SERIES_CONFIG:
                logger.warning(f"Unknown series_id: {series_id}, skipping")
                continue
            
            config = self.SERIES_CONFIG[series_id]
            url = f"{self.BASE_URL}/{config['endpoint']}"
            
            params = {
                "api_key": self.api_key,
                "frequency": "weekly",
                "data[0]": "value",
                "facets[series][]": series_id,
                "start": start_date,
                "end": end_date
            }
            
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if "response" in data and "data" in data["response"]:
                    records = data["response"]["data"]
                    for record in records:
                        all_data.append({
                            "series_id": series_id,
                            "period": record.get("period"),
                            "value": record.get("value"),
                            "unit": record.get("units") or record.get("unit")
                        })
                    logger.info(f"Fetched {len(records)} records for series {series_id}")
                else:
                    logger.warning(f"No data returned for series {series_id}")
                    
            except requests.RequestException as e:
                logger.error(f"Error fetching series {series_id}: {e}")
                raise
        
        if not all_data:
            return pd.DataFrame(columns=["series_id", "period", "value", "unit"])
        
        return pd.DataFrame(all_data)

    def transform(self, data: DataFrame) -> DataFrame:
        """
        Transform EIA petroleum data to the supplemental_records format.
        
        :param data: Input DataFrame from fetch()
        :return: Transformed DataFrame with columns: source, series_id, record_timestamp, value
        """
        if data.empty:
            return pd.DataFrame(columns=["source", "series_id", "record_timestamp", "value"])
        
        df = data.copy()
        
        # Add source column
        df["source"] = self.source
        
        # Convert period to timestamp
        df["record_timestamp"] = pd.to_datetime(df["period"])
        
        # Convert value to float
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        
        # Select required columns
        df = df[["source", "series_id", "record_timestamp", "value"]]
        
        # Enforce types
        df = df.astype({
            "source": "string",
            "series_id": "string",
            "record_timestamp": "datetime64[ns]",
            "value": Float(),
        })
        
        # Drop rows with missing values
        df = df.dropna(subset=["value", "record_timestamp"])
        
        return df

    def validate(self, data: DataFrame) -> List[str]:
        """
        Validate the transformed EIA petroleum data.
        
        :param data: The transformed data to validate
        :return: List of validation error messages. Empty list if no errors.
        """
        errors: List[str] = []
        
        if data.empty:
            errors.append("Dataset is empty")
            return errors
        
        required_cols = {"source", "series_id", "record_timestamp", "value"}
        missing = required_cols - set(data.columns)
        if missing:
            errors.append(f"Missing columns: {sorted(missing)}")
            return errors
        
        # Check for duplicates in (source, series_id, record_timestamp)
        if data.duplicated(subset=["source", "series_id", "record_timestamp"]).any():
            dup_count = data.duplicated(subset=["source", "series_id", "record_timestamp"]).sum()
            errors.append(f"Found {dup_count} duplicate rows for (source, series_id, record_timestamp)")
        
        # Check for NaN values in value column
        if data["value"].isna().any():
            nan_count = data["value"].isna().sum()
            errors.append(f"Found {nan_count} NaN values in value column")
        
        # Check that all series_ids are known
        unknown_series = set(data["series_id"].unique()) - set(self.SERIES_CONFIG.keys())
        if unknown_series:
            errors.append(f"Unknown series_ids: {sorted(unknown_series)}")
        
        return errors
