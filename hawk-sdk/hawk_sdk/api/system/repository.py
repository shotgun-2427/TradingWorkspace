"""
@description: Repository layer for fetching System data from BigQuery.
@author: Rithwik Babu
"""
import logging
from typing import Iterator, List

from google.cloud import bigquery

from hawk_sdk.core.common.utils import get_bigquery_client


class SystemRepository:
    """Repository for accessing System data."""

    def __init__(self, environment: str) -> None:
        """Initializes the repository with a BigQuery client.

        :param bq_client: An instance of BigQuery Client.
        :param environment: The environment to fetch data from (e.g., 'production', 'development').
        """
        self.bq_client = get_bigquery_client()
        self.environment = environment

    def fetch_hawk_ids(self, tickers: List[str]) -> Iterator[dict]:
        """Fetches hawk_ids for the given list of tickers from BigQuery.

        :param tickers: A list of ticker strings to filter by.
        :return: An iterator over raw data rows.
        """
        query = f"""
        SELECT 
            value AS ticker, 
            hawk_id
        FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.hawk_identifiers`
        WHERE 
            id_type = 'TICKER'
            AND value IN UNNEST(@ticker_list)
        """

        query_params = [
            bigquery.ArrayQueryParameter("ticker_list", "STRING", tickers),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch hawk_ids: {e}")
            raise
