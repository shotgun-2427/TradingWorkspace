"""
@description: Repository layer for fetching Universal Supplemental data from BigQuery.
@author: Rithwik Babu
"""
import logging
from typing import Iterator, List, Optional

from google.cloud import bigquery

from hawk_sdk.core.common.utils import get_bigquery_client


class UniversalSupplementalRepository:
    """Repository for accessing supplemental data via source and series_id."""

    def __init__(self, environment: str) -> None:
        """Initializes the repository with a BigQuery client.

        :param environment: The environment to fetch data from (e.g., 'production', 'development').
        """
        self.bq_client = get_bigquery_client()
        self.environment = environment

    def fetch_data(
        self,
        sources: List[str],
        series_ids: List[str],
        start_date: str,
        end_date: str
    ) -> Iterator[dict]:
        """Fetches supplemental data from BigQuery for the given sources and series_ids.

        :param sources: A list of data source identifiers (e.g., 'eia_petroleum', 'fred').
        :param series_ids: A list of series codes within the source (e.g., 'WCESTUS1').
        :param start_date: The start date for the data query (YYYY-MM-DD).
        :param end_date: The end date for the data query (YYYY-MM-DD).
        :return: An iterator over raw data rows.
        """
        query = f"""
        SELECT 
            sr.source,
            sr.series_id,
            ss.name AS series_name,
            sr.record_timestamp,
            sr.value,
            sr.char_value
        FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.supplemental_records` AS sr
        LEFT JOIN 
            `wsb-hc-qasap-ae2e.{self.environment}.supplemental_series` AS ss
            ON sr.source = ss.source AND sr.series_id = ss.series_id
        WHERE 
            sr.source IN UNNEST(@sources)
            AND sr.series_id IN UNNEST(@series_ids)
            AND sr.record_timestamp BETWEEN @start_date AND @end_date
        ORDER BY 
            sr.source, sr.series_id, sr.record_timestamp;
        """

        query_params = [
            bigquery.ArrayQueryParameter("sources", "STRING", sources),
            bigquery.ArrayQueryParameter("series_ids", "STRING", series_ids),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch supplemental data: {e}")
            raise

    def fetch_data_by_source(
        self,
        sources: List[str],
        start_date: str,
        end_date: str
    ) -> Iterator[dict]:
        """Fetches all supplemental data for given sources from BigQuery.

        :param sources: A list of data source identifiers (e.g., 'eia_petroleum', 'fred').
        :param start_date: The start date for the data query (YYYY-MM-DD).
        :param end_date: The end date for the data query (YYYY-MM-DD).
        :return: An iterator over raw data rows.
        """
        query = f"""
        SELECT 
            sr.source,
            sr.series_id,
            ss.name AS series_name,
            sr.record_timestamp,
            sr.value,
            sr.char_value
        FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.supplemental_records` AS sr
        LEFT JOIN 
            `wsb-hc-qasap-ae2e.{self.environment}.supplemental_series` AS ss
            ON sr.source = ss.source AND sr.series_id = ss.series_id
        WHERE 
            sr.source IN UNNEST(@sources)
            AND sr.record_timestamp BETWEEN @start_date AND @end_date
        ORDER BY 
            sr.source, sr.series_id, sr.record_timestamp;
        """

        query_params = [
            bigquery.ArrayQueryParameter("sources", "STRING", sources),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch supplemental data by source: {e}")
            raise

    def fetch_latest_data(
        self,
        sources: List[str],
        series_ids: List[str]
    ) -> Iterator[dict]:
        """Fetches the most recent supplemental data for each series.

        :param sources: A list of data source identifiers.
        :param series_ids: A list of series codes within the source.
        :return: An iterator over raw data rows.
        """
        query = f"""
        WITH latest_per_series AS (
            SELECT
                source,
                series_id,
                MAX(record_timestamp) AS max_ts
            FROM
                `wsb-hc-qasap-ae2e.{self.environment}.supplemental_records`
            WHERE
                source IN UNNEST(@sources)
                AND series_id IN UNNEST(@series_ids)
            GROUP BY
                source, series_id
        )
        SELECT 
            sr.source,
            sr.series_id,
            ss.name AS series_name,
            sr.record_timestamp,
            sr.value,
            sr.char_value
        FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.supplemental_records` AS sr
        INNER JOIN 
            latest_per_series AS lps
            ON sr.source = lps.source 
            AND sr.series_id = lps.series_id 
            AND sr.record_timestamp = lps.max_ts
        LEFT JOIN 
            `wsb-hc-qasap-ae2e.{self.environment}.supplemental_series` AS ss
            ON sr.source = ss.source AND sr.series_id = ss.series_id
        ORDER BY 
            sr.source, sr.series_id;
        """

        query_params = [
            bigquery.ArrayQueryParameter("sources", "STRING", sources),
            bigquery.ArrayQueryParameter("series_ids", "STRING", series_ids),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch latest supplemental data: {e}")
            raise

    def fetch_all_series(self, source: Optional[str] = None) -> Iterator[dict]:
        """Fetches all available series metadata from BigQuery.

        :param source: Optional source to filter series by.
        :return: An iterator over raw data rows containing series metadata.
        """
        if source:
            query = f"""
            SELECT 
                source,
                series_id,
                name,
                description,
                frequency,
                unit
            FROM 
                `wsb-hc-qasap-ae2e.{self.environment}.supplemental_series`
            WHERE
                source = @source
            ORDER BY 
                source, series_id
            """
            query_params = [
                bigquery.ScalarQueryParameter("source", "STRING", source),
            ]
            job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        else:
            query = f"""
            SELECT 
                source,
                series_id,
                name,
                description,
                frequency,
                unit
            FROM 
                `wsb-hc-qasap-ae2e.{self.environment}.supplemental_series`
            ORDER BY 
                source, series_id
            """
            job_config = None

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch series metadata: {e}")
            raise

    def fetch_available_sources(self) -> Iterator[dict]:
        """Fetches all available data sources from BigQuery.

        :return: An iterator over raw data rows containing unique sources.
        """
        query = f"""
        SELECT DISTINCT
            source
        FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.supplemental_series`
        ORDER BY 
            source
        """

        try:
            query_job = self.bq_client.query(query)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch available sources: {e}")
            raise
