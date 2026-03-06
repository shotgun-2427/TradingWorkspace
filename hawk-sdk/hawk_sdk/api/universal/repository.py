"""
@description: Repository layer for fetching Universal data from BigQuery.
@author: Rithwik Babu
"""
import logging
from typing import Iterator, List

from google.cloud import bigquery

from hawk_sdk.core.common.utils import get_bigquery_client


class UniversalRepository:
    """Repository for accessing any data via hawk_ids and field_ids."""

    def __init__(self, environment: str) -> None:
        """Initializes the repository with a BigQuery client.

        :param environment: The environment to fetch data from (e.g., 'production', 'development').
        """
        self.bq_client = get_bigquery_client()
        self.environment = environment

    def fetch_data(
        self,
        hawk_ids: List[int],
        field_ids: List[int],
        start_date: str,
        end_date: str,
        interval: str
    ) -> Iterator[dict]:
        """Fetches raw data from BigQuery for the given hawk_ids and field_ids.

        :param hawk_ids: A list of hawk_ids to fetch data for.
        :param field_ids: A list of field_ids to fetch data for.
        :param start_date: The start date for the data query (YYYY-MM-DD).
        :param end_date: The end date for the data query (YYYY-MM-DD).
        :param interval: The interval for the data query (e.g., '1d', '1h', '1m').
        :return: An iterator over raw data rows.
        """
        query = f"""
        WITH field_info AS (
          SELECT 
            field_id,
            field_name
          FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.fields`
          WHERE 
            field_id IN UNNEST(@field_ids)
        ),
        records_data AS (
          SELECT 
            r.record_timestamp AS date,
            r.hawk_id,
            hi.value AS ticker,
            f.field_id,
            f.field_name,
            r.double_value,
            r.int_value,
            r.char_value
          FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.records` AS r
          JOIN 
            field_info AS f
            ON r.field_id = f.field_id
          LEFT JOIN 
            `wsb-hc-qasap-ae2e.{self.environment}.hawk_identifiers` AS hi
            ON r.hawk_id = hi.hawk_id AND hi.id_type = 'TICKER'
          WHERE 
            r.hawk_id IN UNNEST(@hawk_ids)
            AND r.record_timestamp BETWEEN @start_date AND @end_date
        )
        SELECT 
          date,
          hawk_id,
          ticker,
          field_id,
          field_name,
          double_value,
          int_value,
          char_value
        FROM 
          records_data
        ORDER BY 
          date, hawk_id, field_id;
        """

        query_params = [
            bigquery.ArrayQueryParameter("hawk_ids", "INT64", hawk_ids),
            bigquery.ArrayQueryParameter("field_ids", "INT64", field_ids),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date),
            bigquery.ScalarQueryParameter("end_date", "STRING", end_date),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch universal data: {e}")
            raise

    def fetch_snapshot(
        self,
        hawk_ids: List[int],
        field_ids: List[int],
        timestamp: str
    ) -> Iterator[dict]:
        """Fetches the most recent snapshot data from BigQuery for the given timestamp.

        :param hawk_ids: A list of hawk_ids to fetch data for.
        :param field_ids: A list of field_ids to fetch data for.
        :param timestamp: The cutoff timestamp (YYYY-MM-DD HH:MM:SS).
        :return: An iterator over raw data rows.
        """
        query = f"""
        WITH field_info AS (
          SELECT 
            field_id,
            field_name
          FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.fields`
          WHERE 
            field_id IN UNNEST(@field_ids)
        ),
        latest_timestamp AS (
          SELECT
            MAX(r.record_timestamp) AS max_ts
          FROM
            `wsb-hc-qasap-ae2e.{self.environment}.records` AS r
          WHERE
            r.hawk_id IN UNNEST(@hawk_ids)
            AND r.field_id IN UNNEST(@field_ids)
            AND r.record_timestamp <= @timestamp
        ),
        records_data AS (
          SELECT 
            r.record_timestamp AS date,
            r.hawk_id,
            hi.value AS ticker,
            f.field_id,
            f.field_name,
            r.double_value,
            r.int_value,
            r.char_value
          FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.records` AS r
          JOIN 
            field_info AS f
            ON r.field_id = f.field_id
          LEFT JOIN 
            `wsb-hc-qasap-ae2e.{self.environment}.hawk_identifiers` AS hi
            ON r.hawk_id = hi.hawk_id AND hi.id_type = 'TICKER'
          WHERE 
            r.hawk_id IN UNNEST(@hawk_ids)
            AND r.record_timestamp = (SELECT max_ts FROM latest_timestamp)
        )
        SELECT 
          date,
          hawk_id,
          ticker,
          field_id,
          field_name,
          double_value,
          int_value,
          char_value
        FROM 
          records_data
        ORDER BY 
          hawk_id, field_id;
        """

        query_params = [
            bigquery.ArrayQueryParameter("hawk_ids", "INT64", hawk_ids),
            bigquery.ArrayQueryParameter("field_ids", "INT64", field_ids),
            bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch universal snapshot data: {e}")
            raise

    def fetch_latest_snapshot(
        self,
        hawk_ids: List[int],
        field_ids: List[int]
    ) -> Iterator[dict]:
        """Fetches the most recent data from BigQuery for the given hawk_ids and field_ids.

        :param hawk_ids: A list of hawk_ids to fetch data for.
        :param field_ids: A list of field_ids to fetch data for.
        :return: An iterator over raw data rows.
        """
        query = f"""
        WITH field_info AS (
          SELECT 
            field_id,
            field_name
          FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.fields`
          WHERE 
            field_id IN UNNEST(@field_ids)
        ),
        latest_timestamp AS (
          SELECT
            MAX(r.record_timestamp) AS max_ts
          FROM
            `wsb-hc-qasap-ae2e.{self.environment}.records` AS r
          WHERE
            r.hawk_id IN UNNEST(@hawk_ids)
            AND r.field_id IN UNNEST(@field_ids)
        ),
        records_data AS (
          SELECT 
            r.record_timestamp AS date,
            r.hawk_id,
            hi.value AS ticker,
            f.field_id,
            f.field_name,
            r.double_value,
            r.int_value,
            r.char_value
          FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.records` AS r
          JOIN 
            field_info AS f
            ON r.field_id = f.field_id
          LEFT JOIN 
            `wsb-hc-qasap-ae2e.{self.environment}.hawk_identifiers` AS hi
            ON r.hawk_id = hi.hawk_id AND hi.id_type = 'TICKER'
          WHERE 
            r.hawk_id IN UNNEST(@hawk_ids)
            AND r.record_timestamp = (SELECT max_ts FROM latest_timestamp)
        )
        SELECT 
          date,
          hawk_id,
          ticker,
          field_id,
          field_name,
          double_value,
          int_value,
          char_value
        FROM 
          records_data
        ORDER BY 
          hawk_id, field_id;
        """

        query_params = [
            bigquery.ArrayQueryParameter("hawk_ids", "INT64", hawk_ids),
            bigquery.ArrayQueryParameter("field_ids", "INT64", field_ids),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch latest snapshot data: {e}")
            raise

    def fetch_field_ids_by_name(self, field_names: List[str]) -> Iterator[dict]:
        """Fetches field_ids for the given list of field names from BigQuery.

        :param field_names: A list of field name strings to lookup.
        :return: An iterator over raw data rows containing field_id and field_name.
        """
        query = f"""
        SELECT 
            field_id,
            field_name
        FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.fields`
        WHERE 
            field_name IN UNNEST(@field_names)
        """

        query_params = [
            bigquery.ArrayQueryParameter("field_names", "STRING", field_names),
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch field_ids: {e}")
            raise

    def fetch_all_fields(self) -> Iterator[dict]:
        """Fetches all available fields from BigQuery.

        :return: An iterator over raw data rows containing field_id and field_name.
        """
        query = f"""
        SELECT 
            field_id,
            field_name
        FROM 
            `wsb-hc-qasap-ae2e.{self.environment}.fields`
        ORDER BY 
            field_name
        """

        try:
            query_job = self.bq_client.query(query)
            return query_job.result()
        except Exception as e:
            logging.error(f"Failed to fetch fields: {e}")
            raise
