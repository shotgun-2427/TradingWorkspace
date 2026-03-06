"""
@description: Client to connect to BigQuery, handles initialization and some common methods.
@author: Rithwik Babu
"""
import json
import logging
import os
from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from google.oauth2 import service_account
from pandas import DataFrame

from pipeline.common.constants import FIELDS_TABLE_NAME, HAWK_IDENTIFIERS_TABLE_NAME
from pipeline.common.models import GCPConfig


class BigQueryClient:
    def __init__(self, config: GCPConfig) -> None:
        """
        Initializes the BigQuery client with the appropriate authentication method:
        
        1. When running in Cloud Run: Uses built-in ADC (Application Default Credentials)
        2. When GOOGLE_APPLICATION_CREDENTIALS is set: Uses the service account key file
        3. When SERVICE_ACCOUNT_JSON is set: Uses the service account JSON string
        """
        try:
            # Default approach: Use Application Default Credentials
            # This works automatically in Cloud Run with the service account assigned to the job
            self.client = bigquery.Client(project=config.project)
            logging.info("BigQuery client initialized using Application Default Credentials")
        except Exception as adc_error:
            logging.warning(f"Could not initialize with ADC: {adc_error}")

            # Fallback to explicit credential methods
            if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
                # Initialize the client using the json file in the environment variable
                try:
                    self.client = bigquery.Client(project=config.project)
                    logging.info("BigQuery client initialized using GOOGLE_APPLICATION_CREDENTIALS")
                except Exception as cred_error:
                    raise ConnectionRefusedError(
                        f"Failed to initialize with GOOGLE_APPLICATION_CREDENTIALS: {cred_error}")

            elif 'SERVICE_ACCOUNT_JSON' in os.environ:
                # Load the service account credentials from the JSON string
                try:
                    service_account_json = os.environ.get('SERVICE_ACCOUNT_JSON')
                    credentials = service_account.Credentials.from_service_account_info(
                        json.loads(service_account_json)
                    )
                    self.client = bigquery.Client(
                        credentials=credentials,
                        project=config.project
                    )
                    logging.info("BigQuery client initialized using SERVICE_ACCOUNT_JSON")
                except Exception as json_error:
                    raise ConnectionRefusedError(f"Failed to initialize with SERVICE_ACCOUNT_JSON: {json_error}")
            else:
                raise ConnectionRefusedError("Missing GCP Credentials! Unable to authenticate with Google Cloud.")

        self.config = config

    def insert_records(self, data: DataFrame, table: str) -> None:
        """Append records to a BigQuery table from a DataFrame."""
        destination_id = f"{self.config.project}.{self.config.dataset}.{table}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        job = self.client.load_table_from_dataframe(
            dataframe=data,
            destination=destination_id,
            job_config=job_config
        )

        try:
            job.result()  # This will raise an exception if the job fails.
            print(f"Loaded {len(data)} rows into {destination_id}.")
        except Exception as e:
            print(f"Failed to load data into {destination_id}: {str(e)}")

    def execute_query(self, query: str, params: Optional[dict] = None) -> DataFrame:
        """
        Execute a query in BigQuery.

        :param query: The intended query to run in BigQuery in string format.
        :param params: An optional dictionary of query parameters.
        :returns: Pandas DataFrame with query output.
        """
        job_config = None

        if params:
            query_parameters = [
                bigquery.ScalarQueryParameter(name, str(value[1]), value[0]) for name, value in params.items()
            ]

            job_config = QueryJobConfig(query_parameters=query_parameters)

        query_job = self.client.query(query, job_config=job_config)
        return query_job.result().to_dataframe(create_bqstorage_client=False)

    """Predefined queries for the pipeline."""

    def load_fields(self) -> dict[str, tuple]:
        """Load the fields in the fields table."""
        query = f"""
        SELECT field_id, field_name, field_type
        FROM `{self.config.project}.{self.config.dataset}.{FIELDS_TABLE_NAME}`
        """
        result_df = self.execute_query(query)

        # Convert field_id to Python int type to avoid np.int64 keys
        result_df['field_id'] = result_df['field_id'].astype(int)
        return dict(zip(result_df['field_name'], zip(result_df['field_id'], result_df['field_type'])))

    def load_categorized_hawk_ids(self):
        """
        Load all hawk_ids with labels representing the asset class.

        The output format is a dictionary with asset classes as keys:
        {
            "equities": [10000, 10001, ...],
            "futures": [20000, 20001, ...],
            "ice_bofa_bond_indices": [30000, 30001, ...],
            "global_indices": [31000, 31001, ...],
            "other": []
        }

        This uses the explicit `asset_class` column in hawk_identifiers.
        """
        query = f"""
        SELECT DISTINCT hawk_id, asset_class
        FROM `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        """
        result_df = self.execute_query(query)

        output = {
            "futures": [],
            "equities": [],
            "ice_bofa_bond_indices": [],
            "global_indices": [],
            "other": []
        }

        for _, row in result_df.iterrows():
            hawk_id = int(row["hawk_id"])
            asset_class = str(row["asset_class"]).strip().lower() if row["asset_class"] is not None else ""

            if asset_class in output and asset_class != "other":
                output[asset_class].append(hawk_id)
            else:
                output["other"].append(hawk_id)

        return output
