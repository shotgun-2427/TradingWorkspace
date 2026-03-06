"""
@description: Abstract base class for sources to implement to use in the pipeline. Handles reused logic.
@author: Rithwik Babu
"""
from abc import abstractmethod, ABC

from pandas import DataFrame, Int64Dtype as Integer, Float64Dtype as Float

from pipeline.common.bigquery_client import BigQueryClient
from pipeline.common.constants import HAWK_IDENTIFIERS_TABLE_NAME, RECORDS_TABLE_NAME
from pipeline.common.enums import Environment, WriteMode
from pipeline.common.models import GCPConfig
from pipeline.common.setup_logger import logger
from pipeline.common.utils import read_gcp_config, normalize_data


class BaseSource(ABC):
    def __init__(self, environment: Environment, source_type: str, column_mapping: dict[str, str] = None):
        self.config: GCPConfig = read_gcp_config(environment)
        self.source_type = source_type
        self.column_mapping = column_mapping

        self.bq_client = BigQueryClient(self.config)

        self.fields_mapping = self.bq_client.load_fields()
        self.hawk_id_to_ticker_map = self._load_hawk_ticker()

    def _load_hawk_figi(self) -> dict[int, str]:
        """
        Retrieves Hawk ID to FIGI symbol mapping from BigQuery.
        
        :return: Dictionary mapping Hawk IDs to FIGI symbols
        """
        query = f"""
        SELECT hawk_id, value
        FROM `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        WHERE id_type = 'FIGI'
        """
        result_df = self.bq_client.execute_query(query)

        # Convert hawk_id to Python int type to avoid np.int64 keys
        result_df['hawk_id'] = result_df['hawk_id'].astype(int)
        return dict(zip(result_df['hawk_id'], result_df['value']))

    def _load_hawk_ticker(self) -> dict[int, str]:
        """
        Retrieves Hawk ID to Ticker symbol mapping from BigQuery.
        
        :return: Dictionary mapping Hawk IDs to Ticker symbols
        """
        query = f"""
        SELECT hawk_id, value
        FROM `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        WHERE id_type = 'TICKER'
        """
        result_df = self.bq_client.execute_query(query)

        # Convert hawk_id to Python int type to avoid np.int64 keys
        result_df['hawk_id'] = result_df['hawk_id'].astype(int)
        return dict(zip(result_df['hawk_id'], result_df['value']))

    def _get_ticker(self, hawk_id: int) -> str:
        """
        Get the ticker symbol for a given Hawk ID from the BigQuery table.

        :param hawk_id: Hawk ID to look up.
        :return: Ticker symbol as a string.
        """
        ticker = self.hawk_id_to_ticker_map.get(hawk_id)
        if ticker is None:
            raise ValueError(f"Ticker not found for Hawk ID {hawk_id}")
        return ticker

    @abstractmethod
    def fetch(self, *args, **kwargs) -> DataFrame:
        """
        Retrieves data from source system.
        
        Implement in subclasses with source-specific logic.
        
        :return: Raw data as DataFrame
        """
        pass

    @abstractmethod
    def transform(self, *args, **kwargs) -> DataFrame:
        """
        Processes raw data into the required format.
        
        Implement in subclasses with transformation logic specific to each source.
        
        :return: Transformed DataFrame
        """
        pass

    @abstractmethod
    def validate(self, *args, **kwargs) -> list[str]:
        """
        Verifies data quality and integrity.
        
        Implement in subclasses with validation rules for each source.
        
        :return: List of validation error messages (empty if valid)
        """
        pass

    def normalize(self, data: DataFrame) -> DataFrame:
        """
        Standardizes data structure to match BigQuery schema requirements.
        
        :return: DataFrame with columns and types compatible with BigQuery
        """

        data = normalize_data(
            data=data,
            fields_mapping=self.fields_mapping,
            column_mapping=self.column_mapping
        )

        # Enforce types after processing
        data = data.astype({
            'hawk_id': Integer(),
            'record_timestamp': 'datetime64[ns]',
            'field_id': Integer(),
            'char_value': 'string',
            'int_value': Integer(),
            'double_value': Float()
        })

        return data

    def write(self, data: DataFrame, write_mode: WriteMode, output_path: str = None):
        """
        Persists processed data to the specified destination.
        
        :param data: Normalized DataFrame to write
        :param write_mode: Destination type (BIGQUERY, CSV, LOG)
        :param output_path: File path for CSV output (required for CSV mode)
        """
        if write_mode == WriteMode.BIGQUERY:
            self.bq_client.insert_records(data=data, table=RECORDS_TABLE_NAME)
            logger.info(f"Data successfully written to BigQuery: {self.config}.")
        elif write_mode == WriteMode.CSV:
            data.to_csv(output_path)
            logger.info(f"Data successfully written to {output_path}")
        elif write_mode == WriteMode.LOG:
            print(data.head(100))
            logger.info(f"Data successfully written to log.")
        else:
            raise ValueError(f"Unimplemented write mode: {write_mode}")
