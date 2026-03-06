"""
@description: Abstract base class for supplemental/macro data sources that don't use hawk_ids.
@author: Rithwik Babu
"""
from abc import abstractmethod, ABC

from pandas import DataFrame, Float64Dtype as Float

from pipeline.common.bigquery_client import BigQueryClient
from pipeline.common.constants import SUPPLEMENTAL_RECORDS_TABLE_NAME, SUPPLEMENTAL_SERIES_TABLE_NAME
from pipeline.common.enums import Environment, WriteMode
from pipeline.common.models import GCPConfig
from pipeline.common.setup_logger import logger
from pipeline.common.utils import read_gcp_config


class BaseMacroSource(ABC):
    """
    Base class for supplemental/macro data sources that store data in the
    supplemental_records table instead of the hawk_id-based records table.
    
    Subclasses must implement fetch(), transform(), and validate() methods.
    """

    def __init__(self, environment: Environment, source: str):
        """
        Initialize the macro source.
        
        :param environment: The deployment environment (DEVELOPMENT or PRODUCTION)
        :param source: The data source identifier (e.g., "eia_petroleum", "fred")
        """
        self.config: GCPConfig = read_gcp_config(environment)
        self.source = source
        self.bq_client = BigQueryClient(self.config)
        
        # Load series metadata for this source
        self.series_metadata = self._load_series_metadata()

    def _load_series_metadata(self) -> dict[str, dict]:
        """
        Load series metadata from the supplemental_series table for this source.
        
        :return: Dictionary mapping series_id to metadata dict
        """
        query = f"""
        SELECT series_id, name, description, frequency, unit
        FROM `{self.config.project}.{self.config.dataset}.{SUPPLEMENTAL_SERIES_TABLE_NAME}`
        WHERE source = '{self.source}'
        """
        try:
            result_df = self.bq_client.execute_query(query)
            return {
                row['series_id']: {
                    'name': row['name'],
                    'description': row['description'],
                    'frequency': row['frequency'],
                    'unit': row['unit']
                }
                for _, row in result_df.iterrows()
            }
        except Exception as e:
            logger.warning(f"Could not load series metadata for source '{self.source}': {e}")
            return {}

    @abstractmethod
    def fetch(self, *args, **kwargs) -> DataFrame:
        """
        Retrieves data from the external data source.
        
        Implement in subclasses with source-specific logic.
        
        :return: Raw data as DataFrame
        """
        pass

    @abstractmethod
    def transform(self, *args, **kwargs) -> DataFrame:
        """
        Processes raw data into the required format.
        
        Implement in subclasses with transformation logic specific to each source.
        Expected output columns: source, series_id, record_timestamp, value
        
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
        Standardizes data structure to match supplemental_records BigQuery schema.
        
        Expected input columns: source, series_id, record_timestamp, value
        Output columns: source, series_id, record_timestamp, value, char_value
        
        :param data: Transformed DataFrame
        :return: DataFrame with columns and types compatible with BigQuery
        """
        # Ensure required columns exist
        required_cols = ['source', 'series_id', 'record_timestamp', 'value']
        missing = set(required_cols) - set(data.columns)
        if missing:
            raise ValueError(f"Missing required columns for normalization: {missing}")

        # Create a copy to avoid modifying the original
        df = data.copy()

        # Add char_value column if not present (nullable)
        if 'char_value' not in df.columns:
            df['char_value'] = None

        # Select and order columns for BigQuery schema
        df = df[['source', 'series_id', 'record_timestamp', 'value', 'char_value']]

        # Enforce types
        df = df.astype({
            'source': 'string',
            'series_id': 'string',
            'record_timestamp': 'datetime64[ns]',
            'value': Float(),
            'char_value': 'string'
        })

        return df

    def write(self, data: DataFrame, write_mode: WriteMode, output_path: str = None):
        """
        Persists processed data to the specified destination.
        
        :param data: Normalized DataFrame to write
        :param write_mode: Destination type (BIGQUERY, CSV, LOG)
        :param output_path: File path for CSV output (required for CSV mode)
        """
        if write_mode == WriteMode.BIGQUERY:
            self.bq_client.insert_records(data=data, table=SUPPLEMENTAL_RECORDS_TABLE_NAME)
            logger.info(f"Data successfully written to BigQuery: {self.config}.")
        elif write_mode == WriteMode.CSV:
            data.to_csv(output_path, index=False)
            logger.info(f"Data successfully written to {output_path}")
        elif write_mode == WriteMode.LOG:
            print(data.head(100))
            logger.info(f"Data successfully written to log.")
        else:
            raise ValueError(f"Unimplemented write mode: {write_mode}")
