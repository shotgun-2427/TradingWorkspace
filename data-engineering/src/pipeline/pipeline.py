"""
@description: A data pipeline that processes data through fetch, transform, validate, normalize, and write stages.
@author: Rithwik Babu
"""
import time
from typing import List, Union

from opentelemetry import trace

from pipeline.base_source import BaseSource
from pipeline.base_macro_source import BaseMacroSource
from pipeline.common.enums import WriteMode
from pipeline.common.setup_logger import logger


class Pipeline:
    def __init__(self, source: Union[BaseSource, BaseMacroSource], write_mode: WriteMode):
        """
        :param source: Data source implementation (e.g., SourceA, SourceB)
        :param write_mode: Mode for writing data (e.g., BIGQUERY, LOG, CSV)
        """
        self.source = source
        self.write_mode = write_mode

    def run(self, **fetch_kwargs) -> List[str]:
        """
        Execute the complete data pipeline.

        :param fetch_kwargs: Source-specific parameters for data fetching
        :return: List of validation errors, if any
        """
        span = trace.get_current_span()

        span.set_attribute('pipeline.write_mode', self.write_mode.value)
        span.set_attribute('pipeline.source', self.source.__class__.__name__)

        # 1. Fetch
        start_time = time.perf_counter()
        df_raw = self.source.fetch(**fetch_kwargs)
        end_time = time.perf_counter()

        span.set_attribute('pipeline.num_input_rows', len(df_raw))
        span.set_attribute('pipeline.fetch_duration', end_time - start_time)

        # 2. Transform
        start_time = time.perf_counter()
        df_transformed = self.source.transform(df_raw)
        end_time = time.perf_counter()

        span.set_attribute('pipeline.transform_duration', end_time - start_time)

        # 3. Validate
        start_time = time.perf_counter()
        validation_errors = self.source.validate(df_transformed)
        end_time = time.perf_counter()

        span.set_attribute('pipeline.validation_duration', end_time - start_time)
        span.set_attribute('pipeline.validation_errors', len(validation_errors))
        span.set_attribute('pipeline.validation_errors_list', validation_errors)

        if validation_errors:
            # Log validation errors but continue with the pipeline
            for error in validation_errors:
                logger.warning(f"Validation error: {error}")

        # 4. Normalize
        start_time = time.perf_counter()
        df_normalized = self.source.normalize(df_transformed)
        end_time = time.perf_counter()

        span.set_attribute('pipeline.normalize_duration', end_time - start_time)
        span.set_attribute('pipeline.num_output_rows', len(df_normalized))

        # 5. Write

        start_time = time.perf_counter()
        self.source.write(
            data=df_normalized,
            write_mode=self.write_mode
        )
        end_time = time.perf_counter()

        span.set_attribute('pipeline.write_duration', end_time - start_time)

        return validation_errors
