"""
@description: Cloud Run job for EIA Petroleum pipeline.
@author: Rithwik Babu
"""
import logging
import os
import sys
import traceback
from datetime import date, timedelta

from opentelemetry import trace

from pipeline.common.enums import WriteMode, Environment
from pipeline.common.otel import setup_otel, flush_otel
from pipeline.pipeline import Pipeline
from pipeline.sources.eia_petroleum import EIAPetroleum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)


def get_write_mode(environment: Environment) -> WriteMode:
    """
    Determine write mode from environment variable or default based on environment.
    
    - WRITE_MODE env var can be: BIGQUERY, CSV, LOG
    - Default: LOG for dev, BIGQUERY for prod
    """
    write_mode_str = os.environ.get("WRITE_MODE", "").upper()
    
    if write_mode_str:
        try:
            return WriteMode[write_mode_str]
        except KeyError:
            logging.warning(f"Invalid WRITE_MODE '{write_mode_str}', using default")
    
    # Default based on environment
    return WriteMode.BIGQUERY if environment == Environment.PRODUCTION else WriteMode.LOG


def run_source() -> None:
    """
    EIA Petroleum source - fetches weekly petroleum data and inserts into BigQuery.
    
    This runs weekly to fetch the latest EIA petroleum data including:
    - U.S. Crude Oil Stocks
    - U.S. Net Imports
    - U.S. Field Production
    """
    tracer = trace.get_tracer(__name__)
    env_str = os.environ.get("environment", "dev")
    environment = Environment.PRODUCTION if env_str == "prod" else Environment.DEVELOPMENT
    write_mode = get_write_mode(environment)

    logging.info(f"Running with environment={environment.value}, write_mode={write_mode.value}")

    with tracer.start_as_current_span("eia_petroleum") as span:
        try:
            # Fetch data for the last 2 weeks to ensure we capture any late updates
            end_date = date.today()
            start_date = end_date - timedelta(weeks=2)

            pipeline = Pipeline(
                source=EIAPetroleum(environment=environment),
                write_mode=write_mode,
            )

            pipeline.run(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )

            logging.info("EIA Petroleum source successfully executed.")

        except Exception as e:
            span.set_status(trace.StatusCode.ERROR, str(traceback.format_exc().splitlines()[0]))
            span.record_exception(e)
            logging.error(f"Error running EIA Petroleum source: {e}")
            raise


if __name__ == "__main__":
    setup_otel('eia_petroleum')
    run_source()
    flush_otel()
