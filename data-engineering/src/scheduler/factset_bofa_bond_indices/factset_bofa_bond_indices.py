"""
@description: Cloud Run job for FactSet BofA/ICE Bond Indices pipeline.
@author: Rithwik Babu
"""
import logging
import os
import sys
import traceback
from datetime import date

from opentelemetry import trace

from pipeline.common.bigquery_client import BigQueryClient
from pipeline.common.enums import WriteMode, Environment
from pipeline.common.otel import setup_otel, flush_otel
from pipeline.common.utils import read_gcp_config
from pipeline.pipeline import Pipeline
from pipeline.sources.factset_equities_bofa_bond_indicies import FactsetEquitiesBofABond

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
    FactSet BofA/ICE Bond Indices source - fetches daily bond index data and inserts into BigQuery.
    """
    tracer = trace.get_tracer(__name__)
    env_str = os.environ.get("environment", "dev")
    environment = Environment.PRODUCTION if env_str == "prod" else Environment.DEVELOPMENT
    write_mode = get_write_mode(environment)

    logging.info(f"Running with environment={environment.value}, write_mode={write_mode.value}")

    with tracer.start_as_current_span("factset_bofa_bond_indices") as span:
        try:
            config = read_gcp_config(environment)
            bq_client = BigQueryClient(config)
            categorized_ids = bq_client.load_categorized_hawk_ids()
            bofa_hawk_ids = categorized_ids.get("ice_bofa_bond_indices", [])

            if not bofa_hawk_ids:
                logging.warning("No ice_bofa_bond_indices hawk_ids found. Check hawk_identifiers table.")
                return

        except Exception as e:
            span.set_status(trace.StatusCode.ERROR, str(traceback.format_exc().splitlines()[0]))
            span.record_exception(e)
            logging.error(f"Error loading Hawk IDs from BigQuery: {e}")
            raise

        try:
            pipeline = Pipeline(
                source=FactsetEquitiesBofABond(
                    environment=environment,
                    interval='1d'
                ),
                write_mode=write_mode,
            )

            pipeline.run(
                start_date=date.today().strftime("%Y-%m-%d"),
                end_date=date.today().strftime("%Y-%m-%d"),
                securities=bofa_hawk_ids,
            )

            logging.info("FactSet BofA Bond Indices source successfully executed.")

        except Exception as e:
            span.set_status(trace.StatusCode.ERROR, str(traceback.format_exc().splitlines()[0]))
            span.record_exception(e)
            logging.error(f"Error running FactSet BofA Bond Indices source: {e}")
            raise


if __name__ == "__main__":
    setup_otel('factset_bofa_bond_indices')
    run_source()
    flush_otel()
