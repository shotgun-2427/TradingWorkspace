import asyncio
import time

from opentelemetry import trace

from common.async_polars_gcs import AsyncGCSCSVWriter
from common.logging import setup_logger
from common.otel import setup_otel, flush_otel
from common.utils import read_config_yaml
from production.validation import validate_production_config
from trading_engine.core import (
    read_data, create_model_state, orchestrate_model_backtests, orchestrate_model_simulations,
    orchestrate_portfolio_backtests, orchestrate_portfolio_simulations
)

logger = setup_logger(__name__)


async def main():
    logger.info("Starting production pipeline.")
    # ==== setup
    current_date = time.strftime("%Y-%m-%d")
    writer = AsyncGCSCSVWriter(
        bucket_name="wsb-hc-qasap-bucket-1",
        prefix=f"hcf/production_audit/{current_date}",
        max_workers=4,
    )
    span = trace.get_current_span()
    logger.info(f"Using GCS bucket: {writer.bucket_name}, prefix: {writer.prefix}")

    # ==== read configuration
    c = read_config_yaml("production/config.yaml")
    validate_production_config(c)
    logger.info(f"Configuration loaded: {c}")
    t0 = time.perf_counter()

    # ==== read data
    raw_lf = read_data()
    logger.info(f"Data read complete")
    t1 = time.perf_counter()

    # ==== create model state
    state_df, price_df = create_model_state(
        lf=raw_lf,
        features=c.model_state_features,
        start_date=c.start_date,
        end_date=c.end_date,
        universe=c.universe,
    )
    logger.info(f"Model state created for features: {list(c.model_state_features)}")
    t2 = time.perf_counter()

    # ==== orchestrate model backtests
    model_insights = orchestrate_model_backtests(c.models, c.universe)
    logger.info(f"Model backtests orchestrated for models: {list(model_insights.keys())}")
    t3 = time.perf_counter()

    # ==== orchestrate model simulations
    model_backtests = orchestrate_model_simulations(
        model_insights=model_insights,
        initial_value=1_000_000.0,
    )
    logger.info(f"Model simulations orchestrated for models: {list(model_backtests.keys())}")
    t4 = time.perf_counter()

    # ==== orchestrate portfolio backtests
    portfolio_insights = orchestrate_portfolio_backtests(
        optimizers=c.optimizers,
        model_insights=model_insights,
        backtest_results=model_backtests,
        universe=c.universe,
    )
    logger.info(f"Portfolio backtests orchestrated for optimizers: {list(portfolio_insights.keys())}")
    t5 = time.perf_counter()

    # ==== orchestrate portfolio simulations
    portfolio_backtests = orchestrate_portfolio_simulations(
        portfolio_insights=portfolio_insights,
        initial_value=1_000_000.0,
    )
    logger.info(f"Portfolio simulations orchestrated for optimizers: {list(portfolio_backtests.keys())}")
    t6 = time.perf_counter()

    logger.info("Production pipeline completed successfully.")

    # ==== write results to GCS
    c.dump_to_gcs(f"gs://{writer.bucket_name}/{writer.prefix}/config.json")

    await writer.save(state_df, "model_state.csv")

    for name, insight in model_insights.items():
        await writer.save(insight, f"model_insights_{name}.csv")

    for name, results in model_backtests.items():
        for df_name, df in results.items():
            await writer.save(df, f"model_backtests_{name}_{df_name}.csv")

    for name, insight in portfolio_insights.items():
        await writer.save(insight, f"portfolio_insights_{name}.csv")

    for name, results in portfolio_backtests.items():
        for df_name, df in results.items():
            await writer.save(df, f"portfolio_backtests_{name}_{df_name}.csv")

    await writer.flush()
    await writer.close()
    logger.info("Results written to GCS")
    t7 = time.perf_counter()

    # ==== output
    logger.debug(f"Data read (lazy) in {(t1 - t0) * 1000:.0f}ms")
    logger.debug(f"Model state creation in {(t2 - t1) * 1000:.0f}ms")
    logger.debug(f"Model backtests in {(t3 - t2) * 1000:.0f}ms")
    logger.debug(f"Model simulations in {(t4 - t3) * 1000:.0f}ms")
    logger.debug(f"Portfolio backtests in {(t5 - t4) * 1000:.0f}ms")
    logger.debug(f"Portfolio simulations in {(t6 - t5) * 1000:.0f}ms")
    logger.debug(f"Total time: {(t6 - t0) * 1000:.0f}ms")
    logger.debug(f"Total time with GCS writes: {(t7 - t0) * 1000:.0f}ms")

    span.set_attribute('pipeline.read_duration', t1 - t0)
    span.set_attribute('pipeline.model_state_duration', t2 - t1)
    span.set_attribute('pipeline.model_backtests_duration', t3 - t2)
    span.set_attribute('pipeline.model_simulations_duration', t4 - t3)
    span.set_attribute('pipeline.portfolio_backtests_duration', t5 - t4)
    span.set_attribute('pipeline.portfolio_simulations_duration', t6 - t5)
    span.set_attribute('pipeline.gcs_write_duration', t7 - t6)
    span.set_attribute('pipeline.total_duration', t7 - t0)


if __name__ == "__main__":
    setup_otel('production_engineering')
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("pipeline") as span:
        try:
            asyncio.run(main())
        except Exception as e:
            logger.error(f"Error running production pipeline: {e}")
            span.set_status(trace.StatusCode.ERROR, str(e))
            span.record_exception(e)
            flush_otel()
            raise

    flush_otel()
