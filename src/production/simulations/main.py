import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from opentelemetry import trace

from common.async_gcs_writer import AsyncGCSWriter
from common.exceptions import NotATradingDayException
from common.logging import setup_logger
from common.model import Config
from common.otel import setup_otel, flush_otel, timed
from common.utils import read_config_yaml
from production.paper.validation import validate_production_config
from production.simulations.core import orchestrate_marginal_simulations
from trading_engine.core import (
    read_data, create_model_state, orchestrate_model_backtests, orchestrate_model_simulations,
    orchestrate_portfolio_backtests, orchestrate_portfolio_simulations
)

logger = setup_logger(__name__)


async def setup() -> tuple:
    """Setup function to initialize resources if needed."""
    # Config
    config = read_config_yaml("production/paper/config.yaml")
    validate_production_config(config)
    logger.info(f"Configuration loaded: {config}")

    # TODO: Add max(current_date, config.start_date) logic for testing
    current_date_str = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    # GCS Writer
    gcs_writer = AsyncGCSWriter(
        bucket_name="wsb-hc-qasap-bucket-1",
        prefix=f"hcf/paper/simulations_audit/{current_date_str}",
    )
    logger.info(f"Using GCS bucket: {gcs_writer.bucket_name}, prefix: {gcs_writer.prefix}")

    return config, current_date_str, gcs_writer


async def run_trading_engine(config: Config, writer: AsyncGCSWriter, current_date: str):
    with timed("simulations.read_data_duration"):
        lf = read_data()

    # ==== validate data (check if the latest date in the data is "current" date, otherwise it's not a trading day)
    latest_date = lf.select("date").sort("date", descending=True).first().collect().item()

    if latest_date.strftime("%Y-%m-%d") != current_date:
        logger.info(f'Latest date in data: {latest_date.strftime("%Y-%m-%d")}')
        logger.info(f'Current date: {current_date}')
        logger.warning("Not a trading day.")
        raise NotATradingDayException()

    # ==== create model state
    with timed("simulations.model_state_duration"):
        model_state, prices = create_model_state(
            lf=lf,
            features=config.model_state_features,
            start_date=config.start_date,
            end_date=config.end_date,
            universe=config.universe,
        )
    await writer.save_polars(model_state, "model_state.csv")
    await writer.save_polars(prices, "prices.csv")

    # ==== orchestrate model backtests
    with timed("simulations.model_backtests_duration"):
        model_insights = orchestrate_model_backtests(
            model_state=model_state,
            models=config.models,
            universe=config.universe
        )
    await asyncio.gather(*(
        writer.save_polars(df, f"model_insights_{name}.csv")
        for name, df in model_insights.items()
    ))

    # ==== orchestrate model simulations
    with timed("simulations.model_simulations_duration"):
        model_backtests = orchestrate_model_simulations(
            prices=prices,
            model_insights=model_insights,
            initial_value=1_000_000.0,
        )
    await asyncio.gather(*(
        writer.save_polars(df, f"model_backtests_{model}_{kind}.csv")
        for model, results in model_backtests.items()
        for kind, df in results.items()
    ))

    # ==== orchestrate portfolio backtests
    with timed("simulations.portfolio_backtests_duration"):
        portfolio_insights = orchestrate_portfolio_backtests(
            optimizers=config.optimizers,
            model_insights=model_insights,
            backtest_results=model_backtests,
            universe=config.universe,
        )
    await asyncio.gather(*(
        writer.save_polars(df, f"portfolio_insights_{name}.csv")
        for name, df in portfolio_insights.items()
    ))

    # ==== orchestrate portfolio simulations
    with timed("simulations.portfolio_simulations_duration"):
        portfolio_backtests = orchestrate_portfolio_simulations(
            prices=prices,
            portfolio_insights=portfolio_insights,
            initial_value=1_000_000.0,
        )
    await asyncio.gather(*(
        writer.save_polars(df, f"portfolio_backtests_{pname}_{kind}.csv")
        for pname, results in portfolio_backtests.items()
        for kind, df in results.items()
    ))

    # ==== orchestrate marginal simulations
    with timed("simulations.marginal_simulations_duration"):
        marginal_results = orchestrate_marginal_simulations(
            config=config,
            model_insights=model_insights,
            model_backtests=model_backtests,
            main_portfolio_backtests=portfolio_backtests,
            prices=prices,
        )
    await asyncio.gather(*(
        writer.save_polars(df, f"marginal_results_{model}_{kind}.csv")
        for model, results in marginal_results.items()
        for kind, df in results.items()
    ))

    return portfolio_insights, prices, marginal_results


async def main():
    logger.info("Starting simulations pipeline.")
    # ==== setup
    c, current_date, writer = await setup()

    # ==== trading engine
    portfolio_insights, prices, marginal_results = await run_trading_engine(c, writer, current_date)

    # ==== Flush write to GCS
    c.dump_to_gcs(f"gs://{writer.bucket_name}/{writer.prefix}/config.json")
    await writer.flush()
    await writer.close()


if __name__ == "__main__":
    setup_otel('simulations_paper')
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("simulations") as span:
        try:
            asyncio.run(main())
        except NotATradingDayException:
            logger.info("Not a trading day, exiting gracefully.")
            span.set_status(trace.StatusCode.UNSET, "Not a trading day")
            # We need to find a better way to skip trading days.
        except Exception as e:
            logger.error(f"Error running simulations pipeline: {e}")
            span.set_status(trace.StatusCode.ERROR, str(e))
            span.record_exception(e)
            flush_otel()
            raise

    flush_otel()
