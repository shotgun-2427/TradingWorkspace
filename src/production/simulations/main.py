import asyncio
from datetime import datetime, date, datetime as dt
from zoneinfo import ZoneInfo

import polars as pl
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
    read_data,
    create_model_state,
    orchestrate_model_backtests,
    orchestrate_model_simulations,
    orchestrate_portfolio_aggregation,
    orchestrate_portfolio_optimizations,
    orchestrate_portfolio_simulations,
)

logger = setup_logger(__name__)


async def setup() -> tuple:
    """Setup function to initialize resources if needed."""
    # Config
    config = read_config_yaml("production/paper/config.yaml")
    validate_production_config(config)
    logger.info(f"Configuration loaded: {config}")

    # Use current NY date string (your pipeline expects YYYY-MM-DD)
    current_date_str = (datetime.now(ZoneInfo("America/New_York"))).strftime("%Y-%m-%d")

    # GCS Writer
    gcs_writer = AsyncGCSWriter(
        bucket_name="wsb-hc-qasap-bucket-1",
        prefix=f"hcf/paper/simulations_audit/{current_date_str}",
    )
    logger.info(f"Using GCS bucket: {gcs_writer.bucket_name}, prefix: {gcs_writer.prefix}")

    return config, current_date_str, gcs_writer


def _normalize_date_to_str(x) -> str:
    """Robustly convert a value to YYYY-MM-DD for comparisons."""
    if isinstance(x, dt):
        return x.strftime("%Y-%m-%d")
    if isinstance(x, date):
        return x.strftime("%Y-%m-%d")
    if isinstance(x, str):
        # assume already YYYY-MM-DD (safe for your pipeline)
        return x
    # polars can return Python objects; fallback via string slice
    return str(x)[:10]


async def run_trading_engine(config: Config, writer: AsyncGCSWriter, current_date: str):
    initial_value = 500_000.0  # keep your existing baseline

    # ==== read data
    with timed("simulations.read_data_duration"):
        lf = read_data()

    # ==== validate data (ensure latest data date == current date)
    # Use max(date) from the lazy scan to avoid collecting all rows
    latest_df = lf.select(pl.col("date").max().alias("max_date")).collect()
    latest_date_str = _normalize_date_to_str(latest_df["max_date"][0])

    if latest_date_str != current_date:
        logger.info(f"Latest date in data: {latest_date_str}")
        logger.info(f"Current date: {current_date}")
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

    # ==== orchestrate model backtests (returns LazyFrames)
    with timed("simulations.model_backtests_duration"):
        model_insights = orchestrate_model_backtests(
            model_state=model_state,
            models=config.models,
            universe=config.universe,
        )

    # Save collected model insights (collect from LazyFrame)
    await asyncio.gather(*(
        writer.save_polars(lf.collect(), f"model_insights_{name}.csv")
        for name, lf in model_insights.items()
    ))

    # ==== orchestrate model simulations
    with timed("simulations.model_simulations_duration"):
        model_backtests = orchestrate_model_simulations(
            prices=prices,
            model_insights=model_insights,
            start_date=config.start_date,
            end_date=config.end_date,
            initial_value=initial_value,
        )
    # Save canonical backtest results to GCS (accurate backtest starting from start_date)
    await asyncio.gather(*(
        writer.save_polars(df, f"model_backtests_{model}_{kind}.csv")
        for model, results in model_backtests.items()
        for kind, df in results["backtest_results"].items()
    ))

    # ==== aggregate (NEW: replaces orchestrate_portfolio_backtests)
    with timed("simulations.portfolio_aggregation_duration"):
        aggregated_results = orchestrate_portfolio_aggregation(
            model_insights=model_insights,
            backtest_results=model_backtests,
            universe=config.universe,
            aggregators=config.aggregators,
            start_date=config.start_date,
            end_date=config.end_date,
        )
    await asyncio.gather(*(
        writer.save_polars(df, f"aggregated_insights_{name}.csv")
        for name, df in aggregated_results.items()
    ))

    # ==== optimize (NEW: explicit optimizer stage)
    with timed("simulations.portfolio_optimizations_duration"):
        optimizer_weights = orchestrate_portfolio_optimizations(
            prices=prices,
            aggregated_insights=aggregated_results,
            universe=config.universe,
            optimizers=config.optimizers,
        )
    await asyncio.gather(*(
        writer.save_polars(df, f"optimizer_weights_{name}.csv")
        for name, df in optimizer_weights.items()
    ))

    # ==== simulate portfolios (use optimizer weights)
    with timed("simulations.portfolio_simulations_duration"):
        portfolio_backtests = orchestrate_portfolio_simulations(
            prices=prices,
            portfolio_insights=optimizer_weights,
            start_date=config.start_date,
            end_date=config.end_date,
            initial_value=initial_value,
        )
    await asyncio.gather(*(
        writer.save_polars(df, f"portfolio_backtests_{pname}_{kind}.csv")
        for pname, results in portfolio_backtests.items()
        for kind, df in results.items()
    ))

    # ==== marginal simulations (compares against optimizer-level sims)
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

    # Return both stages’ products if you need them later
    return aggregated_results, optimizer_weights, prices, marginal_results


async def main():
    logger.info("Starting simulations pipeline.")
    # ==== setup
    c, current_date, writer = await setup()

    # ==== trading engine
    aggregated, optimizer_weights, prices, marginal_results = await run_trading_engine(c, writer, current_date)

    # ==== Flush write to GCS
    c.dump_to_gcs(f"gs://{writer.bucket_name}/{writer.prefix}/config.json")
    await writer.flush()
    await writer.close()


if __name__ == "__main__":
    setup_otel("simulations_paper")
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("simulations") as span:
        try:
            asyncio.run(main())
        except NotATradingDayException:
            logger.info("Not a trading day, exiting gracefully.")
            span.set_status(trace.StatusCode.UNSET, "Not a trading day")
        except Exception as e:
            logger.error(f"Error running simulations pipeline: {e}")
            span.set_status(trace.StatusCode.ERROR, str(e))
            span.record_exception(e)
            flush_otel()
            raise

    flush_otel()
