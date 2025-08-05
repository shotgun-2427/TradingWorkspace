import asyncio
import time

from opentelemetry import trace
from polars import DataFrame

from common.async_gcs_writer import AsyncGCSWriter
from common.interactive_brokers import IBKR
from common.logging import setup_logger
from common.model import Config
from common.otel import setup_otel, flush_otel, timed
from common.utils import read_config_yaml, post_to_teams
from production.core import construct_goal_positions, construct_rebalance_orders, generate_trade_report
from production.validation import validate_production_config
from trading_engine.core import (
    read_data, create_model_state, orchestrate_model_backtests, orchestrate_model_simulations,
    orchestrate_portfolio_backtests, orchestrate_portfolio_simulations
)

logger = setup_logger(__name__)


class NotATradingDayException(Exception):
    pass


async def setup() -> tuple:
    """Setup function to initialize resources if needed."""
    # Config
    config = read_config_yaml("production/config.yaml")
    validate_production_config(config)
    current_date_str = time.strftime("%Y-%m-%d")
    logger.info(f"Configuration loaded: {config}")

    # GCS Writer
    gcs_writer = AsyncGCSWriter(
        bucket_name="wsb-hc-qasap-bucket-1",
        prefix=f"hcf/production_audit/{current_date_str}",
    )
    logger.info(f"Using GCS bucket: {gcs_writer.bucket_name}, prefix: {gcs_writer.prefix}")

    return config, current_date_str, gcs_writer


async def run_trading_engine(config: Config, writer: AsyncGCSWriter, current_date: str):
    with timed("pipeline.read_data_duration"):
        lf = read_data()

    # ==== validate data (check if the latest date in the data is "current" date, otherwise it's not a trading day)
    latest_date = lf.select("date").sort("date", descending=True).first().collect().item()

    if latest_date.strftime("%Y-%m-%d") != current_date:
        logger.warning("Not a trading day.")
        raise NotATradingDayException()

    # ==== create model state
    with timed("pipeline.model_state_duration"):
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
    with timed("pipeline.model_backtests_duration"):
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
    with timed("pipeline.model_simulations_duration"):
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
    with timed("pipeline.portfolio_backtests_duration"):
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
    with timed("pipeline.portfolio_simulations_duration"):
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

    return portfolio_insights, prices


async def run_execution_engine(
        config: Config,
        writer: AsyncGCSWriter,
        prices: DataFrame,
        portfolio_insight: DataFrame,
        current_date: str
):
    ib_client = await IBKR.create(
        hostname=config.ib_gateway.host,
        port=config.ib_gateway.port,
        client_id=config.ib_gateway.client_id
    )

    # ==== calculate goal positions
    with timed("pipeline.goal_positions_duration"):
        goal_positions = construct_goal_positions(
            ib_client=ib_client,
            insights=portfolio_insight,
            prices=prices,
            universe=config.universe,
        )

    await writer.save_polars(goal_positions, "goal_positions.csv")

    with timed("pipeline.rebalance_orders_duration"):
        rebalance_df = construct_rebalance_orders(
            ib_client=ib_client,
            targets=goal_positions,
            universe=config.universe,
            close_out_outside_universe=True,
        )

    await writer.save_polars(rebalance_df, "rebalance_orders.csv")

    logger.info("FINISHED EXECUTION")

    # ==== generate trade report
    rebalance_with_px = (
        rebalance_df.join(goal_positions.select(["ticker", "price"]), on="ticker", how="left")
    )

    trade_report = generate_trade_report(
        df=rebalance_with_px,
        as_of=current_date,
    )

    await writer.save_text(trade_report, "trade_report.txt", content_type="text/html")
    # construct location
    trade_report_location = f"gs://{writer.bucket_name}/{writer.prefix}/trade_report.txt"
    return trade_report_location


async def main():
    logger.info("Starting production pipeline.")
    # ==== setup
    c, current_date, writer = await setup()

    # ==== trading engine
    portfolio_insights, prices = await run_trading_engine(c, writer, current_date)

    # ==== execution engine
    portfolio_name, portfolio_insight = portfolio_insights.popitem()

    report_path = None
    try:
        report_path = await run_execution_engine(c, writer, prices, portfolio_insight, current_date)
    except Exception as e:
        logger.error(f"Failed to run execution engine: {e}")

    # ==== Flush write to GCS
    c.dump_to_gcs(f"gs://{writer.bucket_name}/{writer.prefix}/config.json")
    await writer.flush()
    await writer.close()

    # Post Goal Positions to Teams
    last_row_dict = portfolio_insight[-1].to_dict(as_series=False)

    goal_position_list = [f"{k}: {round(v[0] * 100, 2)}%" for k, v in last_row_dict.items() if k != 'date']
    goal_position_string = "<br>".join(goal_position_list)
    message = f"<strong>Goal Positions ({current_date})</strong><br>{goal_position_string}"

    if report_path:
        message += f"<br><br>Execution Report: <a href='{report_path}'>View Report</a>"

    logger.info(message)

    post_to_teams(
        webhook_url=c.notifications["msteams_webhook"],
        message=message
    )


if __name__ == "__main__":
    setup_otel('production_engineering')
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("pipeline") as span:
        try:
            asyncio.run(main())
        except NotATradingDayException:
            logger.info("Not a trading day, exiting gracefully.")
            span.set_status(trace.StatusCode.UNSET, "Not a trading day")
            # We need to find a better way to skip trading days.
        except Exception as e:
            logger.error(f"Error running production pipeline: {e}")
            span.set_status(trace.StatusCode.ERROR, str(e))
            span.record_exception(e)
            flush_otel()
            raise

    flush_otel()
