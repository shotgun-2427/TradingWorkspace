import asyncio
import time

from opentelemetry import trace

from common.IBKR import IBKR
from common.async_polars_gcs import AsyncGCSCSVWriter
from common.logging import setup_logger
from common.otel import setup_otel, flush_otel
from common.utils import read_config_yaml, post_to_teams
from production.core import construct_goal_positions, calculate_rebalance_orders, generate_trade_report
from production.validation import validate_production_config
from trading_engine.core import (
    read_data, create_model_state, orchestrate_model_backtests, orchestrate_model_simulations,
    orchestrate_portfolio_backtests, orchestrate_portfolio_simulations
)

logger = setup_logger(__name__)


class NotATradingDayError(Exception):
    pass


async def main():
    logger.info("Starting production pipeline.")
    # ======== setup
    # ==== initialization
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

    # ======== trading engine
    # ==== read data
    raw_lf = read_data()
    logger.info(f"Data read complete")
    t1 = time.perf_counter()

    # ==== validate data (check if the latest date in the data is "current" date, otherwise it's not a trading day)
    latest_date = raw_lf.select("date").sort("date", descending=True).first().collect().item()

    if latest_date.strftime('%Y-%m-%d') != current_date:
        logger.info(
            f"Latest date in data ({latest_date}) does not match current date ({current_date}). Therefore, it is not a trading day. Gracefully exiting."
        )
        raise NotATradingDayError()

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

    # ======== execution engine
    # ==== calculate goal positions
    portfolio_name, portfolio_insight = portfolio_insights.popitem()  # only one portfolio is configured in production
    ibkr = await IBKR.create(c.ib_gateway.host, c.ib_gateway.port, c.ib_gateway.client_id)
    goal_positions = construct_goal_positions(
        ibkr=ibkr,
        insights=portfolio_insight,
        prices=price_df,
        universe=c.universe,
    )
    logger.info(f"Goal positions calculated for portfolio: {portfolio_name}")
    t7 = time.perf_counter()

    # ==== calculate rebalance orders
    rebalance_df = calculate_rebalance_orders(
        ibkr=ibkr,
        targets=goal_positions,
        universe=c.universe,
        close_out_outside_universe=True,
    )
    logger.info(f"Rebalance orders calculated for portfolio: {portfolio_name}")
    t8 = time.perf_counter()

    # ==== generate trade report
    rebalance_with_px = (
        rebalance_df.join(goal_positions.select(["ticker", "price"]), on="ticker", how="left")
    )

    # TODO: Retain all info.
    # TEMP: ONLY INCLUDE GOAL POSITIONS
    report_txt = generate_trade_report(rebalance_with_px, as_of=current_date)

    print(report_txt)

    # post_to_teams(
    #     webhook_url=c.notifications["msteams_webhook"],
    #     message=report_txt
    # )

    logger.info("Production pipeline completed successfully.")

    # ======== cleanup
    # ==== write results to GCS
    # c.dump_to_gcs(f"gs://{writer.bucket_name}/{writer.prefix}/config.json")
    #
    # await writer.save(state_df, "model_state.csv")
    #
    # for name, insight in model_insights.items():
    #     await writer.save(insight, f"model_insights_{name}.csv")
    #
    # for name, results in model_backtests.items():
    #     for df_name, df in results.items():
    #         await writer.save(df, f"model_backtests_{name}_{df_name}.csv")
    #
    # for name, insight in portfolio_insights.items():
    #     await writer.save(insight, f"portfolio_insights_{name}.csv")
    #
    # for name, results in portfolio_backtests.items():
    #     for df_name, df in results.items():
    #         await writer.save(df, f"portfolio_backtests_{name}_{df_name}.csv")

    # await writer.save(goal_positions, "goal_positions.csv")
    # await writer.save(rebalance_df, "rebalance_orders.csv")

    await writer.flush()
    await writer.close()
    logger.info("Results written to GCS")
    t9 = time.perf_counter()

    # ==== output
    logger.debug(f"Data read (lazy) in {(t1 - t0) * 1000:.0f}ms")
    logger.debug(f"Model state creation in {(t2 - t1) * 1000:.0f}ms")
    logger.debug(f"Model backtests in {(t3 - t2) * 1000:.0f}ms")
    logger.debug(f"Model simulations in {(t4 - t3) * 1000:.0f}ms")
    logger.debug(f"Portfolio backtests in {(t5 - t4) * 1000:.0f}ms")
    logger.debug(f"Portfolio simulations in {(t6 - t5) * 1000:.0f}ms")
    logger.debug(f"Goal positions calculation in {(t7 - t6) * 1000:.0f}ms")
    logger.debug(f"Rebalance orders calculation in {(t8 - t7) * 1000:.0f}ms")
    logger.debug(f"Total time: {(t9 - t0) * 1000:.0f}ms")
    logger.debug(f"Total time with GCS writes: {(t9 - t0) * 1000:.0f}ms")

    span.set_attribute('pipeline.read_duration', t1 - t0)
    span.set_attribute('pipeline.model_state_duration', t2 - t1)
    span.set_attribute('pipeline.model_backtests_duration', t3 - t2)
    span.set_attribute('pipeline.model_simulations_duration', t4 - t3)
    span.set_attribute('pipeline.portfolio_backtests_duration', t5 - t4)
    span.set_attribute('pipeline.portfolio_simulations_duration', t6 - t5)
    span.set_attribute('pipeline.goal_positions_duration', t7 - t6)
    span.set_attribute('pipeline.rebalance_orders_duration', t8 - t7)
    span.set_attribute('pipeline.gcs_write_duration', t9 - t8)
    span.set_attribute('pipeline.total_duration', t9 - t0)


if __name__ == "__main__":
    setup_otel('production_engineering')
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("pipeline") as span:
        try:
            asyncio.run(main())
        except NotATradingDayError:
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
