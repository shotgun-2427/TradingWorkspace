import asyncio
import os

from opentelemetry import trace

from common.exceptions import NotATradingDayException
from common.logging import setup_logger
from common.otel import flush_otel, setup_otel, timed
from production.pipeline.validation import validate_simulation_merged_config
from production.runtime.config_loader import (
    load_simulation_profile_config,
    simulation_overrides_path,
)
from production.runtime.context import load_simulation_context
from production.runtime.orchestration import run_trading_engine
from production.runtime.sinks import build_artifact_sink
from production.simulations.core import orchestrate_marginal_simulations

logger = setup_logger(__name__)


async def main() -> None:
    context = load_simulation_context()
    logger.info(
        "Starting simulations pipeline for profile '%s' in run_mode '%s'.",
        context.profile,
        context.run_mode,
    )

    config = load_simulation_profile_config(context.profile)
    validate_simulation_merged_config(config=config, profile=context.profile)
    logger.info(
        "Loaded simulation config by merging execution profile '%s' with overrides '%s'.",
        context.profile,
        simulation_overrides_path(),
    )

    artifact_sink = build_artifact_sink(context)
    try:
        trading_result = await run_trading_engine(
            config=config,
            artifact_sink=artifact_sink,
            current_date=context.current_date,
            metric_prefix="simulations",
        )

        with timed("simulations.marginal_simulations_duration"):
            reduced_portfolio_backtests = orchestrate_marginal_simulations(
                config=config,
                model_insights=trading_result.model_insights,
                model_backtests=trading_result.model_backtests,
                prices=trading_result.prices,
            )

        await asyncio.gather(
            *(
                artifact_sink.save_polars(
                    df,
                    f"reduced_portfolio_backtests_{removed_model}_{optimizer}_{kind}.csv",
                )
                for removed_model, optimizer_results in reduced_portfolio_backtests.items()
                for optimizer, results in optimizer_results.items()
                for kind, df in results.items()
            )
        )

        await artifact_sink.save_config(config)
    finally:
        await artifact_sink.close()


if __name__ == "__main__":
    raw_profile = os.environ.get("SIMULATION_PROFILE", "paper").strip().lower()
    if raw_profile not in {"paper", "live"}:
        raw_profile = "paper"
    setup_otel(f"simulations_{raw_profile}")
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("simulations") as span:
        try:
            asyncio.run(main())
        except NotATradingDayException:
            logger.info("Not a trading day, exiting gracefully.")
            span.set_status(trace.StatusCode.UNSET, "Not a trading day")
        except Exception as exc:
            logger.error("Error running simulations pipeline: %s", exc)
            span.set_status(trace.StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            flush_otel()
            raise

    flush_otel()
