import asyncio
import os

from opentelemetry import trace

from common.exceptions import NotATradingDayException
from common.logging import setup_logger
from common.otel import flush_otel, setup_otel
from production.pipeline.validation import validate_execution_config
from production.runtime.config_loader import (
    execution_config_path,
    load_execution_profile_config,
)
from production.runtime.context import load_execution_context
from production.runtime.orchestration import (
    ExecutionStageResult,
    build_goal_positions_notification_message,
    run_execution_stage,
    run_trading_engine,
)
from production.runtime.sinks import (
    build_artifact_sink,
    build_execution_sink,
    build_notification_sink,
)

logger = setup_logger(__name__)


async def main() -> None:
    context = load_execution_context()
    logger.info(
        "Starting production pipeline for profile '%s' in run_mode '%s'.",
        context.profile,
        context.run_mode,
    )

    config_path = execution_config_path(context.profile)
    config = load_execution_profile_config(context.profile)
    validate_execution_config(config=config, profile=context.profile)
    logger.info("Loaded execution config: %s", config_path)

    artifact_sink = build_artifact_sink(context)
    notification_sink = build_notification_sink(
        context=context,
        webhook_url=(config.notifications or {}).get("msteams_webhook"),
    )
    execution_sink = build_execution_sink(context)

    execution_result = ExecutionStageResult.empty()
    try:
        trading_result = await run_trading_engine(
            config=config,
            artifact_sink=artifact_sink,
            current_date=context.current_date,
            metric_prefix="production",
        )

        try:
            execution_result = await run_execution_stage(
                config=config,
                artifact_sink=artifact_sink,
                execution_sink=execution_sink,
                prices=trading_result.prices,
                portfolio_insights=trading_result.final_insights,
                metric_prefix="production",
            )
        except Exception as exc:
            logger.error(
                "Failed to run execution stage for profile '%s': %s",
                context.profile,
                exc,
            )

        await artifact_sink.save_config(config)
    finally:
        await artifact_sink.close()

    if execution_result.portfolio_insight is not None:
        message = build_goal_positions_notification_message(
            pipeline_name=context.profile,
            pipeline_mode=context.profile,
            portfolio_name=execution_result.portfolio_name,
            portfolio_insight=execution_result.portfolio_insight,
            current_date=context.current_date,
            basket_path=execution_result.basket_path,
        )
        notification_sink.send(message)


if __name__ == "__main__":
    raw_profile = (
        os.environ.get("PIPELINE_PROFILE")
        or os.environ.get("PIPELINE_MODE")
        or "paper"
    ).strip().lower()
    if raw_profile not in {"paper", "live"}:
        raw_profile = "paper"
    setup_otel(f"production_{raw_profile}")
    tracer = trace.get_tracer(__name__)

    with tracer.start_as_current_span("production") as span:
        try:
            asyncio.run(main())
        except NotATradingDayException:
            logger.info("Not a trading day, exiting gracefully.")
            span.set_status(trace.StatusCode.UNSET, "Not a trading day")
        except Exception as exc:
            logger.error("Error running production pipeline: %s", exc)
            span.set_status(trace.StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            flush_otel()
            raise

    flush_otel()
