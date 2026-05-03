from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from polars import DataFrame

from common.exceptions import NotATradingDayException
from common.model import Config
from common.otel import timed
from trading_engine.core import (
    calculate_max_lookback,
    create_model_state,
    orchestrate_model_backtests,
    orchestrate_model_simulations,
    orchestrate_portfolio_aggregation,
    orchestrate_portfolio_optimizations,
    orchestrate_portfolio_simulations,
    read_data,
)


@dataclass
class TradingRunResult:
    prices: DataFrame
    model_insights: dict[str, DataFrame]
    model_backtests: dict[str, dict[str, Any]]
    final_insights: dict[str, DataFrame]


@dataclass
class ExecutionStageResult:
    portfolio_name: str
    portfolio_insight: DataFrame | None
    basket_path: str | None

    @classmethod
    def empty(cls) -> "ExecutionStageResult":
        """Return an empty execution stage result placeholder."""
        return cls(portfolio_name="", portfolio_insight=None, basket_path=None)


async def run_trading_engine(
    config: Config,
    artifact_sink,
    current_date: str,
    metric_prefix: str,
) -> TradingRunResult:
    """Run shared trading orchestration and persist all generated artifacts."""
    with timed(f"{metric_prefix}.read_data_duration"):
        raw_data_bundle = read_data(include_supplemental=True)

    latest_date = (
        raw_data_bundle.raw_records
        .select("date")
        .sort("date", descending=True)
        .first()
        .collect()
        .item()
    )
    if latest_date.strftime("%Y-%m-%d") != current_date:
        raise NotATradingDayException(
            f"Latest date in data ({latest_date}) does not match current date ({current_date})."
        )

    with timed(f"{metric_prefix}.model_state_duration"):
        total_lookback_days = calculate_max_lookback(
            features=config.model_state_features,
            models=config.models,
            aggregators=config.aggregators,
            optimizers=getattr(config, "optimizers", None),
        )
        model_state_bundle, prices = create_model_state(
            raw_data_bundle=raw_data_bundle,
            features=config.model_state_features,
            start_date=config.start_date,
            end_date=config.end_date,
            universe=config.universe,
            total_lookback_days=total_lookback_days,
            return_bundle=True,
        )
    await artifact_sink.save_polars(model_state_bundle.model_state, "model_state.csv")
    await artifact_sink.save_polars(
        model_state_bundle.supplemental_model_state,
        "supplemental_model_state.csv",
    )
    await artifact_sink.save_polars(prices, "prices.csv")

    with timed(f"{metric_prefix}.model_backtests_duration"):
        model_insights = orchestrate_model_backtests(
            model_state_bundle=model_state_bundle,
            models=config.models,
            universe=config.universe,
        )
    await asyncio.gather(
        *(
            artifact_sink.save_polars(df, f"model_insights_{name}.csv")
            for name, df in model_insights.items()
        )
    )

    with timed(f"{metric_prefix}.model_simulations_duration"):
        model_backtests = orchestrate_model_simulations(
            prices=prices,
            model_insights=model_insights,
            start_date=config.start_date,
            end_date=config.end_date,
            initial_value=500_000.0,
        )
    await asyncio.gather(
        *(
            artifact_sink.save_polars(df, f"model_backtests_{model}_{kind}.csv")
            for model, results in model_backtests.items()
            for kind, df in results["backtest_results"].items()
        )
    )

    with timed(f"{metric_prefix}.portfolio_aggregation_duration"):
        aggregated_insights = orchestrate_portfolio_aggregation(
            model_insights=model_insights,
            backtest_results=model_backtests,
            universe=config.universe,
            aggregators=config.aggregators,
            start_date=config.start_date,
            end_date=config.end_date,
        )
    await asyncio.gather(
        *(
            artifact_sink.save_polars(df, f"aggregated_insights_{name}.csv")
            for name, df in aggregated_insights.items()
        )
    )

    portfolio_optimizers = getattr(config, "optimizers", []) or []
    optimized_insights: dict[str, DataFrame] = {}
    if portfolio_optimizers:
        with timed(f"{metric_prefix}.portfolio_optimization_duration"):
            optimized_insights = orchestrate_portfolio_optimizations(
                prices=prices,
                aggregated_insights=aggregated_insights,
                universe=config.universe,
                optimizers=portfolio_optimizers,
            )
        await asyncio.gather(
            *(
                artifact_sink.save_polars(df, f"optimized_insights_{name}.csv")
                for name, df in optimized_insights.items()
            )
        )

    final_insights = optimized_insights if optimized_insights else aggregated_insights

    with timed(f"{metric_prefix}.portfolio_simulations_duration"):
        portfolio_backtests = orchestrate_portfolio_simulations(
            prices=prices,
            portfolio_insights=final_insights,
            start_date=config.start_date,
            end_date=config.end_date,
            initial_value=1_000_000.0,
        )
    await asyncio.gather(
        *(
            artifact_sink.save_polars(df, f"portfolio_backtests_{pname}_{kind}.csv")
            for pname, results in portfolio_backtests.items()
            for kind, df in results.items()
        )
    )

    return TradingRunResult(
        prices=prices,
        model_insights=model_insights,
        model_backtests=model_backtests,
        final_insights=final_insights,
    )


def select_execution_portfolio(
    config: Config,
    portfolio_insights: dict[str, DataFrame],
) -> tuple[str, DataFrame]:
    """Select the portfolio to use for execution from available insights."""
    configured_name = getattr(config, "execution_portfolio", None)
    if configured_name:
        if configured_name not in portfolio_insights:
            raise ValueError(
                f"Configured execution_portfolio='{configured_name}' not found. "
                f"Available portfolios: {sorted(portfolio_insights.keys())}."
            )
        return configured_name, portfolio_insights[configured_name]

    config_optimizers = getattr(config, "optimizers", []) or []
    if len(config_optimizers) == 1 and config_optimizers[0] in portfolio_insights:
        optimizer_name = config_optimizers[0]
        return optimizer_name, portfolio_insights[optimizer_name]

    if len(portfolio_insights) == 1:
        portfolio_name = next(iter(portfolio_insights.keys()))
        return portfolio_name, portfolio_insights[portfolio_name]

    raise ValueError(
        "Cannot infer execution portfolio from multiple candidates. "
        f"Available: {sorted(portfolio_insights.keys())}. "
        "Set execution_portfolio in config."
    )


async def run_execution_stage(
    config: Config,
    artifact_sink,
    execution_sink,
    prices: DataFrame,
    portfolio_insights: dict[str, DataFrame],
    metric_prefix: str,
) -> ExecutionStageResult:
    """Run execution sink against selected portfolio and return summary outputs."""
    portfolio_name, portfolio_insight = select_execution_portfolio(
        config=config,
        portfolio_insights=portfolio_insights,
    )
    with timed(f"{metric_prefix}.execution_sink_duration"):
        basket_path = await execution_sink.run(
            config=config,
            artifact_sink=artifact_sink,
            prices=prices,
            portfolio_insight=portfolio_insight,
        )
    return ExecutionStageResult(
        portfolio_name=portfolio_name,
        portfolio_insight=portfolio_insight,
        basket_path=basket_path,
    )


def build_goal_positions_notification_message(
    *,
    pipeline_name: str,
    pipeline_mode: str,
    portfolio_name: str,
    portfolio_insight: DataFrame,
    current_date: str,
    basket_path: str | None,
) -> str:
    """Build the HTML goal-position message payload for Teams notifications."""
    last_row_dict = portfolio_insight[-1].to_dict(as_series=False)
    goal_position_list = [
        f"{key}: {round(value[0] * 100, 2)}%"
        for key, value in last_row_dict.items()
        if key != "date"
    ]
    goal_position_string = "<br>".join(goal_position_list)
    message = (
        f"<strong>{pipeline_mode.title()} Pipeline ({pipeline_name}) Goal Positions ({current_date})</strong><br>"
        f"<strong>Portfolio:</strong> {portfolio_name}<br>{goal_position_string}"
    )
    if basket_path:
        message += (
            f"<br><br>IBKR Basket (MOC): <a href='{basket_path}'>Download CSV</a>"
        )
    return message
