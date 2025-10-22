import polars as pl

from common.utils import _get_metric
from trading_engine.core import (
    orchestrate_portfolio_aggregation,
    orchestrate_portfolio_optimizations,
    orchestrate_portfolio_simulations,
)


def orchestrate_marginal_simulations(
        config,
        model_insights,  # Dict[str, pl.LazyFrame] from orchestrate_model_backtests
        model_backtests,  # Dict[str, dict] from orchestrate_model_simulations
        main_portfolio_backtests,  # Dict[str, dict] from orchestrate_portfolio_simulations (after optimizers)
        prices  # pl.DataFrame from construct_prices(...)
):
    """
    For each model in config.models, recompute the full portfolio pipeline with that model removed
    and measure marginal impact vs main_portfolio_backtests (assumed to be optimizer-level sims).
    Returns: { removed_model: { optimizer_name: pl.DataFrame(metric,value) } }
    """
    results = {}

    # quick exits
    if not getattr(config, "models", None) or len(config.models) <= 1:
        return results
    if not getattr(config, "optimizers", None):
        return results
    if not getattr(config, "aggregators", None):
        # We need aggregators to rebuild portfolios in the new pipeline
        return results

    def calculate_marginal_values(main_df: pl.DataFrame | None, reduced_df: pl.DataFrame | None = None) -> dict:
        if main_df is None:
            return {}
        if reduced_df is None:
            # absolute (non-marginal) snapshot for baseline reporting
            return {
                "excess_return": _get_metric(main_df, "annualized_return"),
                "sharpe_improvement": _get_metric(main_df, "sharpe_ratio"),
                "sortino_improvement": _get_metric(main_df, "sortino_ratio"),
                "drawdown_improvement": -_get_metric(main_df, "max_drawdown"),
                "volatility_change": _get_metric(main_df, "annualized_volatility"),
                "turnover_increase": _get_metric(main_df, "portfolio_turnover"),
                "additional_costs": _get_metric(main_df, "cumulative_slippage_cost"),
            }
        # marginal: (main − reduced) with signs so "improvement" > 0 is good
        return {
            "excess_return": _get_metric(main_df, "annualized_return") - _get_metric(reduced_df, "annualized_return"),
            "sharpe_improvement": _get_metric(main_df, "sharpe_ratio") - _get_metric(reduced_df, "sharpe_ratio"),
            "sortino_improvement": _get_metric(main_df, "sortino_ratio") - _get_metric(reduced_df, "sortino_ratio"),
            "drawdown_improvement": _get_metric(reduced_df, "max_drawdown") - _get_metric(main_df, "max_drawdown"),
            "volatility_change": _get_metric(main_df, "annualized_volatility") - _get_metric(reduced_df,
                                                                                             "annualized_volatility"),
            "turnover_increase": _get_metric(main_df, "portfolio_turnover") - _get_metric(reduced_df,
                                                                                          "portfolio_turnover"),
            "additional_costs": _get_metric(main_df, "cumulative_slippage_cost") - _get_metric(reduced_df,
                                                                                               "cumulative_slippage_cost"),
        }

    def create_marginal_df(optimizer_names, main_backtests, reduced_backtests=None):
        """
        Build per-optimizer DataFrames with marginal metrics.
        main_backtests / reduced_backtests are the dicts returned by orchestrate_portfolio_simulations.
        """
        out = {}
        for opt in optimizer_names:
            # both sides must exist to compute marginal deltas
            has_main = opt in main_backtests
            has_reduced = reduced_backtests is None or opt in reduced_backtests
            if not has_main or not has_reduced:
                continue

            main_df = main_backtests[opt]["backtest_metrics"] if has_main else None
            reduced_df = (
                reduced_backtests[opt]["backtest_metrics"] if (reduced_backtests and opt in reduced_backtests) else None
            )

            vals = calculate_marginal_values(main_df, reduced_df)
            if not vals:
                continue
            vals["net_benefit"] = vals["excess_return"] - vals["additional_costs"]
            out[opt] = pl.DataFrame([{"metric": k, "value": v} for k, v in vals.items()])
        return out

    # Iterate: remove one model at a time and rebuild portfolio → optimizer → sims
    for removed in config.models:
        reduced_insights = {k: v for k, v in model_insights.items() if k != removed}
        reduced_model_backtests = {k: v for k, v in model_backtests.items() if k != removed}

        if not reduced_model_backtests:
            # No sims without any models left; report absolute (non-marginal) snapshot
            model_results = create_marginal_df(config.optimizers, main_portfolio_backtests, reduced_backtests=None)
            if model_results:
                results[removed] = model_results
            continue

        # 1) Aggregate the remaining models (same aggregators as main)
        aggregated_results = orchestrate_portfolio_aggregation(
            model_insights=reduced_insights,
            backtest_results=reduced_model_backtests,
            universe=config.universe,
            aggregators=config.aggregators,
        )

        # 2) Optimize (same optimizers as main)
        optimizer_weights = orchestrate_portfolio_optimizations(
            prices=prices,
            aggregated_insights=aggregated_results,
            universe=config.universe,
            optimizers=config.optimizers,
        )

        # 3) Simulate the optimized portfolios
        reduced_portfolio_backtests = orchestrate_portfolio_simulations(
            prices=prices,
            portfolio_insights=optimizer_weights,
            initial_value=getattr(config, "initial_value", 1_000_000.0),
        )

        # 4) Compare to MAIN optimizer-level sims
        model_results = create_marginal_df(config.optimizers, main_portfolio_backtests, reduced_portfolio_backtests)
        if model_results:
            results[removed] = model_results

    return results
