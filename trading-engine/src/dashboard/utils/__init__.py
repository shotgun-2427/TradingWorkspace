"""
Dashboard data loader exports.

Re-exports both:
- the workspace-native loaders (``build_equity_curve``, ``load_positions_snapshot``,
  ``build_holdings_table``, ``compute_curve_metrics``, ``load_home_dashboard_data``,
  …) used by the workspace-native screens.
- the engine-parity loaders required by the ported engine screens
  (``paper_performance``, ``backtest_charts``, ``portfolio_analytics``,
  ``slippage_analysis`` …).
"""

from .data_loaders import (  # workspace-native
    best_available_nav_history,
    build_equity_curve,
    build_holdings_table,
    compute_curve_metrics,
    get_paper_start_date,
    load_home_dashboard_data,
    load_latest_prices,
    load_positions_snapshot,
    synthesize_nav_history,
)
from .data_loaders import (  # engine-parity
    get_active_optimizer,
    get_actual_portfolio_attribution,
    get_actual_portfolio_trades,
    get_historical_nav,
    get_historical_weights,
    get_latest_actual_portfolio_audit,
    get_latest_audit_date,
    get_latest_portfolio_attribution_audit,
    get_latest_production_audit,
    get_latest_simulations_audit,
    get_local_active_optimizer_for_attribution,
    get_model_backtest,
    get_model_backtest_metrics,
    get_portfolio_backtest,
    get_portfolio_backtest_attribution,
    get_portfolio_backtest_metrics,
    get_portfolio_backtest_trades,
    get_position_data_for_date,
    get_production_audit_models,
    get_production_audit_optimizers,
    get_reduced_portfolio_backtest,
    get_spx_prices_from_date,
)

__all__ = [
    "best_available_nav_history",
    "build_equity_curve",
    "build_holdings_table",
    "compute_curve_metrics",
    "get_paper_start_date",
    "synthesize_nav_history",
    "get_active_optimizer",
    "get_actual_portfolio_attribution",
    "get_actual_portfolio_trades",
    "get_historical_nav",
    "get_historical_weights",
    "get_latest_actual_portfolio_audit",
    "get_latest_audit_date",
    "get_latest_portfolio_attribution_audit",
    "get_latest_production_audit",
    "get_latest_simulations_audit",
    "get_local_active_optimizer_for_attribution",
    "get_model_backtest",
    "get_model_backtest_metrics",
    "get_portfolio_backtest",
    "get_portfolio_backtest_attribution",
    "get_portfolio_backtest_metrics",
    "get_portfolio_backtest_trades",
    "get_position_data_for_date",
    "get_production_audit_models",
    "get_production_audit_optimizers",
    "get_reduced_portfolio_backtest",
    "get_spx_prices_from_date",
    "load_home_dashboard_data",
    "load_latest_prices",
    "load_positions_snapshot",
]
