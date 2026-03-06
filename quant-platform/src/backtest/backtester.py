from __future__ import annotations

import pandas as pd

from src.backtest.metrics import compute_metrics


def run_backtest(prices_df: pd.DataFrame, weights_df: pd.DataFrame, initial_capital: float = 100000) -> dict:
    pivot = prices_df.pivot(index="date", columns="symbol", values="close").sort_index()
    returns = pivot.pct_change().fillna(0)

    if "date" in weights_df.columns:
        latest_weights = (
            weights_df.sort_values("date").groupby("symbol").tail(1).set_index("symbol")["weight"]
        )
    else:
        latest_weights = weights_df.set_index("symbol")["weight"]

    aligned = latest_weights.reindex(returns.columns).fillna(0)
    daily_returns = returns.mul(aligned, axis=1).sum(axis=1)
    equity_curve = (1 + daily_returns).cumprod() * initial_capital
    drawdown = equity_curve / equity_curve.cummax() - 1
    metrics = compute_metrics(daily_returns, drawdown)

    return {
        "daily_returns": daily_returns,
        "equity_curve": equity_curve,
        "drawdown": drawdown,
        "metrics": metrics,
    }
