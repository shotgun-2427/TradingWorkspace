import polars as pl
from common.logging import setup_logger
from polars import LazyFrame
from typing import Callable, Dict
import numpy as np
import cvxpy as cp
from sklearn.covariance import LedoitWolf
from trading_engine.optimizers.catalogue.equal_weight import EqualWeightOptimizer

logger = setup_logger(__name__)

def MeanVarianceOptimizer(
    gamma: float = 1.0,
    lookback: int = 252,
    allow_short: bool = False,
    short_limit: float = -1.0,
    max_position: float = 1.0
) -> Callable[[Dict[str, LazyFrame], Dict], LazyFrame]:
    """
    Weights model insights by maximizing mean-variance utility: μ'w - γw'Σw.
    Uses Ledoit-Wolf shrinkage for covariance estimation over the last `lookback` days.
    Higher gamma = more risk aversion.

    Args:
      gamma: Risk aversion parameter (higher = more risk averse)
      lookback: Number of days to use for covariance estimation
      allow_short: If True, allows short positions. If False, adds w >= 0 constraint.
      short_limit: Maximum short weight per position (only used if allow_short=True)
      max_position: Maximum weight per model (default 1.0 = 100%)

    Inputs:
      model_insights: { model_name: LazyFrame(["date", ...tickers...]) }  (wide)
      backtest_results: { model_name: { "backtest_results": pl.DataFrame, ... }, ... }

    Output (RAW, wide):
      LazyFrame(["date", ...tickers...]) with per-ticker weights.
    """
    def run(model_insights: Dict[str, LazyFrame], backtest_results: Dict) -> LazyFrame:
        if not model_insights:
            return pl.DataFrame({"date": []}).lazy()

        models = list(model_insights.keys())

        # Check all models have sufficient data - fall back to equal weight if any missing
        for model in models:
            returns = backtest_results[model]["backtest_results"]["daily_return"]
            if len(returns) < lookback:
                logger.info(f"Model {model} has only {len(returns)} days of returns, needs {lookback} days. Falling back to EqualWeightOptimizer.")
                return EqualWeightOptimizer()(model_insights, backtest_results)

        # Expected returns
        mu = np.array([
            backtest_results[model]["backtest_results"]["daily_return"].mean()
            for model in models
        ])

        # Covariance matrix
        daily_returns = [
            backtest_results[model]["backtest_results"]["daily_return"].tail(lookback).to_numpy()
            for model in models
        ]
        
        X = np.column_stack(daily_returns)
        
        # Ledoit-Wolf shrinkage
        lw = LedoitWolf()
        Sigma = lw.fit(X).covariance_
        Sigma += np.eye(len(models)) * 1e-6

        # Optimization
        w = cp.Variable(len(mu))
        u = mu @ w - gamma * cp.quad_form(w, Sigma)
        obj = cp.Maximize(u)
        constraints = [cp.sum(w) == 1, w <= max_position]

        if allow_short:
            constraints.append(w >= short_limit)
        else:
            constraints.append(w >= 0)

        problem = cp.Problem(obj, constraints)
        problem.solve()

        # Clean weights
        weights = w.value
        if weights is None or np.any(np.isnan(weights)):
            raise ValueError("optimizer failed to converge")

        # Create model weights lookup
        model_weights_lf = pl.DataFrame({
            "model": models,
            "model_weight": weights
        }).lazy()

        # Combine model weights with ticker positions and aggregate
        combined_long_lf = (
            pl.concat([
                lf.unpivot(index="date", variable_name="ticker", value_name="position")
                  .with_columns(pl.lit(model).alias("model"))
                for model, lf in model_insights.items()
            ])
            .join(model_weights_lf, on="model", how="left")
            .with_columns((pl.col("position") * pl.col("model_weight")).alias("w"))
            .group_by(["date", "ticker"])
            .agg(pl.col("w").sum())
        )
        
        combined_wide_df = (
            combined_long_lf
            .collect(engine='streaming')  # materialize before pivot
            .pivot(values="w", index="date", on="ticker", aggregate_function="first")
            .sort("date")
        )
        return combined_wide_df.lazy()
    
    return run