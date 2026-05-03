"""
Trend-strength signal — only takes positions when the trend is strong.

Combines two ideas:
  1. **direction**: sign of medium-term momentum
  2. **strength**: ratio of medium momentum to short-term volatility
     (a Sharpe-like trend-quality measure)

Position size scales with strength × direction, capped to ±max_weight.

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def TrendStrengthModel(
    trade_ticker: str,
    momentum_column: str,
    vol_column: str,
    *,
    min_strength: float = 0.5,
    weight_scale: float = 0.5,
    max_weight: float = 1.0,
    allow_short: bool = True,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    momentum_column : feature column with cumulative return over a window
    vol_column : volatility / NATR column over a similar window
    min_strength : abs(strength) below this → flat
    weight_scale : weight = strength * weight_scale, then capped
    allow_short : enable -1 weights for negative trends
    """

    def run_model(lf: LazyFrame) -> LazyFrame:
        sig = (
            lf.filter(pl.col("ticker") == trade_ticker)
            .select(
                pl.col("date"),
                pl.col(momentum_column).alias("mom"),
                pl.col(vol_column).alias("vol"),
            )
        )

        strength = (
            pl.when(pl.col("vol") > 0)
            .then(pl.col("mom") / pl.col("vol"))
            .otherwise(0.0)
        )

        if allow_short:
            base = strength * weight_scale
            weight = (
                pl.when(strength.abs() >= min_strength)
                .then(base.clip(-max_weight, max_weight))
                .otherwise(0.0)
            )
        else:
            base = (strength * weight_scale).clip(0.0, max_weight)
            weight = (
                pl.when(strength >= min_strength).then(base).otherwise(0.0)
            )

        return sig.select(
            pl.col("date"),
            weight.cast(pl.Float64).alias(trade_ticker),
        )

    return run_model
