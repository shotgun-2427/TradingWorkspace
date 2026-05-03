"""
Quality factor — Sharpe-driven allocation.

Reads trailing returns and volatility for an asset, computes a Sharpe-like
ratio, and produces a weight proportional to that ratio (long when
positive, scaled by Sharpe magnitude).

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def QualitySharpeModel(
    trade_ticker: str,
    return_column: str,
    vol_column: str,
    *,
    weight_per_unit_sharpe: float = 0.5,
    max_weight: float = 1.0,
    min_sharpe: float = 0.0,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    return_column : trailing return feature column (e.g. close_momentum_60)
    vol_column : trailing volatility feature column (NATR or rolling std)
    weight_per_unit_sharpe : weight scaling factor (Sharpe * scale → weight)
    max_weight : cap
    min_sharpe : threshold below which we go flat
    """

    def run_model(lf: LazyFrame) -> LazyFrame:
        sig = (
            lf.filter(pl.col("ticker") == trade_ticker)
            .select(
                pl.col("date"),
                pl.col(return_column).alias("ret"),
                pl.col(vol_column).alias("vol"),
            )
        )

        sharpe = (
            pl.when(pl.col("vol") > 0)
            .then(pl.col("ret") / pl.col("vol"))
            .otherwise(0.0)
        )

        weight = (
            pl.when(sharpe > min_sharpe)
            .then((sharpe * weight_per_unit_sharpe).clip(0.0, max_weight))
            .otherwise(0.0)
        )

        return sig.select(
            pl.col("date"),
            weight.cast(pl.Float64).alias(trade_ticker),
        )

    return run_model
