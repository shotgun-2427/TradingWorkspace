"""
Low-volatility tilt model.

Long with weight inversely proportional to recent volatility, capped at 1.0.
Stays out of the market during high-vol periods.

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def LowVolatilityModel(
    trade_ticker: str,
    natr_column: str,
    *,
    target_natr: float = 1.0,
    max_weight: float = 1.0,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    natr_column : feature column with NATR (% units)
    target_natr : volatility level at which model is fully invested
    max_weight : cap on the produced weight (default 1.0)

    weight = min(target_natr / natr, max_weight)
    """
    if target_natr <= 0:
        raise ValueError("target_natr must be > 0")

    def run_model(lf: LazyFrame) -> LazyFrame:
        sig = (
            lf.filter(pl.col("ticker") == trade_ticker)
            .select(pl.col("date"), pl.col(natr_column).alias("vol"))
        )

        weight = (
            pl.when(pl.col("vol") <= 0).then(pl.lit(0.0))
            .otherwise(
                (pl.lit(target_natr) / pl.col("vol")).clip(0.0, max_weight)
            )
        )

        return sig.select(
            pl.col("date"),
            weight.cast(pl.Float64).alias(trade_ticker),
        )

    return run_model
