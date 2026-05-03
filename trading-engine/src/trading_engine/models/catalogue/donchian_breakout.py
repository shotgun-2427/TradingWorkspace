"""
Donchian channel breakout — classic trend-following signal.

  Long when price > rolling N-day high
  Flat (or short) when price < rolling N-day low
  Held position otherwise

Standard "Turtle traders" rule with N=20 (entry) / N=10 (exit).

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def DonchianBreakoutModel(
    trade_ticker: str,
    price_column: str,
    high_column: str,
    low_column: str,
    *,
    short_on_low_break: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    price_column : current price column
    high_column : feature column with the rolling channel high
    low_column : feature column with the rolling channel low
    short_on_low_break : emit -1 when breaking below the rolling low
    """

    def run_model(lf: LazyFrame) -> LazyFrame:
        sig = (
            lf.filter(pl.col("ticker") == trade_ticker)
            .select(
                pl.col("date"),
                pl.col(price_column).alias("px"),
                pl.col(high_column).alias("hi"),
                pl.col(low_column).alias("lo"),
            )
        )

        long_cond = pl.col("px") >= pl.col("hi")
        short_cond = pl.col("px") <= pl.col("lo")

        if short_on_low_break:
            weight = (
                pl.when(long_cond.fill_null(False)).then(pl.lit(1.0))
                .when(short_cond.fill_null(False)).then(pl.lit(-1.0))
                .otherwise(pl.lit(0.0))
            )
        else:
            weight = (
                pl.when(long_cond.fill_null(False)).then(pl.lit(1.0))
                .otherwise(pl.lit(0.0))
            )

        return sig.select(
            pl.col("date"),
            weight.cast(pl.Float64).alias(trade_ticker),
        )

    return run_model
