"""
Dual moving-average crossover.

Long when fast MA > slow MA, flat (or short) otherwise.

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def DualMACrossoverModel(
    trade_ticker: str,
    fast_ma_column: str,
    slow_ma_column: str,
    *,
    short_when_below: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    trade_ticker : symbol whose weight this model produces
    fast_ma_column, slow_ma_column : feature columns from model_state
        (e.g. ``close_ma_10`` and ``close_ma_50``)
    short_when_below : if True, weight is +1 / 0 / -1 (long/flat/short).
        If False, weight is +1 / 0 (long/flat).
    """

    def run_model(lf: LazyFrame) -> LazyFrame:
        sig = (
            lf.filter(pl.col("ticker") == trade_ticker)
            .select(
                pl.col("date"),
                pl.col(fast_ma_column).alias("fast"),
                pl.col(slow_ma_column).alias("slow"),
            )
        )

        long_cond = pl.col("fast") > pl.col("slow")
        short_cond = pl.col("fast") < pl.col("slow")

        if short_when_below:
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
