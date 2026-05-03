"""
MACD (Moving Average Convergence Divergence) signal.

MACD = fast EMA - slow EMA
Signal line = EMA of MACD
Histogram = MACD - signal_line

Long when histogram > threshold, flat otherwise.

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def MACDModel(
    trade_ticker: str,
    macd_column: str,
    signal_column: str,
    *,
    threshold: float = 0.0,
    short_when_negative: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    macd_column : feature column holding MACD line (typically EMA12 - EMA26)
    signal_column : feature column holding the signal line (EMA9 of MACD)
    threshold : minimum histogram width to act on (filter out micro-crosses)
    short_when_negative : enable -1 weight when histogram below -threshold
    """

    def run_model(lf: LazyFrame) -> LazyFrame:
        sig = (
            lf.filter(pl.col("ticker") == trade_ticker)
            .select(
                pl.col("date"),
                (pl.col(macd_column) - pl.col(signal_column)).alias("hist"),
            )
        )

        long_cond = pl.col("hist") > threshold
        short_cond = pl.col("hist") < -threshold

        if short_when_negative:
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
