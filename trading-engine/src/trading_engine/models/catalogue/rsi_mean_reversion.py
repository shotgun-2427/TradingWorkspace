"""
RSI mean-reversion signal.

Classic 30/70 thresholds:
  RSI < oversold       → long  (+1)
  RSI > overbought     → flat or short (0 or -1)
  in between           → flat   (0)

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def RSIMeanReversionModel(
    trade_ticker: str,
    rsi_column: str,
    *,
    oversold: float = 30.0,
    overbought: float = 70.0,
    short_overbought: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    rsi_column : feature column holding the RSI value (0..100 scale)
    oversold : threshold below which we go long
    overbought : threshold above which we go flat (or short)
    short_overbought : if True, emit -1 when RSI > overbought
    """
    if not (0 < oversold < overbought < 100):
        raise ValueError(
            f"thresholds must satisfy 0 < oversold ({oversold}) < "
            f"overbought ({overbought}) < 100"
        )

    def run_model(lf: LazyFrame) -> LazyFrame:
        sig = (
            lf.filter(pl.col("ticker") == trade_ticker)
            .select(pl.col("date"), pl.col(rsi_column).alias("rsi"))
        )

        long_cond = pl.col("rsi") < oversold
        over_cond = pl.col("rsi") > overbought

        if short_overbought:
            weight = (
                pl.when(long_cond.fill_null(False)).then(pl.lit(1.0))
                .when(over_cond.fill_null(False)).then(pl.lit(-1.0))
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
