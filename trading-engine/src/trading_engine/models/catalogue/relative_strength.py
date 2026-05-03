"""
Cross-asset relative strength.

Compares the trade ticker's momentum to a benchmark ticker:
  ratio = mom(trade) - mom(benchmark)
  long when ratio > threshold

Useful for "this asset is outperforming SPY → tilt toward it" patterns.

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def RelativeStrengthModel(
    trade_ticker: str,
    benchmark_ticker: str,
    momentum_column: str,
    *,
    threshold: float = 0.0,
    short_below: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    trade_ticker : asset whose weight this model produces
    benchmark_ticker : reference asset (e.g. SPY-US)
    momentum_column : feature column on both tickers
    threshold : minimum outperformance to act on
    short_below : emit -1 when underperforming benchmark
    """

    def run_model(lf: LazyFrame) -> LazyFrame:
        trade = (
            lf.filter(pl.col("ticker") == trade_ticker)
            .select(pl.col("date"), pl.col(momentum_column).alias("mom_t"))
        )
        bench = (
            lf.filter(pl.col("ticker") == benchmark_ticker)
            .select(pl.col("date"), pl.col(momentum_column).alias("mom_b"))
        )
        joined = trade.join(bench, on="date", how="inner").select(
            pl.col("date"),
            (pl.col("mom_t") - pl.col("mom_b")).alias("rel"),
        )

        long_cond = pl.col("rel") > threshold
        short_cond = pl.col("rel") < -threshold

        if short_below:
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

        return joined.select(
            pl.col("date"),
            weight.cast(pl.Float64).alias(trade_ticker),
        )

    return run_model
