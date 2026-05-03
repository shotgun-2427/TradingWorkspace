"""
Bollinger-band mean-reversion signal.

Long when price < lower band (price - SMA) / std < -k_std
Short / flat above upper band.

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def BollingerBandModel(
    trade_ticker: str,
    price_column: str,
    sma_column: str,
    std_column: str,
    *,
    k_std: float = 2.0,
    short_above_upper: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    price_column : current price column (e.g. ``adjusted_close_1d``)
    sma_column : moving-average feature column
    std_column : rolling std feature column
    k_std : band width in stdev units (typical: 2.0)
    short_above_upper : emit -1 when above upper band
    """

    def run_model(lf: LazyFrame) -> LazyFrame:
        sig = (
            lf.filter(pl.col("ticker") == trade_ticker)
            .select(
                pl.col("date"),
                (
                    (pl.col(price_column) - pl.col(sma_column))
                    / pl.col(std_column)
                ).alias("z"),
            )
        )

        long_cond = pl.col("z") < -k_std
        upper_cond = pl.col("z") > k_std

        if short_above_upper:
            weight = (
                pl.when(long_cond.fill_null(False)).then(pl.lit(1.0))
                .when(upper_cond.fill_null(False)).then(pl.lit(-1.0))
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
