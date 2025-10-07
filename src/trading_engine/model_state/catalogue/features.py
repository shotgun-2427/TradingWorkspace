from typing import Callable

import polars as pl
from polars import LazyFrame, DataFrame


def moving_average(source_col: str, dest_col: str, window: int) -> Callable[[LazyFrame], LazyFrame]:
    def transform(df: LazyFrame) -> LazyFrame:
        return df.with_columns(
            pl.col(source_col)
            .rolling_mean(window_size=window)
            .over("ticker")
            .alias(dest_col)
        )

    return transform


def momentum(source_col: str, dest_col: str, window: int) -> Callable[[LazyFrame], LazyFrame]:
    def transform(df: LazyFrame) -> LazyFrame:
        return df.with_columns(
            (pl.col(source_col) - pl.col(source_col).shift(window)
             ).over("ticker").alias(dest_col)
        )

    return transform


def rsi(source_col: str, dest_col: str, window: int) -> Callable[[DataFrame], DataFrame]:
    def transform(df: DataFrame) -> DataFrame:
        df = df.sort(["ticker", "date"])
        df = df.with_columns([
            (pl.col(source_col) - pl.col(source_col).shift(1)).alias("delta")
        ])

        df = df.with_columns([
            pl.when(pl.col("delta") > 0).then(
                pl.col("delta")).otherwise(0.0).alias("gain"),
            pl.when(pl.col("delta") < 0).then(-pl.col("delta")
                                              ).otherwise(0.0).alias("loss"),
        ])

        df = df.with_columns([
            pl.col("gain").rolling_mean(window_size=window).over(
                "ticker").alias("avg_gain"),
            pl.col("loss").rolling_mean(window_size=window).over(
                "ticker").alias("avg_loss"),
        ])

        df = df.with_columns([
            (100 - (100 / (1 + (pl.col("avg_gain") / pl.col("avg_loss"))))).alias(dest_col)
        ])

        return df.drop(["delta", "gain", "loss", "avg_gain", "avg_loss"])

    return transform


def natr(
        high_col: str,
        low_col: str,
        close_col: str,
        dest_col: str,
        period: int,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Wilder-style NATR using EWM(α=1/period) per ticker (lazy).
    Adds `dest_col` to the frame. Assumes rows are sorted by ['ticker','date'].

    NATR = 100 * EWM(TR, alpha=1/period) / close
    TR   = max( high-low, |high - close.shift(1)|, |low - close.shift(1)| )
    """
    alpha = 1.0 / float(period)

    def transform(lf: LazyFrame) -> LazyFrame:
        tr = pl.max_horizontal([
            pl.col(high_col) - pl.col(low_col),
            (pl.col(high_col) - pl.col(close_col).shift(1)).abs(),
            (pl.col(low_col) - pl.col(close_col).shift(1)).abs(),
        ])

        atr = tr.ewm_mean(alpha=alpha).over("ticker")
        natr = ((atr / pl.col(close_col)) * 100.0).alias(dest_col)

        return lf.with_columns(natr)

    return transform


def tip_regime_signal(
    source_col: str,
    dest_col: str,
    trend_window: int = 252,
    confirm_days: int = 40,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Generate TIP regime binary signal for inflation-trend filtering.

    Logic:
      - Compute long-term % change in TIP price over `trend_window`.
      - If trend <= 0 ⇒ inflation cooling ⇒ ALLOW (1)
      - If trend > 0 ⇒ inflation rising ⇒ AVOID (0)
      - Apply confirmation smoothing over `confirm_days`.
      - Shift by 1 to remove lookahead bias.
      - If insufficient data for either step ⇒ default to 0.

    Parameters
    ----------
    source_col : str
        Column name for TIP ETF adjusted close (e.g. 'tips_close_1d').
    dest_col : str
        Destination column for computed binary signal.
    trend_window : int
        Days for long-term trend.
    confirm_days : int
        Days for confirmation smoothing.

    Returns
    -------
    Callable[[LazyFrame], LazyFrame]
        Transformation that adds a new column with TIP regime signal.
    """
    def transform(df: LazyFrame) -> LazyFrame:
        # Compute long-term percent change (trend)
        df = df.with_columns(
            (
                (pl.col(source_col) / pl.col(source_col).shift(trend_window) - 1.0)
                .alias("tip_trend")
            )
        )

        # Base regime: 1 if trend <= 0 (inflation cooling), else 0
        df = df.with_columns(
            (pl.when(pl.col("tip_trend") <= 0)
             .then(1)
             .otherwise(0)
             .fill_null(0)
             .alias("tip_regime_raw"))
        )

        # Confirmation smoothing — require regime persistence
        if confirm_days > 1:
            df = df.with_columns(
                pl.col("tip_regime_raw")
                .rolling_mean(window_size=confirm_days, min_periods=confirm_days)
                .over("ticker")
                .map_batches(lambda s: (s > 0.5).cast(pl.Int8))
                .fill_null(0)
                .alias("tip_regime_confirmed")
            )
        else:
            df = df.with_columns(
                pl.col("tip_regime_raw").alias("tip_regime_confirmed")
            )

        # Shift by 1 day to prevent lookahead; fill nulls with 0
        df = df.with_columns(
            pl.col("tip_regime_confirmed")
            .shift(1)
            .fill_null(0)
            .alias(dest_col)
        )

        # Clean up intermediates
        return df.drop(["tip_trend", "tip_regime_raw", "tip_regime_confirmed"])

    return transform
