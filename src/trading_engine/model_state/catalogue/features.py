from polars import LazyFrame
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


def regime_signal_from_ticker(
    regime_ticker: str,
    price_col: str,
    dest_col: str,
    trend_window: int,
    confirm_days: int,
    direction: str,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Compute a binary regime signal based on the price history of a specific ticker (e.g. TIP-US),
    and join that signal back to all rows in the model state by date.

    Logic:
      - Filter rows where ticker == regime_ticker.
      - Compute % change over `trend_window`.
      - direction='down_is_good' → signal=1 if trend<=0 else 0  (TIP logic)
      - direction='up_is_good'   → signal=1 if trend>=0 else 0
      - Apply confirmation smoothing over `confirm_days`.
      - Shift by 1 day to remove lookahead.
      - Default signal=0 when insufficient data.
      - Join the resulting signal back to *all* tickers by date.

    Parameters
    ----------
    regime_ticker : str
        The ticker whose price defines the regime (e.g. 'TIP-US').
    price_col : str
        Column with adjusted close or source prices (e.g. 'adjusted_close_1d').
    dest_col : str
        Column name to store the computed signal (e.g. 'tip_signal').
    trend_window : int
        Lookback window for long-term percent change.
    confirm_days : int
        Window for regime persistence confirmation.
    direction : {'down_is_good', 'up_is_good'}
        Determines which direction indicates a favorable regime.

    Returns
    -------
    Callable[[LazyFrame], LazyFrame]
        Transformation that adds the regime column to the model state.
    """
    if direction not in {"down_is_good", "up_is_good"}:
        raise ValueError("direction must be 'down_is_good' or 'up_is_good'")

    def transform(df: LazyFrame) -> LazyFrame:
        # --- Step 1: Extract TIP (or other regime) rows only ---
        regime_df = df.filter(pl.col("ticker") == regime_ticker).select(
            ["date", price_col])

        # --- Step 2: Compute long-term percent change trend ---
        regime_df = regime_df.with_columns(
            ((pl.col(price_col) / pl.col(price_col).shift(trend_window)) - 1.0)
            .alias("regime_trend")
        )

        # --- Step 3: Direction logic ---
        if direction == "down_is_good":
            base_expr = pl.when(pl.col("regime_trend") <=
                                0).then(1).otherwise(0)
        else:
            base_expr = pl.when(pl.col("regime_trend") >=
                                0).then(1).otherwise(0)

        regime_df = regime_df.with_columns(
            base_expr.fill_null(0).alias("regime_raw"))

        # --- Step 4: Confirmation smoothing ---
        if confirm_days > 1:
            regime_df = regime_df.with_columns(
                pl.col("regime_raw")
                .rolling_mean(window_size=confirm_days, min_periods=confirm_days)
                .map_batches(lambda s: (s > 0.5).cast(pl.Int8))
                .fill_null(0)
                .alias("regime_confirmed")
            )
        else:
            regime_df = regime_df.with_columns(
                pl.col("regime_raw").alias("regime_confirmed"))

        # --- Step 5: Shift to avoid lookahead ---
        regime_df = regime_df.with_columns(
            pl.col("regime_confirmed")
            .shift(1)
            .fill_null(0)
            .alias(dest_col)
        )

        # --- Step 6: Keep only date and final signal ---
        regime_df = regime_df.select(["date", dest_col])

        # --- Step 7: Join regime signal back to all rows ---
        df = df.join(regime_df, on="date", how="left")

        # Default missing signal to 0 (dates before regime exists)
        df = df.with_columns(pl.col(dest_col).fill_null(0).cast(pl.Int8))

        return df

    return transform
