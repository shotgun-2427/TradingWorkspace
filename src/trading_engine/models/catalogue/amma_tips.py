from typing import Callable, Dict
import polars as pl
from polars import LazyFrame


def AMMA_regime(
    ticker: str,
    momentum_weights: Dict[int, float],
    regime_ticker: str,
    price_col: str,
    trend_window: int,
    confirm_days: int,
    direction: str,
    threshold: float = 0.0,
    long_enabled: bool = True,
    short_enabled: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:

    if direction not in {"down_is_good", "up_is_good"}:
        raise ValueError(
            "direction must be either 'down_is_good' or 'up_is_good'")

    def run_model(lf: LazyFrame) -> LazyFrame:
        # Ensure regime_ticker data exists
        regime_filter = lf.filter(
            pl.col("ticker") == regime_ticker).select(["date", price_col])
        if regime_filter.collect().height == 0:
            # No regime data available → default all regime signals to 1 (always allowed)
            lf = lf.with_columns(pl.lit(1).alias("regime_signal"))
        else:
            # Compute regime signal
            regime_df = regime_filter.with_columns(
                ((pl.col(price_col) / pl.col(price_col).shift(trend_window)) - 1.0)
                .alias("regime_trend")
            )

            if direction == "down_is_good":
                base_expr = pl.when(pl.col("regime_trend")
                                    <= 0).then(1).otherwise(0)
            else:
                base_expr = pl.when(pl.col("regime_trend")
                                    >= 0).then(1).otherwise(0)

            regime_df = regime_df.with_columns(
                base_expr.fill_null(0).alias("regime_raw"))

            if confirm_days > 1:
                regime_df = regime_df.with_columns(
                    pl.col("regime_raw")
                    .rolling_mean(window_size=confirm_days, min_periods=confirm_days)
                    .map_batches(lambda s: (s > 0.5).cast(pl.Int8), return_dtype=pl.Int8)
                    .fill_null(0)
                    .alias("regime_confirmed")
                )
            else:
                regime_df = regime_df.with_columns(
                    pl.col("regime_raw").alias("regime_confirmed"))

            regime_df = regime_df.with_columns(
                pl.col("regime_confirmed").shift(
                    1).fill_null(0).alias("regime_signal")
            )

            regime_df = regime_df.select(["date", "regime_signal"])
            lf = lf.join(regime_df, on="date", how="left")
            lf = lf.with_columns(pl.col("regime_signal").fill_null(
                1).cast(pl.Int8))  # <-- default 1, not 0

        # ------------------------------
        # Momentum + regime gating logic
        # ------------------------------
        sig_frames = []
        for window, weight in momentum_weights.items():
            colname = f"close_momentum_{window}"

            sig = (
                lf.filter(pl.col("ticker") == ticker)
                .select(["date", colname, "regime_signal"])
                .rename({colname: "sig"})
            )

            long_cond = (pl.col("sig") > threshold) if long_enabled else None
            short_cond = (pl.col("sig") < -
                          threshold) if short_enabled else None

            expr = pl.lit(0.0)
            if long_enabled and short_enabled:
                expr = (
                    pl.when((pl.col("regime_signal") == 1)
                            & long_cond.fill_null(False))
                    .then(pl.lit(1.0) * weight)
                    .when((pl.col("regime_signal") == 1) & short_cond.fill_null(False))
                    .then(pl.lit(-1.0) * weight)
                    .otherwise(0.0)
                )
            elif long_enabled:
                expr = (
                    pl.when((pl.col("regime_signal") == 1)
                            & long_cond.fill_null(False))
                    .then(pl.lit(1.0) * weight)
                    .otherwise(0.0)
                )
            elif short_enabled:
                expr = (
                    pl.when((pl.col("regime_signal") == 1)
                            & short_cond.fill_null(False))
                    .then(pl.lit(-1.0) * weight)
                    .otherwise(0.0)
                )

            weighted_sig = sig.select([
                pl.col("date"),
                expr.cast(pl.Float64).alias(f"sig_{window}")
            ])
            sig_frames.append(weighted_sig)

        if not sig_frames:
            return lf.select(["date"]).with_columns(pl.lit(0.0).alias(ticker))

        combined = sig_frames[0]
        for frame in sig_frames[1:]:
            combined = combined.join(frame, on="date", how="inner")

        weight_cols = [f"sig_{w}" for w in momentum_weights.keys()]
        final = combined.with_columns(
            sum(pl.col(c) for c in weight_cols).alias(ticker)
        ).select(["date", ticker])

        return final.fill_null(0.0)

    return run_model
