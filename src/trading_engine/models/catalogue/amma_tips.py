from typing import Callable, Dict

import polars as pl
from polars import LazyFrame


def AMMA_TIP(
        ticker: str,
        momentum_weights: Dict[int, float],
        threshold: float = 0.0,
        long_enabled: bool = True,
        short_enabled: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Adaptive Momentum Model Averaging (AMMA) with TIP regime filter.

    This version behaves identically to AMMA(), except that
    each momentum signal is multiplied by the TIP regime column (`tip_signal`),
    which acts as a binary gate: 1 = allow trades, 0 = avoid trades.

    Expected model state columns:
      - close_momentum_<window>  (e.g. close_momentum_20)
      - tip_signal               (from tip_regime_signal feature)
      - ticker, date

    Parameters
    ----------
    ticker : str
        The trade ticker (column name for output weights).
    momentum_weights : dict[int, float]
        Mapping of momentum windows to their assigned weights.
    threshold : float, optional
        Momentum threshold for signal generation.
    long_enabled : bool, optional
        If True, allow long signals when momentum > threshold.
    short_enabled : bool, optional
        If True, allow short signals when momentum < -threshold.

    Returns
    -------
    Callable[[LazyFrame], LazyFrame]
        Function that takes a LazyFrame and returns a LazyFrame of weights.
    """

    def run_model(lf: LazyFrame) -> LazyFrame:
        # Ensure tip_signal exists and fill missing with 0 (avoid)
        if "tip_signal" not in lf.columns:
            lf = lf.with_columns(pl.lit(0).alias("tip_signal"))
        else:
            lf = lf.with_columns(
                pl.col("tip_signal").fill_null(0).cast(pl.Int8))

        sig_frames = []

        for window, weight in momentum_weights.items():
            mom_col = f"close_momentum_{window}"

            # Skip if momentum column missing
            if mom_col not in lf.columns:
                continue

            sig = (
                lf.filter(pl.col("ticker") == ticker)
                .select(["date", mom_col, "tip_signal"])
                .rename({mom_col: "sig"})
            )

            # Basic long/short conditions
            long_cond = (pl.col("sig") > threshold) if long_enabled else None
            short_cond = (pl.col("sig") < -
                          threshold) if short_enabled else None

            # Default signal = 0
            expr = pl.lit(0.0)

            # Momentum logic gated by tip_signal
            if long_enabled and short_enabled:
                expr = (
                    pl.when((pl.col("tip_signal") == 1)
                            & long_cond.fill_null(False))
                    .then(weight)
                    .when((pl.col("tip_signal") == 1) & short_cond.fill_null(False))
                    .then(-weight)
                    .otherwise(0.0)
                )
            elif long_enabled:
                expr = (
                    pl.when((pl.col("tip_signal") == 1)
                            & long_cond.fill_null(False))
                    .then(weight)
                    .otherwise(0.0)
                )
            elif short_enabled:
                expr = (
                    pl.when((pl.col("tip_signal") == 1) &
                            short_cond.fill_null(False))
                    .then(-weight)
                    .otherwise(0.0)
                )

            weighted_sig = sig.with_columns(
                expr.cast(pl.Float64).alias(f"sig_{window}")
            ).select(["date", f"sig_{window}"])

            sig_frames.append(weighted_sig)

        # Combine all window signals
        if not sig_frames:
            # No valid signals → zero output
            return lf.select(["date"]).with_columns(pl.lit(0.0).alias(ticker))

        combined = sig_frames[0]
        for frame in sig_frames[1:]:
            combined = combined.join(frame, on="date", how="outer")

        # Fill missing signal values with 0
        combined = combined.fill_null(0.0)

        # Sum across weighted signals
        weight_cols = [f"sig_{w}" for w in momentum_weights.keys()]
        final = combined.with_columns(
            sum(pl.col(c) for c in weight_cols).alias(ticker)
        ).select(["date", ticker])

        return final.fill_null(0.0)

    return run_model
