from typing import Callable, Dict
import polars as pl
from polars import LazyFrame


def AMMA_TIP(
        ticker: str,
        momentum_weights: Dict[int, float],  # {window: weight}
        threshold: float = 0.0,
        long_enabled: bool = True,
        short_enabled: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Adaptive Momentum Model Averaging (AMMA) with TIP regime filter.

    This behaves the same as AMMA(), except that all momentum signals are
    gated by the binary 'tip_signal' feature from the model state.
    (1 = allow trading, 0 = avoid trading)

    Expected columns in model state:
      - close_momentum_<N>  (e.g. close_momentum_20)
      - tip_signal
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
        # Ensure the TIP signal column exists; if not, default to 0 (avoid)
        if "tip_signal" not in lf.columns:
            lf = lf.with_columns(pl.lit(0).alias("tip_signal"))
        else:
            lf = lf.with_columns(
                pl.col("tip_signal").fill_null(0).cast(pl.Int8))

        sig_frames = []

        for window, weight in momentum_weights.items():
            colname = f"close_momentum_{window}"

            sig = (
                lf.filter(pl.col("ticker") == ticker)
                .select(["date", colname, "tip_signal"])
                .rename({colname: "sig"})
            )

            # Base signal logic
            long_cond = (pl.col("sig") > threshold) if long_enabled else None
            short_cond = (pl.col("sig") < -
                          threshold) if short_enabled else None

            expr = pl.lit(0.0)

            if long_enabled and short_enabled:
                expr = (
                    pl.when((pl.col("tip_signal") == 1)
                            & long_cond.fill_null(False))
                    .then(pl.lit(1.0) * weight)
                    .when((pl.col("tip_signal") == 1) & short_cond.fill_null(False))
                    .then(pl.lit(-1.0) * weight)
                    .otherwise(pl.lit(0.0))
                )
            elif long_enabled:
                expr = (
                    pl.when((pl.col("tip_signal") == 1)
                            & long_cond.fill_null(False))
                    .then(pl.lit(1.0) * weight)
                    .otherwise(pl.lit(0.0))
                )
            elif short_enabled:
                expr = (
                    pl.when((pl.col("tip_signal") == 1) &
                            short_cond.fill_null(False))
                    .then(pl.lit(-1.0) * weight)
                    .otherwise(pl.lit(0.0))
                )

            weighted_sig = sig.select([
                pl.col("date"),
                expr.cast(pl.Float64).alias(f"sig_{window}")
            ])

            sig_frames.append(weighted_sig)

        # Join on date
        combined = sig_frames[0]
        for frame in sig_frames[1:]:
            combined = combined.join(frame, on="date", how="inner")

        # Sum across signals and produce final weight column
        weight_cols = [f"sig_{w}" for w in momentum_weights.keys()]
        final = combined.with_columns(
            sum(pl.col(c) for c in weight_cols).alias(ticker)
        ).select(["date", ticker])

        return final.fill_null(0.0)

    return run_model
