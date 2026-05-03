"""
Volatility-targeting model.

Sizes the position so that asset volatility × weight equals a target
portfolio-level volatility.

  weight = target_vol / asset_vol  (capped at max_weight)
  ×  sign(momentum) if directional, else always long

Used to make different assets contribute equally to portfolio risk.

Output columns: ["date", trade_ticker]
"""
from typing import Callable

import polars as pl
from polars import LazyFrame


def VolatilityTargetModel(
    trade_ticker: str,
    vol_column: str,
    *,
    target_vol: float = 0.10,
    momentum_column: str | None = None,
    max_weight: float = 1.0,
    allow_short: bool = False,
) -> Callable[[LazyFrame], LazyFrame]:
    """
    Parameters
    ----------
    vol_column : trailing realized vol column (annualized; e.g. NATR).
                 Same units as ``target_vol``.
    target_vol : annualized vol target the model sizes toward.
    momentum_column : if set, weight is multiplied by sign(momentum).
                      Otherwise model is always long.
    allow_short : when ``momentum_column`` is set, allow negative weights.
    """
    if target_vol <= 0:
        raise ValueError("target_vol must be > 0")

    def run_model(lf: LazyFrame) -> LazyFrame:
        cols = [pl.col("date"), pl.col(vol_column).alias("vol")]
        if momentum_column is not None:
            cols.append(pl.col(momentum_column).alias("mom"))

        sig = lf.filter(pl.col("ticker") == trade_ticker).select(cols)

        size = (
            pl.when(pl.col("vol") > 0)
            .then((pl.lit(target_vol) / pl.col("vol")).clip(0.0, max_weight))
            .otherwise(0.0)
        )

        if momentum_column is not None:
            direction = (
                pl.when(pl.col("mom") > 0).then(1.0)
                .when(pl.col("mom") < 0).then(-1.0 if allow_short else 0.0)
                .otherwise(0.0)
            )
            weight = size * direction
        else:
            weight = size

        return sig.select(
            pl.col("date"),
            weight.cast(pl.Float64).alias(trade_ticker),
        )

    return run_model
