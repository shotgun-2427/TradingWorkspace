"""
Inverse-Momentum Mean Reversion signal.

Combines two opposing horizons:
  * medium-term momentum (positive contribution): we want long-term winners
  * short-term reversal (negative contribution): we want recent dippers

raw = mom_long - alpha * short_return
  where mom_long  = price_t / price_{t-126} - 1
        short_return = price_t / price_{t-5}  - 1

So an asset that's been steadily up for 6 months but has dipped in the last
week scores higher than one that's also up in the last week (which we'd
expect to mean-revert down).

score = cross-sectional z-score of `raw`.

Defaults: long_lookback=126, short_lookback=5, alpha=3.0.
"""
from __future__ import annotations

import pandas as pd

from src.strategies import zscore
from src.strategies.etf._panel import (
    eligible_columns,
    slice_history,
    to_wide,
)


class InverseMomentumMeanReversion:
    name = "inverse_momentum_mean_reversion"

    def __init__(
        self,
        long_lookback: int = 126,
        short_lookback: int = 5,
        alpha: float = 3.0,
    ):
        self.long_lookback = int(long_lookback)
        self.short_lookback = int(short_lookback)
        self.alpha = float(alpha)
        self.lookback_days_required = self.long_lookback + 1

    def compute(
        self,
        prices: pd.DataFrame,
        as_of: pd.Timestamp,
    ) -> pd.DataFrame:
        wide = to_wide(prices)
        hist = slice_history(wide, as_of, lookback=self.long_lookback + 5)
        symbols = eligible_columns(hist, min_history=self.long_lookback)
        if not symbols:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        h = hist[symbols].ffill()
        last = h.iloc[-1]
        long_ago = h.iloc[-1 - self.long_lookback] if len(h) > self.long_lookback else h.iloc[0]
        short_ago = h.iloc[-1 - self.short_lookback] if len(h) > self.short_lookback else h.iloc[0]

        mom_long = (last / long_ago) - 1.0
        short_ret = (last / short_ago) - 1.0
        raw = mom_long - self.alpha * short_ret
        raw = raw.replace([float("inf"), float("-inf")], pd.NA).dropna()

        if raw.empty:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        out = pd.DataFrame({
            "symbol": raw.index.tolist(),
            "raw": raw.values,
        })
        out["score"] = zscore(out["raw"])
        return out
