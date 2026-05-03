"""
Momentum signal — classic 12-1 (or N-skip) total-return momentum.

raw     = price_t / price_{t-lookback} - 1, with an optional `skip` window
          subtracted so we use returns from t-lookback to t-skip (avoids the
          short-term reversal contamination Jegadeesh & Titman flagged).
score   = cross-sectional z-score of `raw`.

Defaults: lookback=126, skip=5 (~6 months excluding last week).
"""
from __future__ import annotations

import pandas as pd

from src.strategies import zscore
from src.strategies.etf._panel import (
    eligible_columns,
    slice_history,
    to_wide,
)


class MomentumSignal:
    name = "momentum"

    def __init__(self, lookback: int = 126, skip: int = 5):
        self.lookback = int(lookback)
        self.skip = int(max(0, skip))
        self.lookback_days_required = self.lookback + 1

    def compute(
        self,
        prices: pd.DataFrame,
        as_of: pd.Timestamp,
    ) -> pd.DataFrame:
        wide = to_wide(prices)
        hist = slice_history(wide, as_of, lookback=self.lookback)
        symbols = eligible_columns(hist, min_history=self.lookback)
        if not symbols:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        h = hist[symbols].ffill()
        # End point: skip the last `skip` observations to remove short-term
        # reversal contamination. If skip=0, this is just the most recent close.
        end_idx = -1 - self.skip if self.skip < len(h) else -1
        end = h.iloc[end_idx]
        # Start point: lookback observations before `end`.
        start_pos = max(0, len(h) + (end_idx if end_idx < 0 else 0) - self.lookback)
        start = h.iloc[start_pos]
        raw = (end / start) - 1.0
        raw = raw.replace([float("inf"), float("-inf")], pd.NA).dropna()

        out = pd.DataFrame({
            "symbol": raw.index.tolist(),
            "raw": raw.values,
        })
        out["score"] = zscore(out["raw"])
        return out
