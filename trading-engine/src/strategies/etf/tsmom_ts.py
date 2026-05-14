"""
Time-series momentum (TSMOM).

This is the **time-series** counterpart to the cross-sectional ``momentum``
signal — instead of asking "is this ETF outperforming the other ETFs?", it
asks "has this ETF gone up over the last 12 months?". The classic finding
(Moskowitz, Ooi, Pedersen 2012) is that 12-1 time-series momentum is a
robust risk premium across asset classes, and on a single ETF it's the
simplest "trend follower's" rule.

For each symbol, at ``as_of``:
    ret_12_1 = price_t / price_{t - 252 + skip} - 1   (skip last 21d ≈ 1mo)

If ``ret_12_1`` > ``threshold`` → fully invested. Else → flat.

We emit a constant ``score`` in ``{0.0, 2.0}`` so the ``_score_to_weight``
clip-to-[0,1] in ``per_etf_backtest`` resolves to ``{0, 1}``.

Defaults: lookback=252 (≈12 months), skip=21 (≈1 month), threshold=0.0.
"""
from __future__ import annotations

import pandas as pd

from src.strategies.etf._panel import (
    eligible_columns,
    slice_history,
    to_wide,
)


class TSMomentum:
    name = "tsmom_ts"
    kind = "absolute"  # time-series; do not z-score in ensembles

    def __init__(self, lookback: int = 252, skip: int = 21, threshold: float = 0.0):
        self.lookback = int(lookback)
        self.skip = int(max(0, skip))
        # Threshold lets you require a *positive* momentum of at least this
        # much before going long (e.g., 0.02 = need >2% trailing return).
        # Keep at 0.0 for the classic "any positive trend" rule.
        self.threshold = float(threshold)
        # Need lookback + a tiny buffer.
        self.lookback_days_required = self.lookback + 5

    def compute(self, prices: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
        wide = to_wide(prices)
        # Want full lookback + skip + buffer of bars on or before as_of.
        hist = slice_history(wide, as_of, lookback=self.lookback + self.skip + 5)
        symbols = eligible_columns(hist, min_history=self.lookback + self.skip)
        if not symbols:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        h = hist[symbols].ffill()
        end_idx = -1 - self.skip if self.skip < len(h) else -1
        end = h.iloc[end_idx]
        start_idx = max(0, len(h) + end_idx - self.lookback) if end_idx < 0 else 0
        start = h.iloc[start_idx]
        raw = (end / start) - 1.0
        raw = raw.replace([float("inf"), float("-inf")], pd.NA).dropna()
        if raw.empty:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        score = (raw > self.threshold).astype(float) * 2.0
        return pd.DataFrame({
            "symbol": raw.index.tolist(),
            "raw": raw.values,
            "score": score.values,
        })
