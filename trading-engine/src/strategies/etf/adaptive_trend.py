"""
Adaptive trend — multi-window trend filter, fractional exposure.

A single ``trend_filter`` window (e.g. 200-day SMA) is fragile: a short
window whipsaws in choppy markets, a long window is slow to step aside in
a regime change. The "right" window is asset-and-period dependent.

``adaptive_trend`` sidesteps the choice by running three trend filters in
parallel (50/100/200 day SMAs) and averaging their on/off signal — so
weight steps smoothly through {0, 1/3, 2/3, 1} as each window flips. In
practice this delivers most of the trend-following premium of a single
window while reducing turnover and whipsaw, and almost always trades the
peak Sharpe of any individual window for higher robustness across assets.

Like the other time-series signals it emits a constant ``raw`` (the
fraction of windows currently saying "long") and a ``score`` in
[0.0, 2.0]. ``_score_to_weight`` clip-to-[0,1] resolves to the exposure.

Defaults: windows=(50, 100, 200).
"""
from __future__ import annotations

import pandas as pd

from src.strategies.etf._panel import (
    eligible_columns,
    slice_history,
    to_wide,
)


class AdaptiveTrend:
    name = "adaptive_trend"
    kind = "absolute"  # time-series; do not z-score in ensembles

    def __init__(self, windows: tuple[int, ...] = (50, 100, 200)):
        if not windows:
            raise ValueError("AdaptiveTrend needs at least one window")
        # Sort + dedup so the per-window iteration is deterministic.
        self.windows = tuple(sorted({int(w) for w in windows}))
        self.lookback_days_required = max(self.windows) + 5

    def compute(self, prices: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
        wide = to_wide(prices)
        max_window = max(self.windows)
        hist = slice_history(wide, as_of, lookback=max_window + 5)
        symbols = eligible_columns(hist, min_history=max_window)
        if not symbols:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        h = hist[symbols].ffill()
        last = h.iloc[-1]
        # Stack per-window above-SMA flags, then average across windows.
        on_flags = []
        for w in self.windows:
            sma = h.rolling(w, min_periods=w).mean().iloc[-1]
            on_flags.append((last > sma).astype(float))
        # Element-wise mean across the windows for each symbol.
        fraction_long = pd.concat(on_flags, axis=1).mean(axis=1)
        fraction_long = fraction_long.dropna()
        if fraction_long.empty:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        return pd.DataFrame({
            "symbol": fraction_long.index.tolist(),
            "raw": fraction_long.values,
            "score": (fraction_long * 2.0).values,
        })
