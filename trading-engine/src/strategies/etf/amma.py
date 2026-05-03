"""
AMMA — Adaptive Moving Average trend signal (Kaufman's KAMA variant).

A standard moving average smooths price equally regardless of regime. KAMA
adapts: when price is trending cleanly, it tracks closely (fast); when
price is choppy, it lags (slow). The "efficiency ratio" — net price change
divided by total absolute movement — drives the adaptation.

raw = (price_t - amma_t) / amma_t
score = cross-sectional z-score of `raw`.

Positive score → asset is above its adaptive trend (bullish trend).
Negative score → asset is below (bearish trend / under pressure).

Defaults: er_window=10, fast=2, slow=30 — Kaufman's original.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.strategies import zscore
from src.strategies.etf._panel import (
    eligible_columns,
    slice_history,
    to_wide,
)


class AMMATrend:
    name = "amma"

    def __init__(self, er_window: int = 10, fast: int = 2, slow: int = 30):
        self.er_window = int(er_window)
        self.fast = int(fast)
        self.slow = int(slow)
        # Need enough history to seed the smoothing constant + room for the AMMA
        # itself to converge.
        self.lookback_days_required = max(er_window, slow) + 10

    @staticmethod
    def _kama(close: pd.Series, er_window: int, fast: int, slow: int) -> pd.Series:
        """
        Vectorized KAMA.

        The recursion is:
            kama[t] = kama[t-1] + sc[t] * (close[t] - kama[t-1])

        This *can't* be a pure numpy ufunc because each step depends on the
        previous output, but we can drop into raw numpy arrays so the
        per-iteration overhead is just an array index, not a pandas .loc
        lookup (which is roughly 50x faster).
        """
        if close.empty:
            return close.copy()

        prices = close.to_numpy(dtype=float, copy=False)
        n = len(prices)
        if n == 0:
            return close.copy()

        # Efficiency ratio and smoothing constant (vectorized).
        change_abs = np.abs(prices - np.concatenate([[np.nan] * er_window, prices[:-er_window]]))
        diffs_abs = np.abs(np.diff(prices, prepend=prices[0]))
        # Rolling sum of |diff| over er_window via cumsum trick.
        cs = np.cumsum(diffs_abs)
        cs = np.concatenate([[0.0], cs])
        volatility = cs[er_window:] - cs[:-er_window] if n >= er_window else np.full(n, np.nan)
        if volatility.shape[0] != n:
            pad = n - volatility.shape[0]
            volatility = np.concatenate([np.full(pad, np.nan), volatility])

        with np.errstate(divide="ignore", invalid="ignore"):
            er = np.where(volatility > 0, change_abs / (volatility + 1e-12), 0.0)
        er = np.nan_to_num(er, nan=0.0)
        fast_sc = 2.0 / (fast + 1)
        slow_sc = 2.0 / (slow + 1)
        sc = (er * (fast_sc - slow_sc) + slow_sc) ** 2

        # First non-NaN price index = seed for KAMA.
        valid_mask = ~np.isnan(prices)
        if not valid_mask.any():
            return pd.Series(np.nan, index=close.index, dtype=float)
        first_valid = int(np.argmax(valid_mask))

        out = np.full(n, np.nan, dtype=float)
        out[first_valid] = prices[first_valid]
        for i in range(first_valid + 1, n):
            prev = out[i - 1]
            sc_t = sc[i] if not np.isnan(sc[i]) else 0.0
            current = prices[i]
            if np.isnan(current):
                out[i] = prev
            else:
                out[i] = prev + sc_t * (current - prev)

        return pd.Series(out, index=close.index, dtype=float)

    def compute(
        self,
        prices: pd.DataFrame,
        as_of: pd.Timestamp,
    ) -> pd.DataFrame:
        wide = to_wide(prices)
        hist = slice_history(wide, as_of, lookback=self.lookback_days_required + 50)
        symbols = eligible_columns(hist, min_history=self.lookback_days_required)
        if not symbols:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        h = hist[symbols].ffill()
        last_close = h.iloc[-1]
        amma_last: dict[str, float] = {}
        for sym in symbols:
            amma_series = self._kama(h[sym], self.er_window, self.fast, self.slow)
            val = amma_series.iloc[-1]
            if pd.notna(val):
                amma_last[sym] = float(val)

        if not amma_last:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        amma_series_last = pd.Series(amma_last)
        last = last_close.reindex(amma_series_last.index)
        raw = (last - amma_series_last) / amma_series_last
        raw = raw.replace([float("inf"), float("-inf")], pd.NA).dropna()

        out = pd.DataFrame({"symbol": raw.index.tolist(), "raw": raw.values})
        out["score"] = zscore(out["raw"])
        return out
