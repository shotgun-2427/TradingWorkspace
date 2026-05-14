"""
Trend filter — long while price is above its long-window SMA, flat otherwise.

This is the classic Faber "10-month moving average" rule (Faber, 2007) in
trading-day units: at each rebalance, look at the spot close vs the simple
moving average over the last ``sma_window`` trading days. Long if above,
flat if below. The idea is to harvest the equity risk premium during
trending up regimes and step aside during bear markets, which historically
sidesteps the worst drawdowns at a small cost to long-run compounding.

The rule is a *time-series* signal (no cross-sectional comparison), so we
emit a score in ``{0.0, 2.0}`` directly — that maps through the existing
``_score_to_weight`` clip-to-[0,1] in ``per_etf_backtest`` to ``{0, 1}``.

``raw`` is the % distance from the SMA so the diagnostic still has signal:
strongly above → big positive raw; below → negative raw.

Defaults: sma_window=200 (~10 months), buffer=0.00 (no whipsaw band).
"""
from __future__ import annotations

import pandas as pd

from src.strategies.etf._panel import (
    eligible_columns,
    slice_history,
    to_wide,
)


class TrendFilter:
    name = "trend_filter"
    kind = "absolute"  # time-series; do not z-score in ensembles

    def __init__(self, sma_window: int = 200, buffer: float = 0.0):
        self.sma_window = int(sma_window)
        # ``buffer`` adds a dead-band around the crossover (e.g. 0.005 means
        # the price must be 0.5% above the SMA to flip on, and 0.5% below to
        # flip off). Helps reduce monthly-rebalance whipsaw on the edge.
        self.buffer = float(buffer)
        self.lookback_days_required = self.sma_window + 5

    def compute(self, prices: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
        wide = to_wide(prices)
        hist = slice_history(wide, as_of, lookback=self.sma_window + 5)
        symbols = eligible_columns(hist, min_history=self.sma_window)
        if not symbols:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        h = hist[symbols].ffill()
        sma = h.rolling(self.sma_window, min_periods=self.sma_window).mean().iloc[-1]
        last = h.iloc[-1]

        # Distance from SMA, signed. Used as the diagnostic ``raw`` column.
        deviation = (last - sma) / sma
        deviation = deviation.replace([float("inf"), float("-inf")], pd.NA).dropna()
        if deviation.empty:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        # Score = 2.0 (fully invested) when above SMA + buffer, else 0.0.
        score = (deviation > self.buffer).astype(float) * 2.0
        return pd.DataFrame({
            "symbol": deviation.index.tolist(),
            "raw": deviation.values,
            "score": score.values,
        })
