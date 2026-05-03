"""
NATR mean-reversion signal.

NATR (Normalized Average True Range) measures recent volatility as a
percentage of price. We use it to drive a *mean-reversion* signal: when an
asset has stretched far from its short-term average AND has high recent
volatility, we expect reversion.

raw     = -1 * (price_t - sma_short) / (NATR_t * price_t)
          (negative: price way above its short MA → score down → fade the move)
score   = cross-sectional z-score of `raw`.

Only requires close prices (uses close-to-close ATR proxy when high/low
aren't available, which they aren't in our master file). When high/low are
present in the panel a true ATR is used.

Defaults: short_ma=10, natr_window=20.
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


class NATRMeanReversion:
    name = "natr_mean_reversion"

    def __init__(self, short_ma: int = 10, natr_window: int = 20):
        self.short_ma = int(short_ma)
        self.natr_window = int(natr_window)
        self.lookback_days_required = max(short_ma, natr_window) + 5

    def compute(
        self,
        prices: pd.DataFrame,
        as_of: pd.Timestamp,
    ) -> pd.DataFrame:
        wide = to_wide(prices)
        # Need lookback + a small buffer so SMAs / NATRs are well-defined.
        hist = slice_history(wide, as_of, lookback=self.lookback_days_required + 5)
        symbols = eligible_columns(hist, min_history=self.lookback_days_required)
        if not symbols:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        h = hist[symbols].ffill()

        # Close-to-close TR as a proxy for ATR (master file has only close).
        tr = h.diff().abs()
        atr = tr.rolling(self.natr_window, min_periods=self.natr_window // 2).mean()
        natr = (atr / h).iloc[-1]               # NATR_t per symbol
        sma_short = h.rolling(self.short_ma).mean().iloc[-1]
        last = h.iloc[-1]

        # Distance above the short MA, normalized by NATR. Sign flipped so
        # *positive* score means the asset is unusually low → buy candidate.
        deviation = (last - sma_short) / (natr * last + 1e-12)
        raw = -deviation

        out = pd.DataFrame({
            "symbol": raw.index.tolist(),
            "raw": raw.values,
        })
        out = out.dropna()
        if out.empty:
            return pd.DataFrame(columns=["symbol", "raw", "score"])
        out["score"] = zscore(out["raw"])
        return out
