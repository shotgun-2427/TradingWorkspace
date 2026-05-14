"""
Volatility-targeted trend signal.

The plain trend filter (price > 200d SMA → long, else flat) leaves money on
the table during quiet, choppy uptrends and over-allocates during high-vol
"climb a wall of worry" regimes. Volatility targeting scales the position
inversely to realized vol, so the *risk contribution* of the strategy stays
roughly constant.

At each rebalance, for each symbol:
  1. Compute the 200-day SMA. Trend filter: skip the asset if price <= SMA
     (matches ``trend_filter``).
  2. Compute realized 20-day annualized vol from log returns.
  3. weight = min(1.0, target_vol / realized_vol)
     so weight = 1.0 when realized = target, and shrinks below 1.0 when the
     asset is hotter than target. Always long-only — never levers up beyond 1.0.

The emitted ``score`` is ``2.0 * weight`` so the existing
``_score_to_weight`` clip in ``per_etf_backtest`` resolves to ``weight``
exactly.

``raw`` carries the realized vol (annualized) for diagnostics.

Defaults: sma_window=200, vol_window=20, target_vol=0.15 (15% annualized).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.strategies.etf._panel import (
    eligible_columns,
    slice_history,
    to_wide,
)

_TRADING_DAYS = 252


class VolTargetTrend:
    name = "vol_target_trend"
    kind = "absolute"  # time-series; do not z-score in ensembles

    def __init__(
        self,
        sma_window: int = 200,
        vol_window: int = 20,
        target_vol: float = 0.15,
        max_weight: float = 1.0,
    ):
        self.sma_window = int(sma_window)
        self.vol_window = int(vol_window)
        self.target_vol = float(target_vol)
        # Cap weight at 1.0 by default — no leverage. Bumping this above 1.0
        # is allowed but would conflict with the long-only ``_score_to_weight``
        # clip in per_etf_backtest, so it'd be silently truncated.
        self.max_weight = float(max_weight)
        self.lookback_days_required = max(sma_window, vol_window) + 5

    def compute(self, prices: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
        wide = to_wide(prices)
        lookback = max(self.sma_window, self.vol_window) + 5
        hist = slice_history(wide, as_of, lookback=lookback + 5)
        symbols = eligible_columns(hist, min_history=lookback)
        if not symbols:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        h = hist[symbols].ffill()
        sma = h.rolling(self.sma_window, min_periods=self.sma_window).mean().iloc[-1]
        last = h.iloc[-1]
        # Daily log returns -> realized vol -> annualize.
        log_ret = np.log(h).diff()
        realized_vol = log_ret.rolling(
            self.vol_window, min_periods=max(self.vol_window // 2, 5)
        ).std().iloc[-1] * np.sqrt(_TRADING_DAYS)
        realized_vol = realized_vol.replace([np.inf, -np.inf], np.nan)

        trend_on = (last > sma).astype(float)
        # When realized_vol is NaN or zero, set weight to 0 (defensive).
        with np.errstate(divide="ignore", invalid="ignore"):
            weight_unclipped = self.target_vol / realized_vol
        weight = weight_unclipped.where(realized_vol > 0, other=0.0)
        weight = weight.replace([np.inf, -np.inf], 0.0).fillna(0.0)
        weight = weight.clip(lower=0.0, upper=self.max_weight)

        # Trend filter zero-outs the position when below the SMA.
        weight = weight * trend_on

        # Emit realized vol as ``raw`` so the diagnostic stays informative.
        raw = realized_vol
        score = (weight * 2.0).clip(lower=0.0, upper=2.0)
        return pd.DataFrame({
            "symbol": weight.index.tolist(),
            "raw": raw.values,
            "score": score.values,
        }).dropna(subset=["score"])
