"""
Buy-and-hold reference signal.

Emits a constant fully-invested signal for every symbol with enough history.
Used purely as a benchmark in the per-ETF backtest dashboard: any model
worth keeping should outperform buy-and-hold on a risk-adjusted basis.

``raw`` is the cumulative total return since the start of the available
panel (handy for diagnostics). ``score`` is a constant ``2.0`` so the
``_score_to_weight`` mapping in ``per_etf_backtest`` resolves to weight = 1
(fully long).
"""
from __future__ import annotations

import pandas as pd

from src.strategies.etf._panel import eligible_columns, slice_history, to_wide


class BuyAndHold:
    name = "buy_and_hold"
    # Cross-sectional / ensemble layers re-z-score raw; tag the kind so the
    # ensemble can decide to skip us if it ever wants to keep z-scoring pure.
    kind = "absolute"

    def __init__(self, min_history: int = 21):
        # 21 trading days (~1 month) — enough to compute a cum-return for
        # ``raw``. Doesn't actually gate the signal; we just want some bars.
        self.min_history = int(min_history)
        self.lookback_days_required = self.min_history

    def compute(self, prices: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
        wide = to_wide(prices)
        hist = slice_history(wide, as_of, lookback=self.min_history + 5)
        symbols = eligible_columns(hist, min_history=self.min_history)
        if not symbols:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        h = hist[symbols].ffill()
        last = h.iloc[-1]
        first = h.iloc[0]
        cum_return = (last / first) - 1.0
        return pd.DataFrame({
            "symbol": cum_return.index.tolist(),
            "raw": cum_return.values,
            # Constant fully-invested signal. _score_to_weight clips s/2 to
            # [0,1], so 2.0 → weight = 1.0 exactly.
            "score": [2.0] * len(cum_return),
        })
