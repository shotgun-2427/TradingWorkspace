"""
strategies — pluggable signal architecture.

Three asset-class trees (etf, futures, options). Each signal in those trees
implements the ``Signal`` protocol: given a panel of prices and an as-of
date, return per-asset scores that are normalized so multiple signals can
be combined cleanly by the ensemble layer.

Contract (intentionally minimal — keeps all signals trivially testable):

    class MySignal:
        name = "my_signal"                  # stable identifier
        lookback_days_required = 126        # min history needed per asset

        def compute(self, prices, as_of) -> pd.DataFrame:
            ...
            return pd.DataFrame({"symbol": ..., "raw": ..., "score": ...})

The ``score`` column should be cross-sectionally z-scored within the panel
so signals on wildly different scales (returns vs. NATR vs. RSI) compose
sensibly. ``raw`` is the underlying metric, kept around for diagnostics.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class Signal(Protocol):
    name: str
    lookback_days_required: int

    def compute(
        self,
        prices: pd.DataFrame,
        as_of: pd.Timestamp,
    ) -> pd.DataFrame:
        """Return a DataFrame with columns ``symbol``, ``raw``, ``score``.

        ``prices`` is wide-format (date × symbol → close) or long-format
        (date, symbol, close). Implementations should accept either.
        ``as_of`` is the rebalance date — only data on or before this date
        may be used (no look-ahead).
        """
        ...


def zscore(series: pd.Series) -> pd.Series:
    """Cross-sectional z-score with safe handling of zero variance."""
    s = pd.to_numeric(series, errors="coerce")
    mu = s.mean()
    sd = s.std()
    if sd is None or pd.isna(sd) or sd == 0:
        return pd.Series(0.0, index=s.index)
    return (s - mu) / sd
