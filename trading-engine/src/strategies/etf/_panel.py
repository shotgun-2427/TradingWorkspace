"""
Shared helpers for ETF signals.

Every signal needs to:
  1. accept either wide or long-format prices
  2. align them on a common date axis
  3. trim to history available on or before ``as_of``
  4. drop symbols with insufficient history

These three helpers handle that uniformly so the per-signal code can focus
on its math.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def to_wide(prices: pd.DataFrame) -> pd.DataFrame:
    """Coerce prices into wide format (date index × symbol columns).

    Accepts either:
      - long format with columns ``date``, ``symbol``, ``close``
      - already-wide format with a DatetimeIndex
    """
    df = prices.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]
    if {"date", "symbol", "close"}.issubset(df.columns):
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
        df = df.dropna(subset=["date", "symbol"])
        wide = (
            df.pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
            .sort_index()
        )
        return wide
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.sort_index()
    return df


def slice_history(wide: pd.DataFrame, as_of: pd.Timestamp, lookback: int) -> pd.DataFrame:
    """Return rows on or before ``as_of`` keeping at most ``lookback + 1`` of them."""
    as_of = pd.Timestamp(as_of).normalize()
    in_window = wide.loc[wide.index <= as_of]
    if lookback > 0:
        in_window = in_window.tail(lookback + 1)
    return in_window


def eligible_columns(history: pd.DataFrame, min_history: int) -> list[str]:
    """Symbols that have at least ``min_history`` non-NaN observations."""
    counts = history.notna().sum()
    return counts.index[counts >= min_history].tolist()


def safe_returns(history: pd.DataFrame) -> pd.DataFrame:
    """Daily simple returns from a wide price panel."""
    return history.pct_change()


def safe_log_returns(history: pd.DataFrame) -> pd.DataFrame:
    """Daily log returns. NaNs preserved."""
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.log(history).diff()
