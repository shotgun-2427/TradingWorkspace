from __future__ import annotations

import pandas as pd


def rolling_zscore(series: pd.Series, window: int = 20) -> pd.Series:
    mean = series.rolling(window=window).mean()
    std = series.rolling(window=window).std()
    return (series - mean) / std


def simple_momentum(series: pd.Series, lookback: int = 20) -> pd.Series:
    return series.pct_change(lookback)
