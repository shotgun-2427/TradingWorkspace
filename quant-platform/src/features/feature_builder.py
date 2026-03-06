from __future__ import annotations

import pandas as pd

from src.features.indicators import rolling_zscore, simple_momentum


def build_features(prices_df: pd.DataFrame) -> pd.DataFrame:
    ordered = prices_df.sort_values(["symbol", "date"]).copy()
    ordered["return_1d"] = ordered.groupby("symbol")["close"].pct_change()
    ordered["mom_20"] = ordered.groupby("symbol")["close"].transform(simple_momentum)
    ordered["zscore_20"] = ordered.groupby("symbol")["close"].transform(rolling_zscore)
    return ordered
