from __future__ import annotations

import pandas as pd


def normalize_weights(weights: pd.DataFrame) -> pd.DataFrame:
    out = weights.copy()
    total = out["weight"].sum()
    out["weight"] = out["weight"] / total if total else 0.0
    return out
