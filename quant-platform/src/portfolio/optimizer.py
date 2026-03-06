from __future__ import annotations

import pandas as pd


def cap_weights(weights: pd.DataFrame, max_weight: float = 0.4) -> pd.DataFrame:
    out = weights.copy()
    out["weight"] = out["weight"].clip(upper=max_weight)
    total = out["weight"].sum()
    out["weight"] = out["weight"] / total if total else 0.0
    return out
