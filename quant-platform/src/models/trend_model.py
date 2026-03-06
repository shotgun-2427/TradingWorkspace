from __future__ import annotations

import numpy as np
import pandas as pd

from src.models.base_model import BaseModel


class TrendModel(BaseModel):
    def fit(self, model_state: pd.DataFrame) -> dict:
        return {"model": "trend", "rows": len(model_state)}

    def predict_weights(self, model_state: pd.DataFrame) -> pd.DataFrame:
        latest = model_state.sort_values("date").groupby("symbol").tail(1).copy()
        latest["signal"] = np.where(latest["mom_20"].fillna(0) > 0, 1.0, 0.0)
        total = latest["signal"].sum()
        latest["weight"] = latest["signal"] / total if total > 0 else 0.0
        return latest[["date", "symbol", "weight"]]
