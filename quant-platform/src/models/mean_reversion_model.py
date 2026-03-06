from __future__ import annotations

import pandas as pd

from src.models.base_model import BaseModel


class MeanReversionModel(BaseModel):
    def fit(self, model_state: pd.DataFrame) -> dict:
        return {"model": "mean_reversion", "rows": len(model_state)}

    def predict_weights(self, model_state: pd.DataFrame) -> pd.DataFrame:
        latest = model_state.sort_values("date").groupby("symbol").tail(1).copy()
        latest["signal"] = (-latest["zscore_20"].fillna(0)).clip(lower=0)
        total = latest["signal"].sum()
        latest["weight"] = latest["signal"] / total if total > 0 else 0.0
        return latest[["date", "symbol", "weight"]]
