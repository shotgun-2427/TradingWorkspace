from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseModel(ABC):
    @abstractmethod
    def fit(self, model_state: pd.DataFrame) -> dict:
        raise NotImplementedError

    @abstractmethod
    def predict_weights(self, model_state: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError
