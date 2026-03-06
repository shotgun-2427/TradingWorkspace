from __future__ import annotations

import numpy as np
import pandas as pd


def compute_metrics(daily_returns: pd.Series, drawdown: pd.Series) -> dict[str, float]:
    clean = daily_returns.dropna()
    sharpe = float((clean.mean() / clean.std()) * np.sqrt(252)) if clean.std() else 0.0
    annual_return = float((1 + clean.mean()) ** 252 - 1) if len(clean) else 0.0
    volatility = float(clean.std() * np.sqrt(252)) if len(clean) else 0.0
    max_drawdown = float(drawdown.min()) if len(drawdown) else 0.0
    win_rate = float((clean > 0).mean()) if len(clean) else 0.0
    return {
        "sharpe": sharpe,
        "annual_return": annual_return,
        "volatility": volatility,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
    }
