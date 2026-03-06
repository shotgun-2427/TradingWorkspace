from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.backtest.backtester import run_backtest
from src.features.feature_builder import build_features
from src.models.mean_reversion_model import MeanReversionModel
from src.models.trend_model import TrendModel
from src.models.zscore_model import ZScoreModel
from src.portfolio.optimizer import cap_weights
from src.portfolio.weight_allocator import normalize_weights
from src.utils.config_loader import load_yaml


MODEL_MAP = {
    "trend": TrendModel,
    "mean_reversion": MeanReversionModel,
    "zscore": ZScoreModel,
}


def main() -> None:
    config = load_yaml(ROOT / "config" / "config.yaml")
    prices = pd.read_parquet(ROOT / "data" / "cache" / "prices.parquet")
    features = build_features(prices)

    model_name = config.get("model", {}).get("name", "zscore")
    model = MODEL_MAP.get(model_name, ZScoreModel)()
    model.fit(features)

    weights = model.predict_weights(features)
    weights = normalize_weights(cap_weights(weights))

    initial_capital = config.get("backtest", {}).get("initial_capital", 100000)
    out = run_backtest(prices, weights, initial_capital=initial_capital)

    sim_dir = ROOT / "artifacts" / "simulations"
    sim_dir.mkdir(parents=True, exist_ok=True)
    prices.to_parquet(sim_dir / "results.parquet", index=False)
    weights.to_parquet(sim_dir / "weights.parquet", index=False)

    run_dir = ROOT / "artifacts" / "production_runs" / date.today().isoformat()
    run_dir.mkdir(parents=True, exist_ok=True)

    prices.to_parquet(run_dir / "prices.parquet", index=False)
    features.to_parquet(run_dir / "model_state.parquet", index=False)
    weights.to_parquet(run_dir / "weights.parquet", index=False)

    pd.DataFrame(
        {
            "date": out["daily_returns"].index,
            "daily_returns": out["daily_returns"].values,
            "equity_curve": out["equity_curve"].values,
            "drawdown": out["drawdown"].values,
        }
    ).to_parquet(run_dir / "results.parquet", index=False)

    with (run_dir / "metrics.json").open("w", encoding="utf-8") as file:
        json.dump(out["metrics"], file, indent=2)


if __name__ == "__main__":
    main()
