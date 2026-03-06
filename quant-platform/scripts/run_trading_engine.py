from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.execution.ibkr_client import IBKRClient
from src.execution.order_manager import rebalance_portfolio
from src.utils.config_loader import load_yaml


if __name__ == "__main__":
    config = load_yaml(ROOT / "config" / "config.yaml")
    execution = config.get("execution", {})

    weights_path = ROOT / "artifacts" / "simulations" / "weights.parquet"
    if not weights_path.exists():
        raise FileNotFoundError("Run `python scripts/run_backtest.py` first.")

    weights = pd.read_parquet(weights_path)

    client = IBKRClient(
        host=execution.get("host", "127.0.0.1"),
        port=execution.get("port", 7497),
        client_id=execution.get("client_id", 1),
    )
    client.connect_to_tws()
    rebalance_portfolio(client, weights, capital=100000)
