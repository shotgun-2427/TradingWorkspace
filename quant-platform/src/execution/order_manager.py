from __future__ import annotations

import pandas as pd

from src.execution.ibkr_client import IBKRClient


def rebalance_portfolio(client: IBKRClient, target_weights: pd.DataFrame, capital: float) -> None:
    if not client.ib.isConnected():
        client.connect_to_tws()

    for _, row in target_weights.iterrows():
        symbol = row["symbol"]
        qty = max(int(capital * row["weight"] / 100), 1)
        side = "BUY" if row["weight"] > 0 else "SELL"
        client.place_order(symbol=symbol, quantity=qty, side=side)
