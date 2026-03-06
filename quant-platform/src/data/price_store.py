from __future__ import annotations

from pathlib import Path

import pandas as pd


def save_prices(prices: pd.DataFrame, path: str | Path) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    prices.to_parquet(destination, index=False)
