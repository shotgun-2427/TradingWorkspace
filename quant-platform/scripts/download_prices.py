from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from src.data.fmp_client import FMPClient
from src.data.price_store import save_prices
from src.utils.config_loader import load_yaml
from src.utils.logging import get_logger


logger = get_logger(__name__)


def main() -> None:
    config = load_yaml(ROOT / "config" / "config.yaml")
    symbols = config.get("universe", [])
    start_date = config.get("backtest", {}).get("start_date")
    end_date = config.get("backtest", {}).get("end_date")

    client = FMPClient()
    frames: list[pd.DataFrame] = []
    for symbol in symbols:
        try:
            frames.append(client.get_historical_prices(symbol, start_date, end_date))
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", symbol, exc)

    prices = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["date", "symbol", "close", "volume"])
    save_prices(prices, ROOT / "data" / "cache" / "prices.parquet")
    logger.info("Saved %s rows to data/cache/prices.parquet", len(prices))


if __name__ == "__main__":
    main()
