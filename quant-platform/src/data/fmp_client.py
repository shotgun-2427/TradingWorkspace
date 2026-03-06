from __future__ import annotations

import os
from typing import Any

import pandas as pd
import requests

from src.utils.logging import get_logger


logger = get_logger(__name__)


class FMPClient:
    BASE_URL = "https://financialmodelingprep.com/api/v3/historical-price-full"

    def __init__(self, api_key: str | None = None, timeout: int = 30) -> None:
        self.api_key = api_key or os.getenv("FMP_API_KEY", "INSERT_FMP_API_KEY")
        self.timeout = timeout

    def get_daily_prices(self, symbol: str) -> pd.DataFrame:
        return self.get_historical_prices(symbol=symbol, start_date=None, end_date=None)

    def get_historical_prices(
        self, symbol: str, start_date: str | None, end_date: str | None
    ) -> pd.DataFrame:
        params: dict[str, Any] = {"apikey": self.api_key}
        if start_date:
            params["from"] = start_date
        if end_date:
            params["to"] = end_date

        url = f"{self.BASE_URL}/{symbol}"
        response = requests.get(url, params=params, timeout=self.timeout)

        if response.status_code == 429:
            logger.warning("FMP rate limit reached for %s. Returning empty DataFrame.", symbol)
            return pd.DataFrame(columns=["date", "symbol", "close", "volume"])

        response.raise_for_status()
        payload = response.json()

        historical = payload.get("historical") or []
        if not historical:
            logger.warning("No historical prices returned for %s. Payload keys=%s", symbol, payload.keys())
            return pd.DataFrame(columns=["date", "symbol", "close", "volume"])

        frame = pd.DataFrame(historical)
        keep = ["date", "close", "volume"]
        frame = frame[keep]
        frame["symbol"] = symbol
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.sort_values("date").reset_index(drop=True)
        return frame[["date", "symbol", "close", "volume"]]
