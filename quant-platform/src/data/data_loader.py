from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_prices(path: str | Path) -> pd.DataFrame:
    return pd.read_parquet(path)
