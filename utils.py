import math
from typing import Dict

import polars as pl
import json


def calculate_calendar_lookback(lookback_days: int, cushion_days: int = 2) -> int:
    # 5 trading days ≈ 7 calendar days; add a small cushion for holidays/halts
    return int(math.ceil(lookback_days * 7 / 5)) + cushion_days


def parse_backtest_result(backtest_result: Dict[str, pl.DataFrame]) -> str:
    df = backtest_result.get("backtest_metrics")

    # Handle missing/empty
    if df is None or df.is_empty():
        return "No backtest results available."

    # If there can be duplicate metrics, pick the last occurrence
    # (change to .mean(), .max(), etc. if you prefer)
    if df.select(pl.len()).item() != df.select(pl.col("metric").n_unique()).item():
        df = (
            df.select(["metric", "value"])
            .group_by("metric")
            .agg(pl.col("value").last())
            .rename({"value": "value"})
        )
    else:
        df = df.select(["metric", "value"])

    # Build metric -> value mapping
    metrics = dict(df.iter_rows())

    # If you truly need a string back:
    return json.dumps(metrics, default=float)
