from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class TargetResult:
    ok: bool
    profile: str
    action: str
    rows_written: int
    rebalance_dates: int
    symbols_considered: int
    latest_rebalance_date: str | None
    csv_path: str
    parquet_path: str
    preview: list[dict[str, Any]]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _paths() -> dict[str, Path]:
    root = _project_root()
    return {
        "prices_csv": root / "data" / "market" / "cleaned" / "prices" / "etf_prices_master.csv",
        "prices_parquet": root / "data" / "market" / "cleaned" / "prices" / "etf_prices_master.parquet",
        "targets_csv": root / "data" / "market" / "cleaned" / "targets" / "etf_targets_monthly.csv",
        "targets_parquet": root / "data" / "market" / "cleaned" / "targets" / "etf_targets_monthly.parquet",
    }


def _load_prices() -> pd.DataFrame:
    p = _paths()
    if p["prices_parquet"].exists():
        df = pd.read_parquet(p["prices_parquet"])
    elif p["prices_csv"].exists():
        df = pd.read_csv(p["prices_csv"])
    else:
        raise FileNotFoundError("Master price file not found.")

    df.columns = [c.lower().strip() for c in df.columns]
    required = ["date", "symbol", "close"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column in master prices: {col}")

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    df = df.dropna(subset=["date", "symbol", "close"])
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    return df


def _build_features(
    df: pd.DataFrame,
    momentum_lookback: int,
    min_history: int,
) -> pd.DataFrame:
    df = df.copy()

    df["obs_count"] = df.groupby("symbol").cumcount() + 1
    df["mom"] = df.groupby("symbol")["close"].pct_change(momentum_lookback)
    df["eligible"] = (df["obs_count"] >= min_history) & df["mom"].notna()

    # rebalance only on each symbol's last available row in a calendar month
    df["month"] = df["date"].dt.to_period("M")
    month_last = df.groupby(["symbol", "month"])["date"].transform("max")
    df["is_rebalance_row"] = df["date"].eq(month_last)

    return df


def _build_targets(
    df: pd.DataFrame,
    top_k: int,
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []

    rebal_dates = sorted(df.loc[df["is_rebalance_row"], "date"].drop_duplicates().tolist())

    for rebal_date in rebal_dates:
        snap = df[(df["date"] == rebal_date) & (df["is_rebalance_row"])].copy()

        snap = snap[snap["eligible"]].copy()
        if snap.empty:
            continue

        snap = snap.sort_values(["mom", "symbol"], ascending=[False, True]).reset_index(drop=True)
        snap["rank"] = np.arange(1, len(snap) + 1)

        winners = snap.head(top_k).copy()
        if winners.empty:
            continue

        winners["target_weight"] = 1.0 / len(winners)
        winners["rebalance_date"] = rebal_date
        winners["signal_value"] = winners["mom"]

        rows.append(
            winners[
                [
                    "rebalance_date",
                    "symbol",
                    "target_weight",
                    "signal_value",
                    "rank",
                    "close",
                    "obs_count",
                ]
            ].rename(columns={"close": "reference_price"})
        )

    if not rows:
        return pd.DataFrame(
            columns=[
                "rebalance_date",
                "symbol",
                "target_weight",
                "signal_value",
                "rank",
                "reference_price",
                "obs_count",
            ]
        )

    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["rebalance_date", "rank", "symbol"]).reset_index(drop=True)
    return out


def generate_targets(
    profile: str = "paper",
    momentum_lookback: int = 126,
    min_history: int = 126,
    top_k: int = 5,
) -> dict[str, Any]:
    p = _paths()
    p["targets_csv"].parent.mkdir(parents=True, exist_ok=True)

    prices = _load_prices()
    features = _build_features(
        prices,
        momentum_lookback=momentum_lookback,
        min_history=min_history,
    )
    targets = _build_targets(features, top_k=top_k)

    targets.to_csv(p["targets_csv"], index=False)
    targets.to_parquet(p["targets_parquet"], index=False)

    latest_rebal = None
    if not targets.empty:
        latest_rebal = str(pd.to_datetime(targets["rebalance_date"]).max().date())

    preview = targets.tail(15).to_dict(orient="records")

    result = TargetResult(
        ok=True,
        profile=profile,
        action="generate_targets",
        rows_written=len(targets),
        rebalance_dates=int(targets["rebalance_date"].nunique()) if not targets.empty else 0,
        symbols_considered=int(prices["symbol"].nunique()),
        latest_rebalance_date=latest_rebal,
        csv_path=str(p["targets_csv"]),
        parquet_path=str(p["targets_parquet"]),
        preview=preview,
    )
    return asdict(result)


def run(**kwargs: Any) -> dict[str, Any]:
    return generate_targets(**kwargs)


def main(**kwargs: Any) -> dict[str, Any]:
    return generate_targets(**kwargs)