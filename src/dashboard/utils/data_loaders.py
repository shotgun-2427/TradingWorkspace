from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st

from src.dashboard.services.broker_service import get_account_summary


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _latest_file(folder: Path, pattern: str) -> Path | None:
    files = sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _extract_timestamp(path: Path) -> pd.Timestamp | None:
    m = re.search(r"(\d{8})_(\d{6})", path.name)
    if m:
        return pd.to_datetime(f"{m.group(1)} {m.group(2)}", format="%Y%m%d %H%M%S", errors="coerce")
    m = re.search(r"(\d{8})", path.name)
    if m:
        return pd.to_datetime(m.group(1), format="%Y%m%d", errors="coerce")
    try:
        return pd.Timestamp.fromtimestamp(path.stat().st_mtime)
    except Exception:
        return None


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def _first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {_normalize_name(c): c for c in df.columns}
    for candidate in candidates:
        actual = normalized.get(_normalize_name(candidate))
        if actual:
            return actual
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.replace("$", "", regex=False),
        errors="coerce",
    )


def _extract_tag_value(df: pd.DataFrame, tags: list[str]) -> float | None:
    tag_col = _first_existing_col(df, ["tag", "key", "field", "name"])
    value_col = _first_existing_col(df, ["value", "amount", "val"])
    if tag_col is None or value_col is None:
        return None

    temp = df.copy()
    temp[tag_col] = temp[tag_col].astype(str)

    for tag in tags:
        hit = temp.loc[temp[tag_col].str.lower() == tag.lower()]
        if not hit.empty:
            val = _to_float(hit.iloc[0][value_col])
            if val is not None:
                return val
    return None


@st.cache_data(show_spinner=False)
def build_equity_curve() -> pd.DataFrame:
    account_dir = _project_root() / "data" / "broker" / "account"
    rows: list[dict[str, Any]] = []

    for path in sorted(account_dir.glob("paper_account_summary_*.csv")):
        try:
            df = pd.read_csv(path)
            nav = _extract_tag_value(
                df,
                ["NetLiquidation", "EquityWithLoanValue", "Net Liquidation", "Equity With Loan Value"],
            )
            ts = _extract_timestamp(path)
            if nav is not None and ts is not None:
                rows.append({"timestamp": ts, "nav": float(nav)})
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["timestamp", "nav"])

    out = pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    return out


@st.cache_data(show_spinner=False)
def load_positions_snapshot() -> pd.DataFrame:
    path = _project_root() / "data" / "broker" / "positions" / "paper_positions_snapshot.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame()

    symbol_col = _first_existing_col(df, ["symbol", "ticker"])
    shares_col = _first_existing_col(df, ["current_shares", "shares", "position", "qty", "quantity"])
    avg_col = _first_existing_col(df, ["avg_cost", "average_cost", "avg_px", "avgprice"])

    if symbol_col is None or shares_col is None or avg_col is None:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["symbol"] = df[symbol_col].astype(str).str.upper()
    out["shares"] = _to_num(df[shares_col]).fillna(0.0)
    out["avg_cost"] = _to_num(df[avg_col]).fillna(0.0)
    out = out.loc[out["shares"] != 0].copy()
    out = out.sort_values("symbol").reset_index(drop=True)
    return out


@st.cache_data(show_spinner=False)
def load_latest_prices() -> pd.DataFrame:
    prices_dir = _project_root() / "data" / "market" / "cleaned" / "prices"
    parquet_path = prices_dir / "etf_prices_master.parquet"
    csv_path = prices_dir / "etf_prices_master.csv"

    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
    elif csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        return pd.DataFrame()

    df.columns = [str(c).lower().strip() for c in df.columns]
    symbol_col = _first_existing_col(df, ["symbol", "ticker"])
    date_col = _first_existing_col(df, ["date", "datetime", "timestamp"])
    close_col = _first_existing_col(df, ["close", "adj_close", "adjclose", "adjusted_close"])

    if symbol_col is None or date_col is None or close_col is None:
        return pd.DataFrame()

    out = df[[symbol_col, date_col, close_col]].copy()
    out.columns = ["symbol", "date", "close"]
    out["symbol"] = out["symbol"].astype(str).str.upper()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["close"] = _to_num(out["close"])
    out = out.dropna(subset=["date", "close"])
    out = out.sort_values(["symbol", "date"]).reset_index(drop=True)
    return out


@st.cache_data(show_spinner=False)
def build_holdings_table() -> pd.DataFrame:
    positions = load_positions_snapshot()
    prices = load_latest_prices()

    if positions.empty or prices.empty:
        return pd.DataFrame()

    latest = (
        prices.groupby("symbol", as_index=False)
        .tail(1)[["symbol", "close"]]
        .rename(columns={"close": "last"})
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    merged = positions.merge(latest, on="symbol", how="left")
    merged["last"] = merged["last"].fillna(0.0)
    merged["market_value"] = merged["shares"] * merged["last"]
    merged["pnl"] = merged["shares"] * (merged["last"] - merged["avg_cost"])

    total_mv = float(merged["market_value"].sum())
    merged["weight"] = np.where(total_mv > 0, merged["market_value"] / total_mv, 0.0)

    merged = merged.sort_values("market_value", ascending=False).reset_index(drop=True)
    return merged


def compute_curve_metrics(curve: pd.DataFrame) -> dict[str, float | None]:
    if curve.empty or len(curve) < 2:
        latest_nav = float(curve["nav"].iloc[-1]) if not curve.empty else None
        return {
            "latest_nav": latest_nav,
            "daily_pnl": None,
            "total_return": None,
            "annualized_return": None,
            "sharpe": None,
            "max_drawdown": None,
        }

    work = curve.copy().sort_values("timestamp").reset_index(drop=True)
    work["ret"] = work["nav"].pct_change()
    work["cummax"] = work["nav"].cummax()
    work["drawdown"] = work["nav"] / work["cummax"] - 1.0

    latest_nav = float(work["nav"].iloc[-1])
    prior_nav = float(work["nav"].iloc[-2])
    daily_pnl = latest_nav - prior_nav

    total_return = latest_nav / float(work["nav"].iloc[0]) - 1.0

    days = max((work["timestamp"].iloc[-1] - work["timestamp"].iloc[0]).days, 1)
    annualized_return = (1.0 + total_return) ** (365.0 / days) - 1.0 if total_return > -1 else None

    ret = work["ret"].dropna()
    sharpe = float((ret.mean() / ret.std()) * np.sqrt(252)) if len(ret) >= 2 and float(ret.std()) > 0 else None
    max_drawdown = float(work["drawdown"].min()) if not work["drawdown"].empty else None

    return {
        "latest_nav": latest_nav,
        "daily_pnl": daily_pnl,
        "total_return": total_return,
        "annualized_return": annualized_return,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
    }


def _load_live_net_liq(profile: str = "paper") -> float | None:
    try:
        rows = get_account_summary(profile=profile)
        if not rows:
            return None
        df = pd.DataFrame(rows)
        return _extract_tag_value(df, ["NetLiquidation", "EquityWithLoanValue"])
    except Exception:
        return None


def load_home_dashboard_data(profile: str = "paper") -> dict[str, Any]:
    curve = build_equity_curve()
    holdings = build_holdings_table()

    live_nav = _load_live_net_liq(profile=profile)
    now = pd.Timestamp.now().floor("s")

    if live_nav is not None:
        live_row = pd.DataFrame([{"timestamp": now, "nav": float(live_nav)}])
        if curve.empty:
            curve = live_row
        else:
            curve = pd.concat([curve, live_row], ignore_index=True)
            curve = curve.sort_values("timestamp").drop_duplicates("timestamp", keep="last").reset_index(drop=True)

    metrics = compute_curve_metrics(curve)

    warnings: list[str] = []
    if curve.empty:
        warnings.append("No paper account snapshots found.")
    if holdings.empty:
        warnings.append("No positions snapshot or latest prices found.")
    if live_nav is None:
        warnings.append("Live IBKR NetLiquidation could not be loaded.")

    return {
        "curve": curve,
        "holdings": holdings,
        "metrics": metrics,
        "warnings": warnings,
    }