"""
model.py - Model Analysis routes.

Two views:
  - GET  /model/models     -> aggregate / per-model backtest equity curves
  - GET  /model/prices     -> indexed-to-100 ETF price history
  - GET  /model/meta       -> available models + last audit dates
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from fastapi import APIRouter, Query

from src.dashboard.routers._helpers import (
    df_to_records,
    get_optimizer,
    get_profile,
    parse_iso_date,
)
from src.dashboard.utils import (
    get_active_optimizer,
    get_historical_weights,
    get_latest_production_audit,
    get_latest_simulations_audit,
    get_model_backtest,
    get_portfolio_backtest,
    get_production_audit_models,
    get_reduced_portfolio_backtest,
    get_spx_prices_from_date,
    load_latest_prices,
)


router = APIRouter()
TRADING_DAYS = 252


@router.get("/meta")
def meta(profile: str | None = None) -> dict[str, Any]:
    profile = profile or get_profile()
    models = get_production_audit_models(profile)
    # Reduced-portfolio (marginal) artifacts aren't produced locally, so the
    # marginal options would always come back empty. Hide them from the picker
    # rather than confusing the user with "no data" rows.
    return {
        "profile": profile,
        "models": models,
        "available_options": models,
        "active_optimizer": get_active_optimizer(profile),
        "latest_production_audit": _iso(get_latest_production_audit(profile)),
        "latest_simulations_audit": _iso(get_latest_simulations_audit(profile)),
    }


@router.get("/models")
def models_view(
    profile: str | None = None,
    selected: list[str] = Query(default_factory=list),
    start: str | None = None,
    end: str | None = None,
    show_aggregate_portfolio: bool = False,
    show_spx: bool = False,
    preset: str | None = Query(
        default=None,
        description="One of: 3M, 6M, 1Y, YTD. When set, overrides start/end.",
    ),
) -> dict[str, Any]:
    profile = profile or get_profile()
    profile_label = profile.title()

    today = date.today()
    if preset == "3M":
        start_d, end_d = today - timedelta(days=90), today
    elif preset == "6M":
        start_d, end_d = today - timedelta(days=180), today
    elif preset == "1Y":
        start_d, end_d = today - timedelta(days=365), today
    elif preset == "YTD":
        start_d, end_d = date(today.year, 1, 1), today
    else:
        start_d = parse_iso_date(start, datetime(2024, 1, 1).date())
        end_d = parse_iso_date(end, today)

    if start_d > end_d:
        return {
            "profile": profile,
            "error": "Start date must be on or before end date.",
            "series": [],
            "metrics": {},
            "range": _range(start_d, end_d),
        }

    series: list[dict[str, Any]] = []
    metrics: dict[str, Any] = {}
    optimizer_name = get_active_optimizer(profile)

    for model in selected:
        try:
            if model.endswith("_marginal"):
                base_model = model[:-9]
                df_base = get_portfolio_backtest(optimizer_name, profile=profile)
                df_reduced = get_reduced_portfolio_backtest(
                    base_model, optimizer_name=optimizer_name, profile=profile
                )
                if df_base is None or df_base.empty or df_reduced is None or df_reduced.empty:
                    continue
                df = _marginal_returns(df_base, df_reduced)
            else:
                df = get_model_backtest(model, profile=profile)
        except Exception as exc:  # noqa: BLE001
            metrics[model] = {"error": str(exc)}
            continue
        df = _bound_returns(df, start_d, end_d)
        if df is None or df.empty:
            continue
        equity = (1 + df["daily_return"].fillna(0)).cumprod()
        series.append({"name": model, "points": _points(df["date"], equity)})
        metrics[model] = _calc_metrics(df, equity)

    if show_aggregate_portfolio:
        try:
            agg = get_portfolio_backtest(optimizer_name, profile=profile)
        except Exception:  # noqa: BLE001
            agg = None
        agg = _bound_returns(agg, start_d, end_d)
        if agg is not None and not agg.empty:
            equity = (1 + agg["daily_return"].fillna(0)).cumprod()
            label = f"Aggregate {profile_label} Portfolio"
            series.append({"name": label, "points": _points(agg["date"], equity)})
            metrics["Portfolio"] = _calc_metrics(agg, equity)

    if show_spx:
        try:
            spx = get_spx_prices_from_date(start_d)
        except Exception:  # noqa: BLE001
            spx = None
        if spx is not None and not spx.empty:
            spx = spx.copy()
            spx["date"] = pd.to_datetime(spx["date"])
            mask = (spx["date"].dt.date >= start_d) & (spx["date"].dt.date <= end_d)
            spx = spx.loc[mask].copy()
            price_col = "close" if "close" in spx.columns else (
                "price" if "price" in spx.columns else None
            )
            if price_col and not spx.empty:
                returns = spx.set_index("date")[price_col].sort_index().pct_change().fillna(0)
                equity = (1 + returns).cumprod()
                series.append(
                    {"name": "S&P 500", "points": _points(equity.index, equity.values)}
                )

    return {
        "profile": profile,
        "selected": selected,
        "range": _range(start_d, end_d),
        "series": series,
        "metrics": metrics,
        "optimizer": optimizer_name,
    }


@router.get("/prices")
def prices_view(
    symbols: list[str] = Query(default_factory=list),
    start: str | None = None,
    end: str | None = None,
    normalize: str = Query(default="indexed", description="indexed | raw"),
) -> dict[str, Any]:
    df = load_latest_prices()
    if df is None or df.empty:
        return {
            "available": False,
            "all_symbols": [],
            "series": [],
            "latest_quotes": [],
            "range": _range(None, None),
        }
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["symbol"] = df["symbol"].astype(str).str.upper()
    all_symbols = sorted(df["symbol"].unique().tolist())

    if not symbols:
        defaults = [s for s in ("SPY", "QQQ", "GLD", "TLT") if s in all_symbols]
        symbols = defaults or all_symbols[:4]

    date_min = df["date"].min().date()
    date_max = df["date"].max().date()
    start_d = parse_iso_date(start, max(date(2020, 1, 1), date_min))
    end_d = parse_iso_date(end, date_max)

    filtered = df[
        df["symbol"].isin([s.upper() for s in symbols])
        & (df["date"] >= pd.Timestamp(start_d))
        & (df["date"] <= pd.Timestamp(end_d))
    ].copy()
    if filtered.empty:
        return {
            "available": True,
            "message": "No price data in the selected range.",
            "all_symbols": all_symbols,
            "series": [],
            "latest_quotes": [],
            "range": _range(start_d, end_d),
            "selected_symbols": [s.upper() for s in symbols],
            "normalize": normalize,
        }

    filtered = filtered.sort_values(["symbol", "date"])
    if normalize == "indexed":
        filtered["value"] = filtered.groupby("symbol")["close"].transform(
            lambda s: s / s.iloc[0] * 100
        )
        y_label = "Indexed to 100"
    else:
        filtered["value"] = filtered["close"]
        y_label = "Close"

    series_payload = [
        {
            "name": symbol,
            "points": _points(group["date"], group["value"]),
        }
        for symbol, group in filtered.groupby("symbol", sort=True)
    ]

    quotes = (
        filtered.sort_values("date")
        .groupby("symbol", as_index=False)
        .tail(1)[["symbol", "date", "close"]]
        .rename(columns={"close": "last_close", "date": "as_of"})
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    return {
        "available": True,
        "all_symbols": all_symbols,
        "selected_symbols": [s.upper() for s in symbols],
        "normalize": normalize,
        "y_label": y_label,
        "range": _range(start_d, end_d),
        "date_min": date_min.isoformat(),
        "date_max": date_max.isoformat(),
        "series": series_payload,
        "latest_quotes": df_to_records(quotes),
    }


@router.get("/weights")
def weights_view(profile: str | None = None) -> dict[str, Any]:
    """Stacked area chart fallback - which ETFs the active optimizer picked."""
    profile = profile or get_profile()
    weights = get_historical_weights()
    if weights is None or weights.empty:
        return {"available": False, "rows": [], "tickers": []}
    weights = weights.copy()
    weights["date"] = pd.to_datetime(weights["date"])
    tickers = [c for c in weights.columns if c != "date"]
    weights[tickers] = weights[tickers].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    rows = [
        {
            "date": pd.Timestamp(row["date"]).strftime("%Y-%m-%d"),
            **{t: float(row[t]) for t in tickers},
        }
        for _, row in weights.iterrows()
    ]
    return {"available": True, "tickers": tickers, "rows": rows}


# ── Helpers ──────────────────────────────────────────────────────────────────

def _iso(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _points(dates, values) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for d, v in zip(dates, values):
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if not np.isfinite(f):
            continue
        out.append({"date": pd.Timestamp(d).strftime("%Y-%m-%d"), "value": f})
    return out


def _bound_returns(df: pd.DataFrame | None, start_d: date, end_d: date) -> pd.DataFrame | None:
    if df is None or df.empty:
        return df
    if {"date", "daily_return"} - set(df.columns):
        return None
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values("date")
    mask = (out["date"].dt.date >= start_d) & (out["date"].dt.date <= end_d)
    return out.loc[mask].reset_index(drop=True)


def _marginal_returns(df_base: pd.DataFrame, df_reduced: pd.DataFrame) -> pd.DataFrame:
    base = df_base.copy()
    reduced = df_reduced.copy()
    base["date"] = pd.to_datetime(base["date"])
    reduced["date"] = pd.to_datetime(reduced["date"])
    merged = pd.merge(
        base[["date", "daily_return"]],
        reduced[["date", "daily_return"]],
        on="date",
        suffixes=("_base", "_reduced"),
    )
    merged["daily_return"] = (
        merged["daily_return_base"] - merged["daily_return_reduced"]
    )
    return merged[["date", "daily_return"]]


def _calc_metrics(df: pd.DataFrame, equity: pd.Series) -> dict[str, float]:
    if equity.empty:
        return {}
    total_return = float(equity.iloc[-1]) - 1.0
    years = len(equity) / TRADING_DAYS if len(equity) > 1 else 1.0
    annualized_return = (
        (1 + total_return) ** (1 / years) - 1
        if years > 0 and (1 + total_return) > 0
        else 0.0
    )
    daily_vol = float(df["daily_return"].std()) if len(df) > 1 else 0.0
    annualized_vol = daily_vol * np.sqrt(TRADING_DAYS)
    sharpe = (annualized_return / annualized_vol) if annualized_vol > 0 else 0.0
    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1
    max_dd = float(drawdown.min()) if not drawdown.empty else 0.0
    avg_dd = float(drawdown.mean()) if not drawdown.empty else 0.0

    daily = df["daily_return"]
    win_rate = float((daily > 0).sum() / max((daily != 0).sum(), 1))
    avg_daily = float(daily.mean()) if len(daily) else 0.0

    negatives = daily[daily < 0]
    downside_vol = float(negatives.std()) * np.sqrt(TRADING_DAYS) if len(negatives) > 1 else 0.0
    sortino = (annualized_return / downside_vol) if downside_vol > 0 else sharpe

    return {
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": annualized_vol,
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "max_drawdown": max_dd,
        "avg_drawdown": avg_dd,
        "win_rate": win_rate,
        "avg_daily_return": avg_daily,
    }


def _range(start_d: date | None, end_d: date | None) -> dict[str, Any]:
    return {
        "start": start_d.isoformat() if start_d else None,
        "end": end_d.isoformat() if end_d else None,
    }
