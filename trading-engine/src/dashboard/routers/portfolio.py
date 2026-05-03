"""
portfolio.py - Portfolio Performance routes.

Replaces ``screens/paper_performance.py``. Returns the equity curve (with
optional SPX overlay and drawdown), summary metrics, and supporting metadata.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from fastapi import APIRouter, Query

from src.dashboard.routers._helpers import (
    df_to_records,
    get_profile,
    parse_iso_date,
)
from src.dashboard.utils import (
    best_available_nav_history,
    get_historical_nav,
    get_paper_start_date,
    load_latest_prices,
    load_positions_snapshot,
    synthesize_nav_history,
)


router = APIRouter()
PROJECT_ROOT = Path(__file__).resolve().parents[3]
PRICES_PARQUET = PROJECT_ROOT / "data" / "market" / "cleaned" / "prices" / "etf_prices_master.parquet"
TRADING_DAYS = 252


@router.get("/summary")
def summary(
    profile: str | None = None,
    start: str | None = Query(default=None, description="ISO date"),
    end: str | None = Query(default=None, description="ISO date"),
    extend_history: bool = False,
    include_spx: bool = True,
) -> dict[str, Any]:
    profile = profile or get_profile()
    paper_start = get_paper_start_date(profile)
    optimal_start = _portfolio_optimal_start()

    if extend_history:
        nav_df = _build_nav_history_with_anchor(profile, anchor=optimal_start)
    else:
        nav_df = best_available_nav_history(profile, extend_history=False)
    if nav_df is None or nav_df.empty:
        return {
            "profile": profile,
            "paper_start": paper_start.isoformat() if paper_start else None,
            "available": False,
            "message": f"No {profile} historical NAV data is available yet.",
            "nav_source": None,
            "series": [],
            "drawdown": [],
            "metrics": _empty_metrics(),
            "latest_nav": None,
            "range": _range_payload(None, None, None, None),
        }

    nav_df = nav_df.copy()
    nav_df["date"] = pd.to_datetime(nav_df["date"]).dt.date
    nav_df = nav_df.sort_values("date").drop_duplicates("date").reset_index(drop=True)

    nav_first = nav_df["date"].iloc[0]
    nav_last = nav_df["date"].iloc[-1]
    today = date.today()

    if extend_history:
        picker_min = min(nav_first, today - timedelta(days=40 * 365))
        picker_max = max(nav_last, today)
        default_start = nav_first
        default_end = max(nav_last, today)
    else:
        picker_min = paper_start or nav_first
        picker_max = max(nav_last, today)
        default_start = paper_start or nav_first
        default_end = max(nav_last, today)

    start_d = parse_iso_date(start, default_start)
    end_d = parse_iso_date(end, default_end)
    if start_d and end_d and start_d > end_d:
        return {
            "profile": profile,
            "available": True,
            "error": "Start date must be on or before end date.",
            "range": _range_payload(picker_min, picker_max, start_d, end_d),
            "series": [],
            "drawdown": [],
            "metrics": _empty_metrics(),
        }

    has_synth = "source" in nav_df.columns and (nav_df["source"] == "synthesized").any()
    has_snap = "source" in nav_df.columns and (nav_df["source"] == "snapshot").any()
    if has_synth and has_snap:
        nav_source = "snapshot where available, synthesized otherwise"
    elif has_synth:
        nav_source = "synthesized from current positions x historical prices"
    else:
        nav_source = "from account snapshots"

    mask = (nav_df["date"] >= start_d) & (nav_df["date"] <= end_d)
    plot_df = nav_df.loc[mask].copy()

    if plot_df.empty:
        return {
            "profile": profile,
            "available": True,
            "message": "No NAV data is available in the selected date range.",
            "range": _range_payload(picker_min, picker_max, start_d, end_d),
            "series": [],
            "drawdown": [],
            "metrics": _empty_metrics(),
        }

    plot_df["daily_returns"] = plot_df["nav"].pct_change().fillna(0.0)
    plot_df["equity"] = (1 + plot_df["daily_returns"]).cumprod()

    visible_sources = (
        set(plot_df.get("source", pd.Series(dtype=object)).dropna().unique())
        if "source" in plot_df.columns
        else set()
    )
    profile_label = profile.title()
    if visible_sources == {"snapshot"} or (
        "source" not in plot_df.columns and not extend_history
    ):
        portfolio_label = f"Actual {profile_label} Portfolio"
    elif "snapshot" in visible_sources:
        portfolio_label = f"{profile_label} Portfolio (snapshot + synthesized)"
    else:
        portfolio_label = f"{profile_label} Portfolio (synthesized)"

    series_payload: list[dict[str, Any]] = [
        {
            "name": portfolio_label,
            "kind": "portfolio",
            "points": _points(plot_df["date"], plot_df["equity"]),
        }
    ]
    drawdown_payload: list[dict[str, Any]] = [
        {
            "name": portfolio_label,
            "points": _drawdown_points(plot_df["date"], plot_df["equity"]),
        }
    ]

    benchmark_metrics: dict[str, Any] | None = None
    if include_spx:
        spx_df = _load_spx_prices(start_d)
        if spx_df is not None and not spx_df.empty:
            spx_df["date"] = pd.to_datetime(spx_df["date"]).dt.date
            spx_df = spx_df.loc[
                (spx_df["date"] >= start_d) & (spx_df["date"] <= end_d)
            ].sort_values("date")
            if not spx_df.empty:
                spx_df["daily_returns"] = spx_df["price"].pct_change().fillna(0.0)
                spx_df["equity"] = (1 + spx_df["daily_returns"]).cumprod()
                series_payload.append(
                    {
                        "name": "S&P 500",
                        "kind": "benchmark",
                        "points": _points(spx_df["date"], spx_df["equity"]),
                    }
                )
                drawdown_payload.append(
                    {
                        "name": "S&P 500",
                        "points": _drawdown_points(spx_df["date"], spx_df["equity"]),
                    }
                )
                benchmark_metrics = _compute_metrics(
                    spx_df["equity"], dates=pd.to_datetime(spx_df["date"])
                )

    metrics = _compute_metrics(
        plot_df["equity"], dates=pd.to_datetime(plot_df["date"])
    )

    # Alpha = annualized excess return over the benchmark on the same window.
    alpha: float | None = None
    if (
        benchmark_metrics
        and benchmark_metrics.get("annualized_return") is not None
        and metrics.get("annualized_return") is not None
    ):
        alpha = metrics["annualized_return"] - benchmark_metrics["annualized_return"]
    metrics["alpha"] = alpha
    metrics["benchmark"] = benchmark_metrics

    latest_nav = float(nav_df["nav"].iloc[-1]) if not nav_df.empty else None

    return {
        "profile": profile,
        "available": True,
        "extend_history": extend_history,
        "paper_start": paper_start.isoformat() if paper_start else None,
        "optimal_start": optimal_start.isoformat() if optimal_start else None,
        "nav_source": nav_source,
        "latest_nav": latest_nav,
        "range": _range_payload(picker_min, picker_max, start_d, end_d),
        "series": series_payload,
        "drawdown": drawdown_payload,
        "metrics": metrics,
    }


def _portfolio_optimal_start() -> date | None:
    """Latest first-trade date across currently-held ETFs.

    Anchors the hypothetical-backtest curve so it doesn't extend back into
    pre-listing periods (e.g. 1993, before most of these ETFs existed).
    """
    try:
        positions = load_positions_snapshot()
        prices = load_latest_prices()
    except Exception:  # noqa: BLE001
        return None
    if positions is None or positions.empty or prices is None or prices.empty:
        return None
    held = (
        positions.loc[positions["shares"] != 0, "symbol"]
        .astype(str)
        .str.upper()
        .unique()
        .tolist()
    )
    if not held:
        return None
    px = prices.copy()
    px["symbol"] = px["symbol"].astype(str).str.upper()
    px["date"] = pd.to_datetime(px["date"], errors="coerce").dt.date
    earliest = (
        px[px["symbol"].isin(held)]
        .dropna(subset=["date"])
        .groupby("symbol")["date"]
        .min()
    )
    if earliest.empty:
        return None
    return earliest.max()


def _build_nav_history_with_anchor(
    profile: str, *, anchor: date | None
) -> pd.DataFrame:
    """Like best_available_nav_history(extend_history=True) but anchored.

    The default best_available_nav_history extends synthesis back to the master
    price file's earliest date. That over-counts because not every held ETF
    existed back then; the curve has crazy jumps as ETFs come online. Anchoring
    to ``anchor`` (the latest first-trade-date across held ETFs) keeps the
    backtest in a window where every position can actually be priced.
    """
    snap = get_historical_nav(profile)
    synth = synthesize_nav_history(profile, start=anchor)

    snap = snap.copy() if snap is not None and not snap.empty else pd.DataFrame()
    synth = (
        synth[["date", "nav"]].copy()
        if synth is not None and not synth.empty
        else pd.DataFrame()
    )

    if not snap.empty:
        snap["date"] = pd.to_datetime(snap["date"]).dt.normalize()
        snap["source"] = "snapshot"
    if not synth.empty:
        synth["date"] = pd.to_datetime(synth["date"]).dt.normalize()
        synth["source"] = "synthesized"

    if snap.empty and synth.empty:
        return pd.DataFrame(columns=["date", "nav", "source"])
    if snap.empty:
        return synth.sort_values("date").reset_index(drop=True)
    if synth.empty:
        return snap.sort_values("date").reset_index(drop=True)

    snap_dates = set(snap["date"].tolist())
    synth_only = synth.loc[~synth["date"].isin(snap_dates)]
    return (
        pd.concat([snap, synth_only], ignore_index=True)
        .sort_values("date")
        .reset_index(drop=True)
    )


# ── Helpers ──────────────────────────────────────────────────────────────────

def _points(dates: pd.Series, values: pd.Series) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for d, v in zip(dates, values):
        if pd.isna(v):
            continue
        f = float(v)
        if not np.isfinite(f):
            continue
        out.append({"date": pd.Timestamp(d).strftime("%Y-%m-%d"), "value": f})
    return out


def _drawdown_points(dates: pd.Series, equity: pd.Series) -> list[dict[str, Any]]:
    s = equity.dropna().reset_index(drop=True)
    if s.empty:
        return []
    running_max = s.cummax()
    drawdown = (s / running_max) - 1.0
    return _points(dates.reset_index(drop=True), drawdown)


def _load_spx_prices(start_d: date) -> pd.DataFrame | None:
    try:
        if PRICES_PARQUET.exists():
            df = pd.read_parquet(PRICES_PARQUET)
        else:
            csv = PRICES_PARQUET.with_suffix(".csv")
            if not csv.exists():
                return None
            df = pd.read_csv(csv)
    except Exception:  # noqa: BLE001
        return None
    df.columns = [c.lower() for c in df.columns]
    spy = df[df["symbol"].astype(str).str.upper() == "SPY"].copy()
    if spy.empty:
        return None
    spy["date"] = pd.to_datetime(spy["date"]).dt.date
    spy = spy[spy["date"] >= start_d].sort_values("date")
    return spy[["date", "close"]].rename(columns={"close": "price"})


def _empty_metrics() -> dict[str, Any]:
    return {
        "total_return": None,
        "annualized_return": None,
        "annualized_volatility": None,
        "max_drawdown": None,
        "sharpe_ratio": None,
        "years_observed": None,
        "days_observed": None,
    }


def _compute_metrics(
    equity: pd.Series, dates: pd.Series | None = None
) -> dict[str, Any]:
    metrics = _empty_metrics()
    s = equity.dropna()
    if s.empty or len(s) < 2:
        return metrics

    metrics["total_return"] = float(s.iloc[-1] - 1.0)
    daily_returns = s.pct_change().dropna()
    if len(daily_returns) > 1:
        metrics["annualized_volatility"] = float(
            daily_returns.std() * np.sqrt(TRADING_DAYS)
        )

    years: float | None = None
    if dates is not None:
        try:
            d0 = pd.to_datetime(dates.iloc[0])
            d1 = pd.to_datetime(dates.iloc[-1])
            cal_days = max((d1 - d0).days, 0)
            if cal_days > 0:
                years = cal_days / 365.25
        except Exception:  # noqa: BLE001
            years = None
    if years is None:
        years = len(s) / TRADING_DAYS

    if years and years >= 0.25:
        ending = float(s.iloc[-1])
        if ending > 0:
            metrics["annualized_return"] = ending ** (1.0 / years) - 1.0

    running_max = s.cummax()
    drawdown = (s / running_max) - 1
    metrics["max_drawdown"] = float(drawdown.min())

    ar = metrics.get("annualized_return")
    av = metrics.get("annualized_volatility")
    if ar is not None and av is not None and av and av > 0:
        metrics["sharpe_ratio"] = ar / av

    metrics["years_observed"] = years
    metrics["days_observed"] = years * 365.25 if years else None
    return metrics


def _range_payload(
    picker_min: date | None,
    picker_max: date | None,
    start_d: date | None,
    end_d: date | None,
) -> dict[str, Any]:
    return {
        "picker_min": picker_min.isoformat() if picker_min else None,
        "picker_max": picker_max.isoformat() if picker_max else None,
        "start": start_d.isoformat() if start_d else None,
        "end": end_d.isoformat() if end_d else None,
    }
