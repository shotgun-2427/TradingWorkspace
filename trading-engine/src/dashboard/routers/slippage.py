"""
slippage.py - Slippage Analysis routes.
"""
from __future__ import annotations

from datetime import date
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
from src.dashboard.utils.slippage import (
    ABSOLUTE_SLIPPAGE_MODE,
    SIGNED_SLIPPAGE_MODE,
    SLIPPAGE_MODE_OPTIONS,
    SIMULATED_SLIPPAGE_BPS,
    build_daily_slippage_summary,
    build_slippage_overview_summary,
    get_earliest_slippage_audit,
    get_latest_slippage_audit,
    get_slippage_history_bundle,
    get_slippage_metric_column,
    get_slippage_metric_label,
    get_slippage_preview_root,
)


router = APIRouter()


@router.get("/meta")
def meta(profile: str | None = None) -> dict[str, Any]:
    profile = profile or get_profile()
    preview_root = get_slippage_preview_root()
    earliest = get_earliest_slippage_audit(profile=profile, preview_root=preview_root)
    latest = get_latest_slippage_audit(profile=profile, preview_root=preview_root)
    return {
        "profile": profile,
        "preview_root": str(preview_root) if preview_root else None,
        "earliest": earliest.isoformat() if earliest else None,
        "latest": latest.isoformat() if latest else None,
        "modes": SLIPPAGE_MODE_OPTIONS,
        "default_mode": SIGNED_SLIPPAGE_MODE,
        "simulated_slippage_bps": SIMULATED_SLIPPAGE_BPS,
    }


@router.get("/report")
def report(
    profile: str | None = None,
    start: str | None = None,
    end: str | None = None,
    mode: str = Query(default=SIGNED_SLIPPAGE_MODE),
) -> dict[str, Any]:
    profile = profile or get_profile()
    preview_root = get_slippage_preview_root()
    earliest = get_earliest_slippage_audit(profile=profile, preview_root=preview_root)
    latest = get_latest_slippage_audit(profile=profile, preview_root=preview_root)
    if earliest is None or latest is None:
        return {
            "available": False,
            "message": f"No {profile} slippage reports available yet.",
        }

    start_d = parse_iso_date(start, earliest)
    end_d = parse_iso_date(end, latest)
    if start_d > end_d:
        return {"available": True, "error": "Start date must be on or before end date."}

    bundle = get_slippage_history_bundle(
        start_d,
        end_d,
        profile=profile,
        preview_root_str=str(preview_root) if preview_root else None,
    )
    history: pd.DataFrame = bundle["history"]
    legacy_skipped = bundle["legacy_reports_skipped"]
    reports_loaded = bundle["reports_loaded"]
    if history is None or history.empty:
        return {
            "available": True,
            "message": "No v2 slippage data in selected range.",
            "legacy_reports_skipped": legacy_skipped,
            "reports_loaded": reports_loaded,
        }

    metric_column = get_slippage_metric_column(mode)
    metric_label = get_slippage_metric_label(mode)
    daily = build_daily_slippage_summary(history, start_d, end_d)
    overview = build_slippage_overview_summary(history)

    asset_summary = _asset_summary(history, metric_column=metric_column)
    ticker_table = _ticker_table(history)

    daily_rows = []
    for _, row in daily.iterrows():
        daily_rows.append(
            {
                "trade_date": pd.Timestamp(row["trade_date"]).strftime("%Y-%m-%d"),
                "trades": int(row.get("trades", 0) or 0),
                "slippage_impact_bps": _safe_float(row.get("slippage_impact_bps")),
                "commission_impact_bps": _safe_float(row.get("commission_impact_bps")),
                "cumulative_execution_impact_bps": _safe_float(
                    row.get("cumulative_execution_impact_bps")
                ),
                "total_slippage_cost": _safe_float(row.get("total_slippage_cost")),
                "total_commission": _safe_float(row.get("total_commission")),
                "cumulative_execution_cost": _safe_float(row.get("cumulative_execution_cost")),
            }
        )

    scatter_rows = [
        {
            "ticker": str(row["ticker"]),
            "action": str(row.get("action", "")),
            "nav_pct": _safe_float(row.get("nav_pct")),
            "metric": _safe_float(row.get(metric_column)),
            "filled_quantity": _safe_float(row.get("filled_quantity")),
            "fill_ratio": _safe_float(row.get("fill_ratio")),
            "gross_cost_bps": _safe_float(row.get("gross_cost_bps")),
            "net_cost_bps": _safe_float(row.get("net_cost_bps")),
        }
        for _, row in history.iterrows()
    ]

    return {
        "available": True,
        "profile": profile,
        "range": {"start": start_d.isoformat(), "end": end_d.isoformat()},
        "earliest": earliest.isoformat(),
        "latest": latest.isoformat(),
        "mode": mode,
        "metric_label": metric_label,
        "metric_column": metric_column,
        "preview_root": str(preview_root) if preview_root else None,
        "legacy_reports_skipped": legacy_skipped,
        "reports_loaded": reports_loaded,
        "overview": _serialise_overview(overview),
        "calibration": {
            "simulated_slippage_bps": SIMULATED_SLIPPAGE_BPS,
            "weighted_gross_cost_bps": _safe_float(overview.get("weighted_gross_cost_bps")),
            "weighted_net_cost_bps": _safe_float(overview.get("weighted_net_cost_bps")),
            "delta_bps": _safe_float(
                (overview.get("weighted_gross_cost_bps") or 0) - SIMULATED_SLIPPAGE_BPS
            ),
        },
        "daily": daily_rows,
        "asset_summary": df_to_records(asset_summary),
        "scatter": scatter_rows,
        "ticker_table": df_to_records(ticker_table),
        "raw": df_to_records(history.sort_values(["trade_date", "ticker", "action"]).head(500)),
    }


# ── Helpers ─────────────────────────────────────────────────────────────────

def _asset_summary(history: pd.DataFrame, *, metric_column: str) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(
            columns=["ticker", "average_metric_bps", "trades", "total_fill_notional"]
        )
    work = history.copy()
    work["abs_fill_notional"] = work["fill_notional"].abs()
    summary = (
        work.groupby("ticker", as_index=False)
        .agg(
            trades=("ticker", "count"),
            total_fill_notional=("abs_fill_notional", "sum"),
            average_metric_bps=(metric_column, "mean"),
        )
        .fillna({"average_metric_bps": 0.0})
    )
    return summary.sort_values("average_metric_bps", ascending=False).reset_index(drop=True)


def _ticker_table(history: pd.DataFrame) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame(
            columns=[
                "ticker",
                "trades",
                "mean_signed_slippage_bps",
                "mean_absolute_slippage_bps",
                "mean_commission_bps",
                "mean_net_cost_bps",
                "mean_gross_cost_bps",
                "median_fill_ratio",
            ]
        )
    return (
        history.groupby("ticker")
        .agg(
            trades=("ticker", "count"),
            mean_signed_slippage_bps=("slippage_bps", "mean"),
            mean_absolute_slippage_bps=("absolute_slippage_bps", "mean"),
            mean_commission_bps=("commission_bps", "mean"),
            mean_net_cost_bps=("net_cost_bps", "mean"),
            mean_gross_cost_bps=("gross_cost_bps", "mean"),
            median_fill_ratio=("fill_ratio", "median"),
        )
        .round(2)
        .reset_index()
    )


def _serialise_overview(overview: dict[str, Any]) -> dict[str, Any]:
    return {k: _safe_float(v) if isinstance(v, (int, float, np.floating, np.integer)) else v
            for k, v in overview.items()}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(f):
        return None
    return f
