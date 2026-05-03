"""
slippage.py — Local-first slippage analytics helpers.

Mirrors the API of the original Trading-engine dashboard but reads from local
artifacts instead of a cloud bucket. Inventory is discovered by scanning two
locations:

  1. ``artifacts/reports/<YYYY-MM-DD>/slippage_report.csv`` for per-day reports.
  2. ``data/broker/reconciliations/<YYYY-MM-DD>/slippage_report.csv`` for
     reconciliation-side reports.

If the ``SLIPPAGE_PREVIEW_DIR`` environment variable is set it overrides both
locations and the helpers read only from there.
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError

from src.dashboard._cache import cache_data as _cache_data


# Compatibility shim - this module's loaders used ``@st.cache_data``; the
# FastAPI rewrite swaps Streamlit for a small in-process cache. We expose a
# class (not an instance) so ``st.cache_data`` is a plain function reference.
class _StShim:
    cache_data = staticmethod(_cache_data)


st = _StShim

SIGNED_SLIPPAGE_MODE = "Signed Slippage"
ABSOLUTE_SLIPPAGE_MODE = "Absolute Slippage"
SLIPPAGE_MODE_OPTIONS = [SIGNED_SLIPPAGE_MODE, ABSOLUTE_SLIPPAGE_MODE]
SIMULATED_SLIPPAGE_BPS = 1.0
SLIPPAGE_REPORT_FILE_NAME = "slippage_report.csv"
SLIPPAGE_REPORT_KIND_V2 = "v2"
SLIPPAGE_REPORT_KIND_LEGACY = "legacy"

SLIPPAGE_V2_REQUIRED_COLUMNS = {
    "report_version",
    "trade_date",
    "ticker",
    "action",
    "order_quantity",
    "filled_quantity",
    "fill_ratio",
    "decision_price",
    "fill_price",
    "slippage_bps",
    "absolute_slippage_bps",
    "commission",
    "commission_bps",
    "net_cost_bps",
    "gross_cost_bps",
    "order_notional",
    "fill_notional",
    "nav_at_order",
    "nav_pct",
}

SLIPPAGE_V2_NUMERIC_COLUMNS = [
    "order_quantity",
    "filled_quantity",
    "fill_ratio",
    "decision_price",
    "fill_price",
    "slippage_bps",
    "absolute_slippage_bps",
    "commission",
    "commission_bps",
    "net_cost_bps",
    "gross_cost_bps",
    "order_notional",
    "fill_notional",
    "nav_at_order",
    "nav_pct",
]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _default_report_roots(profile: str) -> list[Path]:
    root = _project_root()
    return [
        root / "artifacts" / "reports",
        root / "data" / "broker" / "reconciliations" / profile,
        root / "data" / "broker" / "reconciliations",
    ]


def is_v2_slippage_report(report: pd.DataFrame) -> bool:
    if report.empty:
        return False
    if not SLIPPAGE_V2_REQUIRED_COLUMNS.issubset(report.columns):
        return False
    versions = pd.to_numeric(report["report_version"], errors="coerce").dropna().unique()
    return len(versions) > 0 and set(versions) == {2}


def normalize_v2_slippage_report(report: pd.DataFrame) -> pd.DataFrame:
    normalized = report.copy()
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"])
    for column in SLIPPAGE_V2_NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized["report_version"] = pd.to_numeric(
        normalized["report_version"],
        errors="coerce",
    ).astype("Int64")
    return normalized.sort_values(["trade_date", "ticker", "action"]).reset_index(drop=True)


def _classify_slippage_report(report: pd.DataFrame) -> str:
    if is_v2_slippage_report(report):
        return SLIPPAGE_REPORT_KIND_V2
    return SLIPPAGE_REPORT_KIND_LEGACY


def _read_csv_head_from_path(report_path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(report_path, nrows=1)
    except (EmptyDataError, FileNotFoundError):
        return pd.DataFrame()


def get_slippage_metric_column(mode: str) -> str:
    if mode == ABSOLUTE_SLIPPAGE_MODE:
        return "absolute_slippage_bps"
    return "slippage_bps"


def get_slippage_metric_label(mode: str) -> str:
    if mode == ABSOLUTE_SLIPPAGE_MODE:
        return "Absolute Slippage"
    return "Signed Slippage"


def get_slippage_reference_bps(mode: str) -> float:
    if mode == ABSOLUTE_SLIPPAGE_MODE:
        return SIMULATED_SLIPPAGE_BPS
    return 0.0


def get_slippage_reference_label(mode: str) -> str:
    if mode == ABSOLUTE_SLIPPAGE_MODE:
        return "Simulated Slippage"
    return "Zero Baseline"


def get_slippage_preview_root() -> Path | None:
    preview_root = os.getenv("SLIPPAGE_PREVIEW_DIR")
    if not preview_root:
        return None
    return Path(preview_root).expanduser()


def _iter_reports_in_directory(root: Path) -> list[tuple[date, Path]]:
    if not root.exists():
        return []

    reports: list[tuple[date, Path]] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            report_date = date.fromisoformat(child.name)
        except ValueError:
            continue
        report_path = child / SLIPPAGE_REPORT_FILE_NAME
        if report_path.exists():
            reports.append((report_date, report_path))
    return sorted(reports, key=lambda item: item[0])


@st.cache_data(ttl=3600)
def list_slippage_report_inventory(
    *,
    profile: str = "paper",
    preview_root_str: str | None = None,
) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    roots: list[Path] = []

    if preview_root_str:
        roots.append(Path(preview_root_str).expanduser())
    else:
        roots.extend(_default_report_roots(profile))

    seen: set[tuple[date, str]] = set()
    for root in roots:
        for report_date, report_path in _iter_reports_in_directory(root):
            key = (report_date, str(report_path))
            if key in seen:
                continue
            seen.add(key)
            inventory.append(
                {
                    "report_date": report_date,
                    "source": str(report_path),
                    "kind": _classify_slippage_report(
                        _read_csv_head_from_path(report_path)
                    ),
                }
            )

    return sorted(inventory, key=lambda item: item["report_date"])


def _get_v2_report_dates(
    *,
    profile: str = "paper",
    preview_root: Path | None = None,
) -> list[date]:
    preview_root_str = str(preview_root) if preview_root is not None else None
    return [
        report["report_date"]
        for report in list_slippage_report_inventory(
            profile=profile,
            preview_root_str=preview_root_str,
        )
        if report["kind"] == SLIPPAGE_REPORT_KIND_V2
    ]


def get_earliest_slippage_audit(
    profile: str = "paper",
    preview_root: Path | None = None,
) -> date | None:
    v2_dates = _get_v2_report_dates(profile=profile, preview_root=preview_root)
    return v2_dates[0] if v2_dates else None


def get_latest_slippage_audit(
    profile: str = "paper",
    preview_root: Path | None = None,
) -> date | None:
    v2_dates = _get_v2_report_dates(profile=profile, preview_root=preview_root)
    return v2_dates[-1] if v2_dates else None


def build_daily_slippage_summary(
    history: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    enriched = history.copy()
    enriched["trade_date"] = pd.to_datetime(enriched["trade_date"])
    enriched["decision_notional_filled"] = (
        enriched["decision_price"] * enriched["filled_quantity"]
    )
    enriched["slippage_cost"] = (
        enriched["slippage_bps"] / 10_000 * enriched["decision_notional_filled"]
    )
    enriched["total_execution_cost"] = enriched["slippage_cost"] + enriched["commission"]

    daily = (
        enriched.groupby("trade_date")
        .agg(
            mean_signed_slippage_bps=("slippage_bps", "mean"),
            mean_absolute_slippage_bps=("absolute_slippage_bps", "mean"),
            mean_net_cost_bps=("net_cost_bps", "mean"),
            mean_gross_cost_bps=("gross_cost_bps", "mean"),
            mean_commission_bps=("commission_bps", "mean"),
            total_commission=("commission", "sum"),
            total_slippage_cost=("slippage_cost", "sum"),
            total_execution_cost=("total_execution_cost", "sum"),
            daily_nav=("nav_at_order", "median"),
            trades=("ticker", "count"),
        )
        .reset_index()
    )
    daily["slippage_impact_bps"] = (
        daily["total_slippage_cost"] / daily["daily_nav"] * 10_000
    )
    daily["commission_impact_bps"] = (
        daily["total_commission"] / daily["daily_nav"] * 10_000
    )
    daily["total_execution_impact_bps"] = (
        daily["total_execution_cost"] / daily["daily_nav"] * 10_000
    )
    daily["cumulative_execution_impact_bps"] = daily["total_execution_impact_bps"].cumsum()
    daily["cumulative_execution_cost"] = daily["total_execution_cost"].cumsum()

    full_range = pd.DataFrame(
        {"trade_date": pd.date_range(start=start_date, end=end_date, freq="D")}
    )
    daily = full_range.merge(daily, on="trade_date", how="left")
    daily["trades"] = daily["trades"].fillna(0).astype(int)
    zero_fill_columns = [
        "total_commission",
        "total_slippage_cost",
        "total_execution_cost",
        "slippage_impact_bps",
        "commission_impact_bps",
        "total_execution_impact_bps",
        "cumulative_execution_cost",
    ]
    for column in zero_fill_columns:
        daily[column] = daily[column].fillna(0.0)
    daily["cumulative_execution_impact_bps"] = (
        daily["total_execution_impact_bps"].cumsum()
    )
    daily["cumulative_execution_cost"] = daily["total_execution_cost"].cumsum()
    return daily.sort_values("trade_date").reset_index(drop=True)


def _to_bps(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator * 10_000


def build_slippage_overview_summary(history: pd.DataFrame) -> dict[str, float | int]:
    trades = int(len(history))
    if trades == 0:
        return {
            "trades": 0,
            "total_fill_notional": 0.0,
            "total_commission_dollars": 0.0,
            "signed_slippage_dollars": 0.0,
            "absolute_slippage_dollars": 0.0,
            "total_net_cost_dollars": 0.0,
            "weighted_slippage_bps": 0.0,
            "weighted_commission_bps": 0.0,
            "weighted_net_cost_bps": 0.0,
            "weighted_gross_cost_bps": 0.0,
            "favorable_fill_rate": 0.0,
            "adverse_fill_rate": 0.0,
            "flat_fill_rate": 0.0,
            "small_trade_count": 0,
            "small_trade_share": 0.0,
            "one_share_trade_count": 0,
        }

    total_fill_notional = float(history["fill_notional"].abs().sum())
    total_commission_dollars = float(history["commission"].sum())
    decision_notional_filled = (
        history["decision_price"] * history["filled_quantity"]
    ).abs()
    signed_slippage_dollars = float(
        ((history["slippage_bps"] / 10_000) * decision_notional_filled).sum()
    )
    absolute_slippage_dollars = float(
        ((history["absolute_slippage_bps"] / 10_000) * decision_notional_filled).sum()
    )
    total_net_cost_dollars = total_commission_dollars + signed_slippage_dollars

    return {
        "trades": trades,
        "total_fill_notional": total_fill_notional,
        "total_commission_dollars": total_commission_dollars,
        "signed_slippage_dollars": signed_slippage_dollars,
        "absolute_slippage_dollars": absolute_slippage_dollars,
        "total_net_cost_dollars": total_net_cost_dollars,
        "weighted_slippage_bps": _to_bps(
            signed_slippage_dollars,
            total_fill_notional,
        ),
        "weighted_commission_bps": _to_bps(
            total_commission_dollars,
            total_fill_notional,
        ),
        "weighted_net_cost_bps": _to_bps(
            total_net_cost_dollars,
            total_fill_notional,
        ),
        "weighted_gross_cost_bps": _to_bps(
            total_commission_dollars + absolute_slippage_dollars,
            total_fill_notional,
        ),
        "favorable_fill_rate": float((history["slippage_bps"] < 0).mean()),
        "adverse_fill_rate": float((history["slippage_bps"] > 0).mean()),
        "flat_fill_rate": float((history["slippage_bps"] == 0).mean()),
        "small_trade_count": int((history["fill_notional"].abs() < 500).sum()),
        "small_trade_share": float((history["fill_notional"].abs() < 500).mean()),
        "one_share_trade_count": int((history["filled_quantity"] == 1).sum()),
    }


@st.cache_data
def get_slippage_history_bundle(
    start_date: date,
    end_date: date,
    *,
    profile: str = "paper",
    preview_root_str: str | None = None,
) -> dict[str, Any]:
    frames: list[pd.DataFrame] = []
    legacy_reports_skipped = 0
    reports_loaded = 0
    inventory = list_slippage_report_inventory(
        profile=profile,
        preview_root_str=preview_root_str,
    )

    for report in inventory:
        report_date = report["report_date"]
        if not (start_date <= report_date <= end_date):
            continue

        if report["kind"] != SLIPPAGE_REPORT_KIND_V2:
            legacy_reports_skipped += 1
            continue

        try:
            loaded_report = pd.read_csv(report["source"])
        except (EmptyDataError, FileNotFoundError):
            continue
        if loaded_report.empty:
            continue

        frames.append(normalize_v2_slippage_report(loaded_report))
        reports_loaded += 1

    history = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return {
        "history": history,
        "legacy_reports_skipped": legacy_reports_skipped,
        "reports_loaded": reports_loaded,
    }
