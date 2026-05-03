"""Shared helpers for the dashboard routers."""
from __future__ import annotations

import math
import os
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd


def df_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """pandas.DataFrame -> JSON-safe list-of-dicts.

    Replaces NaN/NaT/Infinity with None, casts numpy scalars to Python types,
    and serialises Timestamps and datetime.date to ISO strings.
    """
    if df is None or df.empty:
        return []
    safe = df.copy()
    for column in safe.columns:
        if pd.api.types.is_datetime64_any_dtype(safe[column]):
            safe[column] = (
                pd.to_datetime(safe[column], errors="coerce")
                .dt.strftime("%Y-%m-%d")
                .where(safe[column].notna(), None)
            )
    records = safe.to_dict(orient="records")
    return [_clean_value(row) for row in records]


def _clean_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _clean_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_clean_value(v) for v in value]
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        f = float(value)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if isinstance(value, (pd.Timestamp,)):
        if pd.isna(value):
            return None
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    if isinstance(value, date):
        return value.isoformat()
    if value is pd.NaT:
        return None
    if isinstance(value, str) or value is None:
        return value
    if pd.isna(value):
        return None
    return value


def parse_iso_date(value: str | None, default: date | None = None) -> date | None:
    if not value:
        return default
    try:
        return date.fromisoformat(value)
    except ValueError:
        return default


def filter_nav_for_range(
    df: pd.DataFrame, start: date | None, end: date | None
) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    work = df.copy()
    if "date" in work.columns:
        work["date"] = pd.to_datetime(work["date"]).dt.date
        if start is not None:
            work = work.loc[work["date"] >= start]
        if end is not None:
            work = work.loc[work["date"] <= end]
    return work.reset_index(drop=True)


# Profile / optimizer selection are tracked in process state so the sidebar
# can update them and other routes pick them up.
_STATE: dict[str, Any] = {
    "profile": "paper",
    "optimizer": os.environ.get("CAPITALFUND_OPTIMIZER", "inverse_vol"),
}


def get_profile() -> str:
    return _STATE.get("profile", "paper")


def set_profile(profile: str) -> str:
    if profile not in {"paper", "live"}:
        raise ValueError(f"Unknown profile {profile!r}")
    _STATE["profile"] = profile
    return profile


def get_optimizer() -> str:
    return os.environ.get("CAPITALFUND_OPTIMIZER") or _STATE.get(
        "optimizer", "inverse_vol"
    )


def set_optimizer(name: str) -> str:
    _STATE["optimizer"] = name
    os.environ["CAPITALFUND_OPTIMIZER"] = name
    return name
