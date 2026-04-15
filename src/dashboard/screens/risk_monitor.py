"""
risk_monitor.py — Pre-submission risk checks.

Runs a set of automated checks before allowing order submission:
- Paper mode active
- IBKR connection
- Price / target file freshness
- Basket integrity
- Duplicate submission guard
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from src.dashboard.services.broker_service import get_broker_status, load_latest_positions
from src.dashboard.services.order_service import duplicate_submission_guard, load_latest_basket
from src.dashboard.services.pipeline_service import get_pipeline_status


def _age_hours(ts_iso: str | None) -> float | None:
    if not ts_iso:
        return None
    try:
        dt = datetime.fromisoformat(ts_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    except Exception:
        return None


def _staleness_check(label: str, ts: str | None, max_hours: float) -> dict:
    age = _age_hours(ts)
    if age is None:
        return {"Check": label, "Status": "❌ FAIL", "Detail": "No timestamp"}
    if age <= max_hours:
        return {"Check": label, "Status": "✅ PASS", "Detail": f"{age:.1f}h old"}
    return {"Check": label, "Status": "⚠️ WARN", "Detail": f"{age:.1f}h old (max {max_hours:.0f}h)"}


def render() -> None:
    profile = st.session_state.get("mode", "paper")

    checks: list[dict] = []

    # ── Fetch status ──────────────────────────────────────────────────────────
    try:
        pipeline_status = get_pipeline_status(profile=profile)
    except Exception as exc:
        pipeline_status = {}
        st.warning(f"Pipeline status error: {exc}")

    try:
        broker_status = get_broker_status(profile=profile)
    except Exception as exc:
        broker_status = {"connected": False, "error": str(exc)}

    try:
        guard = duplicate_submission_guard(profile=profile)
    except Exception as exc:
        guard = {"allowed": None, "reason": str(exc)}

    basket_df = None
    try:
        basket_df = load_latest_basket()
    except Exception:
        pass

    positions_df = None
    try:
        positions_df = load_latest_positions(profile=profile)
    except Exception:
        pass

    # ── Build check list ──────────────────────────────────────────────────────
    checks.append({
        "Check": "Paper mode",
        "Status": "✅ PASS" if profile == "paper" else "⚠️ WARN",
        "Detail": f"Mode: {profile}",
    })

    connected = broker_status.get("connected", False)
    checks.append({
        "Check": "IBKR connection",
        "Status": "✅ PASS" if connected else "❌ FAIL",
        "Detail": f"{broker_status.get('host')}:{broker_status.get('port')}"
                  if connected else broker_status.get("error", "disconnected"),
    })

    checks.append({
        "Check": "Prices file",
        "Status": "✅ PASS" if pipeline_status.get("prices_path") else "❌ FAIL",
        "Detail": pipeline_status.get("prices_path") or "Missing",
    })
    checks.append({
        "Check": "Targets file",
        "Status": "✅ PASS" if pipeline_status.get("targets_path") else "❌ FAIL",
        "Detail": pipeline_status.get("targets_path") or "Missing",
    })
    checks.append({
        "Check": "Basket file",
        "Status": "✅ PASS" if pipeline_status.get("basket_path") else "⚠️ WARN",
        "Detail": pipeline_status.get("basket_path") or "Not built yet",
    })

    checks.append(_staleness_check("Prices freshness", pipeline_status.get("prices_timestamp"), 72))
    checks.append(_staleness_check("Targets freshness", pipeline_status.get("targets_timestamp"), 72))
    checks.append(_staleness_check("Basket freshness",  pipeline_status.get("basket_timestamp"),  24))

    checks.append({
        "Check": "Duplicate submit guard",
        "Status": "✅ PASS" if guard.get("allowed") else "⚠️ WARN",
        "Detail": guard.get("reason", "unknown"),
    })

    # Basket row checks
    if basket_df is not None:
        import polars as pl
        if isinstance(basket_df, pl.DataFrame):
            n_rows = basket_df.height
        else:
            n_rows = len(basket_df)
        checks.append({
            "Check": "Basket has rows",
            "Status": "✅ PASS" if n_rows > 0 else "⚠️ WARN",
            "Detail": f"{n_rows} rows",
        })
    else:
        checks.append({"Check": "Basket has rows", "Status": "⚠️ WARN", "Detail": "No basket loaded"})

    # Positions check
    if positions_df is not None:
        n_pos = len(positions_df)
        checks.append({
            "Check": "Position snapshot",
            "Status": "✅ PASS" if n_pos > 0 else "⚠️ WARN",
            "Detail": f"{n_pos} rows in snapshot",
        })
    else:
        checks.append({"Check": "Position snapshot", "Status": "⚠️ WARN", "Detail": "No saved snapshot"})

    # ── Display ───────────────────────────────────────────────────────────────
    checks_df = pd.DataFrame(checks)

    pass_n = (checks_df["Status"].str.startswith("✅")).sum()
    warn_n = (checks_df["Status"].str.startswith("⚠️")).sum()
    fail_n = (checks_df["Status"].str.startswith("❌")).sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("✅ Pass", pass_n)
    c2.metric("⚠️ Warn", warn_n)
    c3.metric("❌ Fail", fail_n)

    if fail_n == 0 and warn_n == 0:
        st.success("All checks passed — safe to submit.")
    elif fail_n == 0:
        st.warning("No hard failures, but there are warnings to review.")
    else:
        st.error(f"{fail_n} check(s) failed. Resolve before submitting orders.")

    st.divider()
    st.dataframe(checks_df, use_container_width=True, hide_index=True)

    if basket_df is not None:
        with st.expander("Basket preview"):
            import polars as pl
            if isinstance(basket_df, pl.DataFrame):
                st.dataframe(basket_df.to_pandas(), use_container_width=True, hide_index=True)
            else:
                st.dataframe(basket_df, use_container_width=True, hide_index=True)


def app() -> None:
    render()
