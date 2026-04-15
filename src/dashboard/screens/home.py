"""
home.py — Operations & status home page.

Shows:
- Account NAV, cash, open P&L at a glance
- Current positions
- Data pipeline status (last append date, gaps)
- Latest daily run log
- IBKR broker connection status
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from src.dashboard.services.broker_service import (
    get_account_summary,
    get_broker_status,
    get_positions,
)
from src.dashboard.services.pipeline_service import get_pipeline_status

PROJECT_ROOT   = Path(__file__).resolve().parents[3]
ACCOUNT_DIR    = PROJECT_ROOT / "data" / "broker" / "account"
PRICES_PARQUET = PROJECT_ROOT / "data" / "market" / "cleaned" / "prices" / "etf_prices_master.parquet"
TARGETS_PARQUET= PROJECT_ROOT / "data" / "market" / "cleaned" / "targets" / "etf_targets_monthly.parquet"
RUNS_DIR       = PROJECT_ROOT / "artifacts" / "runs"


def _safe(fn, *a, **kw):
    try:
        return fn(*a, **kw), None
    except Exception as exc:
        return None, str(exc)


def _tag_value(rows: list[dict], *tags: str) -> float | None:
    for row in rows:
        t = str(row.get("tag", "")).lower()
        for tag in tags:
            if tag.lower() == t:
                try:
                    return float(str(row.get("value", "")).replace(",", ""))
                except Exception:
                    pass
    return None


def _master_last_date() -> str | None:
    try:
        if PRICES_PARQUET.exists():
            df = pd.read_parquet(PRICES_PARQUET, columns=["date"])
        else:
            csv = PRICES_PARQUET.with_suffix(".csv")
            if not csv.exists():
                return None
            df = pd.read_csv(csv, usecols=["date"])
        df["date"] = pd.to_datetime(df["date"])
        return str(df["date"].max().date())
    except Exception:
        return None


def _target_last_date() -> str | None:
    try:
        if TARGETS_PARQUET.exists():
            df = pd.read_parquet(TARGETS_PARQUET)
        else:
            csv = TARGETS_PARQUET.with_suffix(".csv")
            if not csv.exists():
                return None
            df = pd.read_csv(csv)
        df["rebalance_date"] = pd.to_datetime(df["rebalance_date"])
        return str(df["rebalance_date"].max().date())
    except Exception:
        return None


def _trading_days_gap(last_date_str: str | None) -> int | None:
    if last_date_str is None:
        return None
    try:
        last  = pd.Timestamp(last_date_str)
        today = pd.Timestamp.today().normalize()
        bdays = pd.bdate_range(last, today)
        return max(0, len(bdays) - 1)
    except Exception:
        return None


def _latest_run_log() -> dict | None:
    import json
    logs = sorted(RUNS_DIR.glob("daily_run_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not logs:
        return None
    try:
        return json.loads(logs[0].read_text())
    except Exception:
        return None


def render() -> None:
    profile = st.session_state.get("mode", "paper")

    # ── Broker status ─────────────────────────────────────────────────────────
    st.subheader("🔌 Broker Status")

    broker_status, broker_err = _safe(get_broker_status, profile=profile)
    connected = bool(broker_status and broker_status.get("connected"))

    if connected:
        st.success(
            f"Connected to IBKR {profile.upper()}  —  "
            f"{broker_status.get('host')}:{broker_status.get('port')}"
        )
        st.session_state["last_broker_refresh"] = datetime.now().strftime("%H:%M:%S")
    else:
        err_msg = broker_err or (broker_status or {}).get("error", "unknown")
        st.error(f"IBKR not reachable: {err_msg}")
        st.info(
            "Make sure IBG / TWS is running on port "
            f"{'4002' if profile == 'paper' else '4001'}."
        )

    # ── Account summary ───────────────────────────────────────────────────────
    if connected:
        acct_rows, acct_err = _safe(get_account_summary, profile=profile)
        if acct_rows:
            nav    = _tag_value(acct_rows, "NetLiquidation",  "Net Liquidation")
            cash   = _tag_value(acct_rows, "TotalCashValue",  "Total Cash Value")
            pnl    = _tag_value(acct_rows, "UnrealizedPnL",   "Unrealized PnL")
            margin = _tag_value(acct_rows, "BuyingPower",     "Buying Power")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Net Liquidation", f"${nav:,.0f}"    if nav    is not None else "N/A")
            c2.metric("Cash",            f"${cash:,.0f}"   if cash   is not None else "N/A")
            c3.metric("Unrealized P&L",  f"${pnl:+,.0f}"  if pnl    is not None else "N/A")
            c4.metric("Buying Power",    f"${margin:,.0f}" if margin is not None else "N/A")

    st.divider()

    # ── Current positions ─────────────────────────────────────────────────────
    st.subheader("📌 Current Positions")

    if connected:
        positions, pos_err = _safe(get_positions, profile=profile)
        if positions:
            df_pos = pd.DataFrame(positions)
            stocks = df_pos[df_pos.get("secType", pd.Series()) == "STK"].copy() if "secType" in df_pos.columns else df_pos
            if not stocks.empty:
                stocks = stocks[["symbol", "position", "avgCost"]].copy()
                stocks["position"]  = stocks["position"].astype(int)
                stocks["avgCost"]   = stocks["avgCost"].round(2)
                stocks["est_value"] = (stocks["position"] * stocks["avgCost"]).round(0)
                st.dataframe(
                    stocks.sort_values("est_value", ascending=False),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.info("No stock positions found.")
        else:
            st.warning(pos_err or "Could not load positions.")
    else:
        snap = PROJECT_ROOT / "data" / "broker" / "positions" / "paper_positions_snapshot.csv"
        if snap.exists():
            st.caption("⚠️ Showing last saved snapshot (IBKR offline)")
            st.dataframe(pd.read_csv(snap), use_container_width=True, hide_index=True)
        else:
            st.info("Connect to IBKR to see live positions.")

    st.divider()

    # ── Data pipeline status ──────────────────────────────────────────────────
    st.subheader("⚙️ Data Pipeline Status")

    master_last = _master_last_date()
    target_last = _target_last_date()
    gap_days    = _trading_days_gap(master_last)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Price Data Last Date",  master_last or "missing")
    c2.metric("Latest Rebalance Date", target_last or "missing")
    c3.metric("Trading Days Behind",   f"{gap_days}d" if gap_days is not None else "?")
    c4.metric("Today",                 str(datetime.now().date()))

    if gap_days is not None and gap_days > 1:
        st.warning(
            f"⚠️ **Data gap: {gap_days} trading days behind.** "
            "Go to **Run Pipeline → Append IBKR Daily Bars**, or run in terminal:\n\n"
            "```\ncd /Users/tradingworkspace/TradingWorkspace/trading-engine\n"
            "python -m src.production.backfill_gaps\n```"
        )
    elif gap_days in (0, 1):
        st.success("✓ Price data is up to date.")

    with st.expander("Pipeline file paths", expanded=False):
        status = get_pipeline_status(profile=profile)
        path_map = {
            "Master Prices":    status.get("prices_path"),
            "Targets":          status.get("targets_path"),
            "Latest Snapshot":  status.get("snapshot_path"),
            "Latest Basket":    status.get("basket_path"),
            "Latest Submission":status.get("submission_path"),
            "Latest Fill Log":  status.get("fill_log_path"),
        }
        for label, path in path_map.items():
            col_a, col_b = st.columns([2, 4])
            col_a.write(f"**{label}**")
            col_b.code(path or "—", language=None)

    st.divider()

    # ── Latest daily run ──────────────────────────────────────────────────────
    st.subheader("🗒️ Latest Daily Run Log")

    run_log = _latest_run_log()
    if run_log:
        ok       = run_log.get("ok")
        run_date = run_log.get("date", "?")
        run_id   = run_log.get("run_id", "?")

        if ok:
            st.success(f"Last run OK — {run_date} (id: {run_id})")
        else:
            st.error(f"Last run FAILED — {run_date}: {run_log.get('error', 'unknown')}")

        steps = run_log.get("steps", {})
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Data Append",   "✓" if steps.get("append_daily",     {}).get("ok") else "—")
        c2.metric("Targets",       "✓" if steps.get("generate_targets", {}).get("ok") else "—")
        c3.metric("Basket Built",  "✓" if steps.get("build_basket",     {}).get("ok") else "—")
        sub = steps.get("submit_orders", {})
        c4.metric("Orders",
                  "dry run" if sub.get("skipped") else "✓" if sub.get("ok") else "—")

        with st.expander("Full run log JSON"):
            st.json(run_log)
    else:
        st.info(
            "No daily run logs yet. Logs appear in `artifacts/runs/` after the first automated run."
        )

    # ── Auto-refresh ──────────────────────────────────────────────────────────
    st.divider()
    _, col_b = st.columns([3, 1])
    with col_b:
        auto = st.toggle("Auto-refresh (30s)", value=False, key="home_autorefresh")

    if auto:
        import time
        st.caption(f"Auto-refreshing... last: {datetime.now().strftime('%H:%M:%S')}")
        time.sleep(30)
        st.rerun()


def app() -> None:
    render()
