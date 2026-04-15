"""
run_pipeline.py — Pipeline control page.

Four actions:
  1. Run Full Daily Pipeline  (all-in-one: append + targets + basket + submit)
  2. Append IBKR Daily Bars   (just data pull)
  3. Generate Targets         (just momentum targets)
  4. Backfill Data Gap        (longer lookback to fill gaps)
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from src.dashboard.services.pipeline_service import (
    append_ibkr_daily,
    get_pipeline_status,
    refresh_targets,
    run_daily_pipeline,
)


def _profile() -> str:
    return st.session_state.get("mode", "paper")


def _host() -> str:
    p = _profile()
    return st.session_state.get("ibkr_paper_host" if p == "paper" else "ibkr_live_host", "127.0.0.1")


def _port() -> int:
    p = _profile()
    return int(st.session_state.get("ibkr_paper_port" if p == "paper" else "ibkr_live_port", 4002))


def _ts_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def render() -> None:
    profile = _profile()
    st.caption(f"Active profile: **{profile}**")

    if profile != "paper":
        st.warning("This page is currently intended for paper workflow only.")

    # ── Pipeline status summary ───────────────────────────────────────────────
    status = get_pipeline_status(profile=profile)

    c1, c2, c3 = st.columns(3)
    c1.metric("Master Prices", "✓" if status.get("prices_path") else "missing",
              delta=status.get("prices_timestamp", "—"), delta_color="off")
    c2.metric("Targets",       "✓" if status.get("targets_path") else "missing",
              delta=status.get("targets_timestamp", "—"), delta_color="off")
    c3.metric("Latest Snapshot", status.get("snapshot_timestamp") or "none",
              delta_color="off")

    st.divider()

    # ═══════════════════════════════════════════════════════════════════════════
    # Section A: One-click full daily run
    # ═══════════════════════════════════════════════════════════════════════════
    st.markdown("## ⚡ Full Daily Pipeline")
    st.caption(
        "Runs the complete cycle: append daily bars → regenerate targets → "
        "check rebalance → build basket → submit paper orders."
    )

    col_a, col_b, col_c = st.columns(3)
    full_host      = col_a.text_input("Host",      value=_host(),  key="full_host")
    full_port      = col_b.number_input("Port",    value=_port(),  step=1, key="full_port")
    full_client_id = col_c.number_input("Client ID", value=101, step=1, key="full_cid")

    col_d, col_e = st.columns(2)
    full_dry_run  = col_d.checkbox("Dry run (skip order submission)", value=True, key="full_dry")
    full_force    = col_e.checkbox("Force rebalance today", value=False, key="full_force")

    if st.button("▶ Run Full Daily Pipeline", type="primary", use_container_width=True):
        with st.spinner("Running full daily pipeline… (may take ~60s if IBKR is slow)"):
            result = run_daily_pipeline(
                host=full_host,
                port=int(full_port),
                client_id=int(full_client_id),
                profile=profile,
                lookback="5 D",
                dry_run=full_dry_run,
                force_rebalance=full_force,
            )
        st.session_state["full_pipeline_result"] = result
        st.session_state["last_pipeline_run"] = _ts_now()

    full_result = st.session_state.get("full_pipeline_result")
    if full_result:
        if full_result.get("ok"):
            st.success("Pipeline completed successfully.")
        else:
            st.error(f"Pipeline failed: {full_result.get('error', 'unknown')}")
        with st.expander("Full pipeline result"):
            st.json(full_result)

    st.divider()

    # ═══════════════════════════════════════════════════════════════════════════
    # Section B: Append daily bars
    # ═══════════════════════════════════════════════════════════════════════════
    st.markdown("## 1. Append IBKR Daily Bars")
    st.caption("Pulls the most recent trading days from IBKR and appends to master prices.")

    c1, c2, c3 = st.columns(3)
    host      = c1.text_input("Host",      value=_host(), key="app_host")
    port      = c2.number_input("Port",    value=_port(), step=1, key="app_port")
    client_id = c3.number_input("Client ID", value=101,  step=1, key="app_cid")
    lookback  = st.text_input("Lookback", value="15 D",
                              help="IBKR duration string: '5 D', '15 D', '45 D', '3 M'",
                              key="app_lookback")

    if st.button("Append IBKR Daily Bars", use_container_width=True):
        with st.spinner("Connecting to IBKR and fetching bars…"):
            result = append_ibkr_daily(
                profile=profile, host=host, port=int(port),
                client_id=int(client_id), lookback=lookback,
            )
        st.session_state["append_result"] = result
        st.session_state["last_broker_refresh"] = _ts_now()
        if result.get("ok"):
            st.session_state["last_pipeline_run"] = _ts_now()

    append_result = st.session_state.get("append_result")
    if append_result:
        if append_result.get("ok"):
            st.success("Daily bars appended.")
            a1, a2, a3 = st.columns(3)
            a1.metric("Symbols With Data", append_result.get("symbols_with_data", 0))
            a2.metric("New Rows Added",    append_result.get("new_rows_added_to_master", 0))
            a3.metric("Latest Date",       append_result.get("latest_date") or "N/A")
            st.caption(f"Snapshot: `{append_result.get('snapshot_path')}`")
            preview = append_result.get("preview", [])
            if preview:
                st.dataframe(pd.DataFrame(preview), use_container_width=True, hide_index=True)
        else:
            st.error(append_result.get("error", "Append failed."))

    st.divider()

    # ═══════════════════════════════════════════════════════════════════════════
    # Section C: Generate targets
    # ═══════════════════════════════════════════════════════════════════════════
    st.markdown("## 2. Generate Targets")
    st.caption(
        "Runs the momentum model (AMMA-style top-K selection) "
        "and writes monthly rebalance targets to disk."
    )

    t1, t2, t3 = st.columns(3)
    mom_lb  = t1.number_input("Momentum Lookback (days)", value=126, step=1)
    min_h   = t2.number_input("Min History (days)",       value=126, step=1)
    top_k   = t3.number_input("Top K ETFs",               value=5,   step=1)

    if st.button("Generate Targets", use_container_width=True):
        with st.spinner("Running momentum model…"):
            result = refresh_targets(
                profile=profile,
                momentum_lookback=int(mom_lb),
                min_history=int(min_h),
                top_k=int(top_k),
            )
        st.session_state["targets_result"] = result
        st.session_state["last_pipeline_run"] = _ts_now()

    targets_result = st.session_state.get("targets_result")
    if targets_result:
        if targets_result.get("ok"):
            st.success("Targets generated.")
            b1, b2, b3 = st.columns(3)
            b1.metric("Rows Written",     targets_result.get("rows_written", 0))
            b2.metric("Rebalance Dates",  targets_result.get("rebalance_dates", 0))
            b3.metric("Latest Rebalance", targets_result.get("latest_rebalance_date") or "N/A")
            preview = targets_result.get("preview", [])
            if preview:
                st.dataframe(pd.DataFrame(preview), use_container_width=True, hide_index=True)
        else:
            st.error(targets_result.get("error", "Target generation failed."))

    st.divider()

    # ═══════════════════════════════════════════════════════════════════════════
    # Section D: Backfill data gap
    # ═══════════════════════════════════════════════════════════════════════════
    st.markdown("## 3. Backfill Data Gap")
    st.caption(
        "Use a larger lookback window to fill in missing historical data. "
        "Prices are currently behind — use **45 D** or **3 M** to catch up."
    )

    bf1, bf2, bf3 = st.columns(3)
    bf_host      = bf1.text_input("Host",      value=_host(), key="bf_host")
    bf_port      = bf2.number_input("Port",    value=_port(), step=1, key="bf_port")
    bf_client_id = bf3.number_input("Client ID", value=101,  step=1, key="bf_cid")
    bf_lookback  = st.text_input(
        "Lookback",
        value="45 D",
        help="Use 45 D to fill ~6 weeks of gap. Use '3 M' for 3 months.",
        key="bf_lookback",
    )

    if st.button("▶ Run Backfill", type="secondary", use_container_width=True):
        with st.spinner(f"Backfilling with lookback={bf_lookback}… this may take a few minutes."):
            result = append_ibkr_daily(
                profile=profile,
                host=bf_host,
                port=int(bf_port),
                client_id=int(bf_client_id),
                lookback=bf_lookback,
            )
        st.session_state["backfill_result"] = result
        st.session_state["last_broker_refresh"] = _ts_now()

    bf_result = st.session_state.get("backfill_result")
    if bf_result:
        if bf_result.get("ok"):
            st.success("Backfill complete.")
            z1, z2, z3 = st.columns(3)
            z1.metric("Symbols", bf_result.get("symbols_with_data", 0))
            z2.metric("New rows added", bf_result.get("new_rows_added_to_master", 0))
            z3.metric("Latest date now", bf_result.get("latest_date") or "N/A")
        else:
            st.error(bf_result.get("error", "Backfill failed."))


def app() -> None:
    render()
