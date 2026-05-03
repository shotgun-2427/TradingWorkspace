from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import streamlit as st

from src.dashboard.services.order_service import (
    clear_submit_lock,
    duplicate_submission_guard,
    get_order_status,
    load_latest_fill_log,
    load_latest_submission_log,
    submit_paper_orders,
)


def _read_table(path_str: str | None) -> pl.DataFrame | None:
    if not path_str:
        return None

    path = Path(path_str)
    if not path.exists():
        return None

    suffix = path.suffix.lower()

    if suffix == ".parquet":
        return pl.read_parquet(path)

    if suffix == ".csv":
        return pl.read_csv(path)

    return None


def _to_pandas(df: pl.DataFrame | None) -> pd.DataFrame | None:
    if df is None or df.height == 0:
        return None
    return df.to_pandas()


def _render_guard(guard: dict[str, Any]) -> None:
    allowed = bool(guard.get("allowed", False))
    reason = guard.get("reason", "No reason provided.")
    last_submission = guard.get("last_submission_path")
    basket_path = guard.get("basket_path")

    c1, c2 = st.columns(2)
    with c1:
        st.write("**Allowed to submit:**", "Yes" if allowed else "No")
        st.write("**Reason:**", reason)
    with c2:
        st.write("**Basket path:**", basket_path or "Not found")
        st.write("**Last submission:**", last_submission or "None")

    if allowed:
        st.success("Submission guard passed.")
    else:
        st.warning("Submission guard is blocking submission.")


def render() -> None:
    profile = st.session_state.get("mode", "paper")

    st.caption(f"Active profile: {profile}")

    if profile != "paper":
        st.error("This page only supports paper order submission right now.")
        return

    status = get_order_status(profile=profile)
    guard = duplicate_submission_guard(profile=profile)

    st.markdown("## Submission Status")
    c1, c2, c3 = st.columns(3)
    c1.metric("Mode", str(profile).upper())
    c2.metric("Submit Lock", "ON" if st.session_state.get("submit_locked") else "OFF")
    c3.metric("Guard Allowed", "YES" if guard.get("allowed") else "NO")

    st.write("**Latest basket file:**", status.get("basket_path") or "Not found")
    st.write("**Latest submission file:**", status.get("submission_path") or "Not found")
    st.write("**Latest fill log:**", status.get("fill_log_path") or "Not found")
    st.write("**Session submit lock:**", "ON" if st.session_state.get("submit_locked") else "OFF")

    st.divider()

    st.markdown("## Duplicate Submission Guard")
    _render_guard(guard)

    st.divider()

    st.markdown("## Basket Preview")
    basket_df = _read_table(status.get("basket_path"))
    basket_pd = _to_pandas(basket_df)

    if basket_pd is None:
        st.info("No basket file found to preview.")
    else:
        st.metric("Basket rows", len(basket_pd))
        st.dataframe(basket_pd, use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("## Submit Controls")
    confirm_paper = st.checkbox("I confirm this is paper trading.", value=False)
    force_submit = st.checkbox(
        "Force submit even if duplicate submission guard fails.",
        value=False,
        help="Use only if you know the latest basket has not actually been submitted yet.",
    )
    clear_lock = st.checkbox(
        "Clear file-based submit lock before submit.",
        value=False,
        help="Only use this if you intentionally want to re-enable submission.",
    )

    c1, c2 = st.columns(2)

    with c1:
        if st.button("Clear Submit Lock", use_container_width=True):
            clear_submit_lock(profile=profile)
            st.session_state["submit_locked"] = False
            st.session_state["last_submit_action"] = "clear_submit_lock"
            st.session_state["last_submit_result"] = {
                "ok": True,
                "action": "clear_submit_lock",
                "timestamp": datetime.now().isoformat(timespec="seconds"),
            }
            st.rerun()

    with c2:
        session_locked = st.session_state.get("submit_locked", False)
        submit_disabled = (
            session_locked
            or not confirm_paper
            or (guard.get("allowed") is False and not force_submit)
        )

        if st.button(
            "Submit Paper Orders",
            type="primary",
            use_container_width=True,
            disabled=submit_disabled,
        ):
            if clear_lock:
                clear_submit_lock(profile=profile)
                st.session_state["submit_locked"] = False

            with st.spinner("Submitting paper orders..."):
                result = submit_paper_orders(profile=profile, force=force_submit)

            st.session_state["last_submit_action"] = "submit_paper_orders"
            st.session_state["last_submit_result"] = result
            st.session_state["last_submit_time"] = datetime.now().isoformat(timespec="seconds")

            if result.get("ok"):
                st.session_state["submit_locked"] = True
                st.session_state["last_submit_path"] = result.get("submission_path")
                st.session_state["last_fill_log_path"] = result.get("fill_log_path")

            st.rerun()

    if not confirm_paper:
        st.info("Check the confirmation box to enable submission.")

    if st.session_state.get("submit_locked"):
        st.warning("Session submit lock is active. Clear it before attempting another submission.")

    if guard.get("allowed") is False and not force_submit:
        st.warning("Duplicate submission guard is blocking submit.")

    st.divider()

    st.markdown("## Last Submission Result")
    submit_result = st.session_state.get("last_submit_result")
    if submit_result:
        if submit_result.get("ok"):
            st.success("Last submit action completed successfully.")
        else:
            st.error(submit_result.get("error", "Last submit action failed."))
        st.json(submit_result)
    else:
        st.info("No submit action has been run in this session.")

    st.divider()

    st.markdown("## Latest Submission Log")
    submitted_df = load_latest_submission_log()
    if submitted_df is None or submitted_df.height == 0:
        st.info("No submission log found.")
    else:
        st.metric("Submitted rows", submitted_df.height)
        st.dataframe(submitted_df.to_pandas(), use_container_width=True, hide_index=True)

    st.divider()

    st.markdown("## Latest Fill Log")
    fills_df = load_latest_fill_log()
    if fills_df is None or fills_df.height == 0:
        st.info("No fill log found.")
    else:
        st.metric("Fill rows", fills_df.height)
        st.dataframe(fills_df.to_pandas(), use_container_width=True, hide_index=True)


def app() -> None:
    render()