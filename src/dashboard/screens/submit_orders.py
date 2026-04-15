from __future__ import annotations

from datetime import datetime

import streamlit as st

from src.dashboard.services.order_service import (
    clear_submit_lock,
    duplicate_submission_guard,
    get_order_status,
    load_latest_basket,
    load_latest_fill_log,
    load_latest_submission_log,
    submit_paper_orders,
)


def _render_guard(profile: str) -> dict:
    guard = duplicate_submission_guard(profile=profile)

    st.subheader("Submission Guard")

    if guard.get("allowed") is True:
        st.success(guard.get("reason", "Basket is eligible for submission."))
    elif guard.get("allowed") is False:
        st.warning(guard.get("reason", "Submission is blocked."))
    else:
        st.error("Unable to determine submission guard state.")

    st.write("**Basket path:**", guard.get("basket_path"))
    st.write("**Basket fingerprint:**", guard.get("basket_fingerprint"))

    lock = guard.get("lock")
    if lock:
        with st.expander("Current Submit Lock"):
            st.json(lock)

    return guard


def _render_basket_preview() -> None:
    st.subheader("Basket Preview")

    basket_df = load_latest_basket()
    if basket_df is None or basket_df.height == 0:
        st.info("No basket file found.")
        return

    st.dataframe(basket_df.to_pandas(), use_container_width=True)
    st.caption(f"Rows: {basket_df.height}")


def _render_submission_logs() -> None:
    st.subheader("Latest Submission Log")

    submitted_df = load_latest_submission_log()
    if submitted_df is None or submitted_df.height == 0:
        st.info("No submitted-order log found.")
    else:
        st.dataframe(submitted_df.to_pandas(), use_container_width=True)

    st.subheader("Latest Fill Log")

    fills_df = load_latest_fill_log()
    if fills_df is None or fills_df.height == 0:
        st.info("No fill log found.")
    else:
        st.dataframe(fills_df.to_pandas(), use_container_width=True)


def render() -> None:
    st.title("Submit Paper Orders")

    profile = st.session_state.get("mode", "paper")
    st.caption(f"Active profile: {profile}")

    if profile != "paper":
        st.error("This page only allows paper-order submission.")
        return

    st.warning("Paper trading only. Do not use this page for live order submission.")

    status = get_order_status(profile=profile)
    guard = _render_guard(profile=profile)

    st.divider()

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Basket rows", status.get("basket_rows", 0))
    with c2:
        st.metric("Submitted rows", status.get("submission_rows", 0))
    with c3:
        st.metric("Fill rows", status.get("fill_rows", 0))

    st.write("**Basket file:**", status.get("basket_path"))
    st.write("**Basket timestamp:**", status.get("basket_timestamp"))
    st.write("**Latest submission file:**", status.get("submission_path"))
    st.write("**Latest fill log:**", status.get("fill_log_path"))

    st.divider()
    _render_basket_preview()

    st.divider()
    st.subheader("Submit Controls")

    confirm_paper = st.checkbox("I confirm this is PAPER trading.")
    force_submit = st.checkbox("Force submit even if duplicate guard is blocking.")
    clear_lock = st.checkbox("Clear duplicate-submit lock before submitting.")

    c1, c2 = st.columns(2)

    with c1:
        if st.button("Clear Submit Lock", use_container_width=True):
            clear_submit_lock(profile=profile)
            st.session_state["last_submit_action"] = "clear_submit_lock"
            st.session_state["last_submit_result"] = {
                "ok": True,
                "action": "clear_submit_lock",
                "timestamp": datetime.now().isoformat(timespec="seconds"),
            }
            st.rerun()

    with c2:
        submit_disabled = not confirm_paper or (guard.get("allowed") is False and not force_submit)

        if st.button("Submit Paper Orders", type="primary", use_container_width=True, disabled=submit_disabled):
            if clear_lock:
                clear_submit_lock(profile=profile)

            with st.spinner("Submitting paper orders..."):
                result = submit_paper_orders(profile=profile, force=force_submit)

            st.session_state["last_submit_action"] = "submit_paper_orders"
            st.session_state["last_submit_result"] = result
            st.session_state["last_submit_time"] = datetime.now().isoformat(timespec="seconds")
            st.rerun()

    if not confirm_paper:
        st.info("Tick the paper confirmation box to enable submission.")
    elif guard.get("allowed") is False and not force_submit:
        st.info("Submission is blocked by duplicate protection. Use force only when you are certain.")

    st.divider()
    st.subheader("Last Submit Result")

    last_submit_action = st.session_state.get("last_submit_action")
    last_submit_result = st.session_state.get("last_submit_result")
    last_submit_time = st.session_state.get("last_submit_time")

    if last_submit_action and last_submit_result:
        st.write("**Last action:**", last_submit_action)
        if last_submit_time:
            st.write("**Time:**", last_submit_time)

        if last_submit_result.get("ok"):
            st.success("Last submit action completed.")
        else:
            st.error("Last submit action failed or was blocked.")

        st.json(last_submit_result)
    else:
        st.info("No submit action has been run in this session yet.")

    st.divider()
    _render_submission_logs()