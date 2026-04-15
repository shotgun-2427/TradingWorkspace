from __future__ import annotations

import streamlit as st

from src.dashboard.services.order_service import (
    get_order_status,
    load_latest_fill_log,
    load_latest_submission_log,
)


def render() -> None:
    profile = st.session_state.get("mode", "paper")
    status = get_order_status(profile=profile)

    st.caption(f"Active profile: {profile}")

    st.markdown("## Order and Fill Status")
    c1, c2, c3 = st.columns(3)
    c1.metric("Mode", str(profile).upper())
    c2.metric("Basket Found", "YES" if status.get("basket_path") else "NO")
    c3.metric("Submission Found", "YES" if status.get("submission_path") else "NO")

    st.write("**Latest basket file:**", status.get("basket_path") or "Not found")
    st.write("**Latest submission file:**", status.get("submission_path") or "Not found")
    st.write("**Latest fill log:**", status.get("fill_log_path") or "Not found")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Latest Submission Log")
        submitted_df = load_latest_submission_log()
        if submitted_df is None or submitted_df.height == 0:
            st.info("No submission log found.")
        else:
            st.metric("Submitted rows", submitted_df.height)
            st.dataframe(submitted_df.to_pandas(), use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Latest Fill Log")
        fills_df = load_latest_fill_log()
        if fills_df is None or fills_df.height == 0:
            st.info("No fill log found.")
        else:
            st.metric("Fill rows", fills_df.height)
            st.dataframe(fills_df.to_pandas(), use_container_width=True, hide_index=True)


def app() -> None:
    render()