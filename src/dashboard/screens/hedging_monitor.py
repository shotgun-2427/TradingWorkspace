from __future__ import annotations

import streamlit as st


def render() -> None:
    st.title("Hedging")
    st.caption("Reserved for futures and options overlay monitoring")

    st.info(
        "This page is a placeholder for the hedge overlay layer.\n\n"
        "Planned items:\n"
        "- futures hedge state\n"
        "- options overlay state\n"
        "- hedge trigger conditions\n"
        "- exposure adjustments\n"
        "- overlay PnL and risk contribution"
    )


def app() -> None:
    render()
