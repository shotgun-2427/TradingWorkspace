from __future__ import annotations

import streamlit as st


def render_metrics(metrics: dict[str, float]) -> None:
    cols = st.columns(len(metrics))
    for idx, (k, v) in enumerate(metrics.items()):
        cols[idx].metric(k.replace("_", " ").title(), f"{v:.4f}")
