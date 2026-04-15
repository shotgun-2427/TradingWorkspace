from __future__ import annotations

from typing import Any

import polars as pl
import streamlit as st

from src.dashboard.services.order_service import get_order_status, load_latest_basket
from src.dashboard.services.pipeline_service import get_pipeline_status


def _first_existing_column(df: pl.DataFrame, candidates: list[str]) -> str | None:
    lowered = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        actual = lowered.get(candidate.lower())
        if actual:
            return actual
    return None


def _sum_if_present(df: pl.DataFrame, candidates: list[str]) -> float:
    col = _first_existing_column(df, candidates)
    if col is None:
        return 0.0
    try:
        return float(df.get_column(col).cast(pl.Float64, strict=False).fill_null(0).sum())
    except Exception:
        return 0.0


def _count_side(df: pl.DataFrame, side_value: str) -> int:
    side_col = _first_existing_column(df, ["side", "order_side", "action"])
    if side_col is None:
        return 0
    try:
        return int(
            df.filter(pl.col(side_col).cast(pl.String).str.to_uppercase() == side_value.upper()).height
        )
    except Exception:
        return 0


def _numeric_metric(df: pl.DataFrame, candidates: list[str], default: float = 0.0) -> float:
    return _sum_if_present(df, candidates) if df.height > 0 else default


def _render_summary(df: pl.DataFrame) -> None:
    rows = df.height
    buy_count = _count_side(df, "BUY")
    sell_count = _count_side(df, "SELL")

    total_notional = _numeric_metric(
        df,
        ["estimated_notional", "order_notional", "notional", "trade_notional"],
    )
    total_commission = _numeric_metric(
        df,
        ["estimated_commission", "commission", "est_commission"],
    )
    total_qty = _numeric_metric(
        df,
        ["order_qty", "quantity", "qty", "delta_shares"],
    )
    total_turnover = _numeric_metric(
        df,
        ["turnover", "estimated_turnover"],
    )

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Rows", rows)
    c2.metric("Buys", buy_count)
    c3.metric("Sells", sell_count)
    c4.metric("Total qty", f"{total_qty:,.0f}")
    c5.metric("Total notional", f"${total_notional:,.2f}")
    c6.metric("Est. commission", f"${total_commission:,.2f}")

    if total_turnover > 0:
        st.metric("Turnover", f"${total_turnover:,.2f}")


def _render_health(df: pl.DataFrame) -> None:
    symbol_col = _first_existing_column(df, ["symbol", "ticker"])
    price_col = _first_existing_column(df, ["reference_price", "price", "last_price", "px"])
    qty_col = _first_existing_column(df, ["order_qty", "quantity", "qty"])
    delta_col = _first_existing_column(df, ["delta_shares", "share_delta"])

    warnings: list[str] = []

    if symbol_col:
        dupes = (
            df.group_by(symbol_col)
            .len()
            .filter(pl.col("len") > 1)
            .sort("len", descending=True)
        )
        if dupes.height > 0:
            warnings.append(f"Duplicate symbols found: {dupes.height}")

    if price_col:
        bad_prices = df.filter(pl.col(price_col).cast(pl.Float64, strict=False) <= 0)
        if bad_prices.height > 0:
            warnings.append(f"Non-positive prices found: {bad_prices.height}")

    if qty_col:
        bad_qty = df.filter(pl.col(qty_col).cast(pl.Float64, strict=False) <= 0)
        if bad_qty.height > 0:
            warnings.append(f"Non-positive order quantities found: {bad_qty.height}")
    elif delta_col:
        zero_delta = df.filter(pl.col(delta_col).cast(pl.Float64, strict=False) == 0)
        if zero_delta.height > 0:
            warnings.append(f"Zero-delta rows found: {zero_delta.height}")

    st.subheader("Basket Health")

    if not warnings:
        st.success("No obvious basket issues found.")
    else:
        for warning in warnings:
            st.warning(warning)


def render() -> None:
    st.title("Basket Review")

    profile = st.session_state.get("mode", "paper")
    st.caption(f"Active profile: {profile}")

    pipeline_status = get_pipeline_status(profile=profile)
    order_status = get_order_status(profile=profile)

    c1, c2 = st.columns(2)
    with c1:
        st.write("**Targets file:**", pipeline_status.get("targets_path"))
        st.write("**Targets timestamp:**", pipeline_status.get("targets_timestamp"))
        st.write("**Basket file:**", order_status.get("basket_path"))
        st.write("**Basket timestamp:**", order_status.get("basket_timestamp"))
    with c2:
        st.write("**Latest submission:**", order_status.get("submission_path"))
        st.write("**Latest fill log:**", order_status.get("fill_log_path"))
        guard = order_status.get("submit_guard", {})
        if guard.get("allowed") is True:
            st.success(guard.get("reason", "Basket is eligible for submission."))
        elif guard.get("allowed") is False:
            st.warning(guard.get("reason", "Submission is blocked."))

    basket_df = load_latest_basket()
    if basket_df is None or basket_df.height == 0:
        st.info("No basket file found.")
        return

    st.divider()
    _render_summary(basket_df)

    st.divider()
    _render_health(basket_df)

    st.divider()
    st.subheader("Basket Table")
    st.dataframe(basket_df.to_pandas(), use_container_width=True)

    with st.expander("Columns"):
        st.write(basket_df.columns)