"""
backtest_charts.py — Marginal model backtest analytics.

Currently shows:
- NAV curve from master prices for each ETF in universe
- Momentum signal history (from targets file)
- Allocation history over time

(Cloud/GCS-backed audit functions removed — local-first only.)
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT    = Path(__file__).resolve().parents[3]
PRICES_PARQUET  = PROJECT_ROOT / "data" / "market" / "cleaned" / "prices"  / "etf_prices_master.parquet"
TARGETS_PARQUET = PROJECT_ROOT / "data" / "market" / "cleaned" / "targets" / "etf_targets_monthly.parquet"


def _load_prices() -> pd.DataFrame | None:
    try:
        if PRICES_PARQUET.exists():
            return pd.read_parquet(PRICES_PARQUET)
        csv = PRICES_PARQUET.with_suffix(".csv")
        if csv.exists():
            return pd.read_csv(csv)
    except Exception as exc:
        st.error(f"Could not load prices: {exc}")
    return None


def _load_targets() -> pd.DataFrame | None:
    try:
        if TARGETS_PARQUET.exists():
            return pd.read_parquet(TARGETS_PARQUET)
        csv = TARGETS_PARQUET.with_suffix(".csv")
        if csv.exists():
            return pd.read_csv(csv)
    except Exception as exc:
        st.error(f"Could not load targets: {exc}")
    return None


def render() -> None:
    # ── Price chart ───────────────────────────────────────────────────────────
    st.subheader("📈 ETF Price History")

    prices = _load_prices()
    if prices is not None and not prices.empty:
        prices["date"]   = pd.to_datetime(prices["date"])
        prices["symbol"] = prices["symbol"].str.upper()

        all_symbols = sorted(prices["symbol"].unique().tolist())
        selected = st.multiselect(
            "Select ETFs to chart",
            options=all_symbols,
            default=["SPY", "GLD", "TLT", "SLV"] if "SPY" in all_symbols else all_symbols[:4],
        )

        date_range = st.slider(
            "Date range",
            min_value=prices["date"].min().to_pydatetime(),
            max_value=prices["date"].max().to_pydatetime(),
            value=(
                pd.Timestamp("2020-01-01").to_pydatetime(),
                prices["date"].max().to_pydatetime(),
            ),
        )

        if selected:
            filtered = prices[
                prices["symbol"].isin(selected)
                & (prices["date"] >= pd.Timestamp(date_range[0]))
                & (prices["date"] <= pd.Timestamp(date_range[1]))
            ].copy()

            # Normalize to 100
            filtered = filtered.sort_values(["symbol", "date"])
            filtered["idx_close"] = filtered.groupby("symbol")["close"].transform(
                lambda s: s / s.iloc[0] * 100
            )

            pivot = filtered.pivot(index="date", columns="symbol", values="idx_close")

            st.line_chart(pivot, use_container_width=True)
            st.caption("Indexed to 100 at start of selected date range.")
        else:
            st.info("Select at least one ETF above.")
    else:
        st.warning("No price data found. Run the pipeline to load data.")

    st.divider()

    # ── Allocation history ────────────────────────────────────────────────────
    st.subheader("🎯 Momentum Allocation History")

    targets = _load_targets()
    if targets is not None and not targets.empty:
        targets["rebalance_date"] = pd.to_datetime(targets["rebalance_date"])

        # Pivot: date × symbol → target_weight
        pivot_weights = targets.pivot_table(
            index="rebalance_date",
            columns="symbol",
            values="target_weight",
            fill_value=0,
        )

        st.area_chart(pivot_weights, use_container_width=True)
        st.caption(
            "Shows which ETFs were selected each month by the momentum model (top-5 by "
            "126-day momentum). Each row sums to 1.0 = 100% allocation."
        )

        st.subheader("Latest Targets")
        latest_date = targets["rebalance_date"].max()
        latest_targets = targets[targets["rebalance_date"] == latest_date].sort_values("rank")
        st.dataframe(
            latest_targets[["symbol", "rank", "target_weight", "signal_value", "reference_price"]],
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"Rebalance date: **{latest_date.date()}**")
    else:
        st.warning("No targets found. Run **Run Pipeline → Generate Targets** first.")


def app() -> None:
    render()
