"""
paper_performance.py — Paper portfolio equity curves and performance metrics.

Replicates the rich portfolio analysis layout:
- Date range controls + checkboxes
- Plotly equity curve (actual + SPX benchmark)
- Optional drawdown chart
- Performance metrics (4 columns)
"""
from __future__ import annotations

from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ACCOUNT_DIR = PROJECT_ROOT / "data" / "broker" / "account"
PRICES_PARQUET = PROJECT_ROOT / "data" / "market" / "cleaned" / "prices" / "etf_prices_master.parquet"

TRADING_DAYS = 252


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_account_history() -> pd.DataFrame | None:
    """Load all account snapshots and build NAV time series."""
    try:
        files = sorted(ACCOUNT_DIR.glob("paper_account_summary_*.csv"))
        if not files:
            return None

        frames = []
        for f in files:
            try:
                df = pd.read_csv(f)
                # Extract timestamp from filename
                parts = f.stem.split("_")
                if len(parts) >= 4:
                    ts_str = f"{parts[-2]}_{parts[-1]}"
                    df["timestamp"] = pd.to_datetime(ts_str, format="%Y%m%d_%H%M%S", errors="coerce")
                    frames.append(df)
            except Exception:
                pass

        if not frames:
            return None

        combined = pd.concat(frames, ignore_index=True)
        if combined.empty:
            return None

        # Extract NetLiquidation value from account summary rows
        combined.columns = [c.lower().strip() for c in combined.columns]
        candidates = ["tag", "key", "field"]
        tag_col = next((c for c in candidates if c in combined.columns), None)
        candidates_val = ["value", "amount", "val"]
        val_col = next((c for c in candidates_val if c in combined.columns), None)

        if tag_col and val_col:
            net_liq = combined[
                combined[tag_col].str.lower().str.contains("netliquidation", na=False)
            ].copy()
            if not net_liq.empty:
                net_liq["nav"] = pd.to_numeric(
                    net_liq[val_col].astype(str).str.replace(",", ""), errors="coerce"
                )
                net_liq = net_liq[["timestamp", "nav"]].dropna()
                net_liq = net_liq.sort_values("timestamp").drop_duplicates("timestamp")
                net_liq["date"] = net_liq["timestamp"].dt.date
                return net_liq[["date", "nav"]]

        return None
    except Exception:
        return None


def _load_spx_prices(start_date: date) -> pd.DataFrame | None:
    """Load SPY prices as S&P 500 proxy."""
    try:
        if PRICES_PARQUET.exists():
            df = pd.read_parquet(PRICES_PARQUET)
        else:
            csv = PRICES_PARQUET.with_suffix(".csv")
            if not csv.exists():
                return None
            df = pd.read_csv(csv)

        df.columns = [c.lower() for c in df.columns]
        spy = df[(df["symbol"] == "SPY")].copy()
        if spy.empty:
            return None

        spy["date"] = pd.to_datetime(spy["date"]).dt.date
        spy = spy[spy["date"] >= start_date].sort_values("date")
        return spy[["date", "close"]].rename(columns={"close": "price"})
    except Exception:
        return None


def _compute_metrics(equity: pd.Series) -> dict:
    """Compute performance metrics from equity curve."""
    metrics = {
        "total_return": np.nan,
        "annualized_return": np.nan,
        "annualized_volatility": np.nan,
        "max_drawdown": np.nan,
        "sharpe_ratio": np.nan,
    }

    equity = equity.dropna()
    if equity.empty or len(equity) < 2:
        return metrics

    metrics["total_return"] = equity.iloc[-1] - 1.0
    daily_returns = equity.pct_change().dropna()

    if len(daily_returns) > 1:
        metrics["annualized_volatility"] = daily_returns.std() * np.sqrt(TRADING_DAYS)

    years = len(equity) / TRADING_DAYS
    if years > 0:
        metrics["annualized_return"] = (equity.iloc[-1]) ** (1 / years) - 1

    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1
    metrics["max_drawdown"] = drawdown.min()

    if metrics.get("annualized_volatility", 0) > 0:
        metrics["sharpe_ratio"] = metrics["annualized_return"] / metrics["annualized_volatility"]

    return metrics


def _key(profile: str, suffix: str) -> str:
    return f"{profile}_{suffix}"


# ── Render ────────────────────────────────────────────────────────────────────

def render(profile: str = "paper", *, show_title: bool = True) -> None:
    profile_label = profile.title()

    # Load NAV history
    nav_df = _load_account_history()

    if nav_df is None or nav_df.empty:
        if show_title:
            st.title("Portfolio Performance")
        else:
            st.subheader("Portfolio Performance")
        st.info(f"No {profile_label.lower()} historical NAV data is available yet.")
        return

    nav_df["date"] = pd.to_datetime(nav_df["date"]).dt.date
    nav_df = nav_df.sort_values("date").drop_duplicates("date")

    # Header
    left, right = st.columns([3, 1])
    with left:
        if show_title:
            st.title("Portfolio Performance")
        else:
            st.subheader("Portfolio Performance")
        st.caption(
            f"Equity curves for the {profile_label.lower()} portfolio and S&P 500 benchmark."
        )
    with right:
        latest_nav = nav_df["nav"].iloc[-1] if not nav_df.empty else None
        st.metric(
            f"{profile_label} NAV",
            f"${latest_nav:,.0f}" if latest_nav else "N/A",
        )

    st.divider()

    default_start = nav_df["date"].iloc[0]
    default_end = nav_df["date"].iloc[-1]

    # Controls
    with st.expander("Controls", expanded=True):
        col1, col2, col3 = st.columns([1, 1, 1])

        with col1:
            start_date = st.date_input(
                "Start Date",
                value=default_start,
                min_value=default_start,
                max_value=default_end,
                key=_key(profile, "pp_start_date"),
            )

        with col2:
            end_date = st.date_input(
                "End Date",
                value=default_end,
                min_value=default_start,
                max_value=default_end,
                key=_key(profile, "pp_end_date"),
            )

        with col3:
            show_spx = st.checkbox(
                "Show SPX",
                value=True,
                key=_key(profile, "pp_show_spx"),
            )

        show_log_scale = st.checkbox(
            "Log Scale (Base 2)",
            value=False,
            key=_key(profile, "pp_log_scale"),
        )
        show_drawdown = st.checkbox(
            "Show Drawdown Chart",
            value=False,
            key=_key(profile, "pp_show_drawdown"),
        )

    if start_date > end_date:
        st.error("Start date must be on or before end date.")
        return

    # Filter NAV to date range
    mask = (nav_df["date"] >= start_date) & (nav_df["date"] <= end_date)
    plot_df = nav_df.loc[mask].copy()

    if plot_df.empty:
        st.info("No NAV data is available in the selected date range.")
        return

    plot_df["daily_returns"] = plot_df["nav"].pct_change().fillna(0)
    plot_df["equity"] = (1 + plot_df["daily_returns"]).cumprod()

    combined_series = {
        f"Actual {profile_label} Portfolio": plot_df.set_index("date")["equity"]
    }

    # Load SPX
    if show_spx:
        spx_df = _load_spx_prices(start_date)
        if spx_df is not None and not spx_df.empty:
            spx_df["date"] = pd.to_datetime(spx_df["date"]).dt.date
            spx_mask = (spx_df["date"] >= start_date) & (spx_df["date"] <= end_date)
            spx_df = spx_df.loc[spx_mask].sort_values("date")
            if not spx_df.empty:
                spx_df["daily_returns"] = spx_df["price"].pct_change().fillna(0)
                spx_df["equity"] = (1 + spx_df["daily_returns"]).cumprod()
                combined_series["S&P 500"] = spx_df.set_index("date")["equity"]

    # Build chart data
    combined = pd.concat(combined_series.values(), axis=1)
    combined.columns = list(combined_series.keys())
    combined = combined.reset_index()
    long = combined.melt(id_vars="date", var_name="series", value_name="equity")

    # Plotly chart
    series_colors = ["#63b3ed", "#ed8936"]
    fig = px.line(
        long,
        x="date",
        y="equity",
        color="series",
        color_discrete_sequence=series_colors,
        labels={"date": "Date", "equity": "Cumulative Returns", "series": "Series"},
        title="Equity Curves",
    )

    use_log_scale = show_log_scale
    if show_log_scale and (long["equity"] <= 0).any():
        st.warning(
            "Log base-2 scale requires all equity values to be positive. Showing linear scale."
        )
        use_log_scale = False

    if use_log_scale:
        min_equity = long["equity"].min()
        max_equity = long["equity"].max()
        min_exp = int(np.floor(np.log2(min_equity)))
        max_exp = int(np.ceil(np.log2(max_equity)))
        tickvals = [2**exp for exp in range(min_exp, max_exp + 1)]
        ticktext = [f"2^{exp}" for exp in range(min_exp, max_exp + 1)]
        fig.update_yaxes(
            type="log",
            tickmode="array",
            tickvals=tickvals,
            ticktext=ticktext,
        )
    else:
        fig.update_yaxes(type="linear")

    fig.update_traces(mode="lines")
    st.plotly_chart(fig, use_container_width=True)

    # Drawdown chart
    if show_drawdown:
        dd_long_parts = []
        for column in combined_series:
            series = combined.set_index("date")[column].dropna()
            running_max = series.cummax()
            drawdown = (series / running_max) - 1
            dd_long_parts.append(
                pd.DataFrame({
                    "date": drawdown.index,
                    "series": column,
                    "drawdown": drawdown.values,
                })
            )
        dd_long = pd.concat(dd_long_parts, axis=0, ignore_index=True)
        dd_fig = px.area(
            dd_long,
            x="date",
            y="drawdown",
            color="series",
            color_discrete_sequence=series_colors,
            labels={"date": "Date", "drawdown": "Drawdown", "series": "Series"},
            title="Drawdown",
        )
        dd_fig.update_traces(mode="lines")
        st.plotly_chart(dd_fig, use_container_width=True)

    # Metrics
    metrics = _compute_metrics(plot_df["equity"])
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Return", f"{metrics['total_return'] * 100:.2f}%")
    m2.metric("Annualized Return", f"{metrics['annualized_return'] * 100:.2f}%")
    m3.metric("Annualized Volatility", f"{metrics['annualized_volatility'] * 100:.2f}%")
    m4.metric("Max Drawdown", f"{metrics['max_drawdown'] * 100:.2f}%")


def app(profile: str = "paper") -> None:
    render(profile)
