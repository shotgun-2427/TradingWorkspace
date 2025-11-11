import pandas as pd
import streamlit as st
import plotly.express as px

from src.dashboard.utils import (
    get_latest_production_audit,
    get_historical_nav,
    get_spx_prices_from_date,
)


def app():
    st.title("Paper Portfolio Performance")
    st.markdown(f"Latest Production Audit Date: {get_latest_production_audit()}")

    # Load historical NAV
    nav_df = get_historical_nav()

    if nav_df is None or nav_df.empty:
        st.info("No historical NAV data available.")
        return

    if 'date' not in nav_df.columns or 'nav' not in nav_df.columns:
        st.warning("historical_nav.csv missing expected columns 'date' and 'nav'.")
        return

    # Parse dates
    nav_df = nav_df.copy()
    nav_df['date'] = pd.to_datetime(nav_df['date'])
    nav_df = nav_df.sort_values('date')

    # Date range selector with defaults from data
    default_start = nav_df['date'].iloc[0].date()
    default_end = nav_df['date'].iloc[-1].date()

    # Date range selector (separate start and end inputs, matching backtest_charts style)
    st.subheader("Select Date Range")
    col1, col2 = st.columns(2)

    with col1:
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            min_value=default_start,
            max_value=default_end,
            key="pp_start_date",
        )

    with col2:
        end_date = st.date_input(
            "End Date",
            value=default_end,
            min_value=default_start,
            max_value=default_end,
            key="pp_end_date",
        )

    # Toggle to show/hide SPX comparison (default: shown)
    show_spx = st.checkbox("Display SPX Equity Curve", value=True)

    # Ensure start_date <= end_date
    if start_date > end_date:
        st.error("Start date must be on or before end date.")
        return

    # Filter dataframe to selection
    mask = (nav_df['date'].dt.date >= start_date) & (nav_df['date'].dt.date <= end_date)
    plot_df = nav_df.loc[mask]

    if plot_df.empty:
        st.info("No NAV data in the selected date range.")
        return

    # Build equity curve: normalize NAV so the first value in the selection equals 1
    plot_df = plot_df.copy()
    first_nav = plot_df['nav'].iloc[0]
    plot_df['equity'] = plot_df['nav'] / first_nav

    # Optionally load SPX prices and normalize to start at 1 over the same date range
    spx_df = None
    if show_spx:
        try:
            spx_df = get_spx_prices_from_date(default_start)
            # Ensure datetime type
            spx_df['date'] = pd.to_datetime(spx_df['date'])
            # Filter to selected range
            spx_mask = (spx_df['date'].dt.date >= start_date) & (spx_df['date'].dt.date <= end_date)
            spx_df = spx_df.loc[spx_mask].sort_values('date')
            if not spx_df.empty:
                first_spx = spx_df['close'].iloc[0]
                spx_df['equity'] = spx_df['close'] / first_spx
            else:
                spx_df = None
        except Exception as exc:
            st.warning(f"Could not load SPX prices: {exc}")
            spx_df = None

    # Combine portfolio equity and SPX equity into one long dataframe for plotting
    combined_series = {}
    combined_series['Portfolio'] = plot_df.set_index('date')['equity']

    if spx_df is not None and not spx_df.empty:
        combined_series['SPX'] = spx_df.set_index('date')['equity']

    # Concatenate series and melt to long form for Plotly
    combined = pd.concat(combined_series.values(), axis=1)
    combined.columns = list(combined_series.keys())
    combined = combined.reset_index()
    long = combined.melt(id_vars='date', var_name='series', value_name='equity')

    fig = px.line(
        long,
        x='date',
        y='equity',
        color='series',
        labels={'date': 'Date', 'equity': 'Cumulative Returns', 'series': 'Series'},
        title='Equity Curves',
    )
    fig.update_traces(mode='lines')
    fig.update_layout(legend_title_text='Series')

    st.subheader('Equity Curves')
    st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    app()