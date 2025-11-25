import pandas as pd
import streamlit as st
import plotly.express as px

from src.dashboard.utils import (
    get_latest_production_audit,
    get_historical_nav,
    get_spx_prices_from_date,
    get_portfolio_backtest,
    get_active_optimizer
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
    show_spx = st.checkbox("Display S&P 500 Equity Curve", value=True)

    show_sim = st.checkbox(
        "Show Simulated Portfolio",
        value=True,
        key="show_sim"
    )

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

    # Combined series that get plotted
    combined_series = {}

    # Build equity curve: normalize NAV so the first value in the selection equals 1
    plot_df = plot_df.copy()
    plot_df['daily_returns'] = plot_df['nav'].pct_change().fillna(0)
    plot_df['equity'] = (1 + plot_df['daily_returns']).cumprod()
    combined_series['Actual Portfolio'] = plot_df.set_index('date')['equity']

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
                spx_df['daily_returns'] = spx_df['close'].pct_change().fillna(0)
                spx_df['equity'] = (1 + spx_df['daily_returns']).cumprod()
            else:
                spx_df = None
        except Exception as exc:
            st.warning(f"Could not load SPX prices: {exc}")
            spx_df = None
    
    sim_df = None
    if show_sim:
        # TODO: This is what the paper pipeline currently does (assumes single optimizer). Change this 
        # if that assumption is no longer valid.
        opt = get_active_optimizer()
        sim_df = get_portfolio_backtest(opt)

        sim_df['date'] = pd.to_datetime(sim_df['date'])
        sim_df = sim_df.sort_values('date')

        mask = (sim_df['date'].dt.date >= start_date) & (sim_df['date'].dt.date <= end_date)
        sim_df = sim_df[mask]

        if len(sim_df) == 0:
            st.warning(f"No data for optimizer '{opt}' in selected date range")

        sim_df['daily_return'].iloc[0] = 0 # set initial day to zero for proper cumulative calculation
        sim_df['equity'] = (1 + sim_df['daily_return']).cumprod()

    if spx_df is not None and not spx_df.empty:
        combined_series['S&P 500'] = spx_df.set_index('date')['equity']
    if show_sim and sim_df is not None:
        combined_series['Simulated Portfolio'] = sim_df.set_index('date')['equity']

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