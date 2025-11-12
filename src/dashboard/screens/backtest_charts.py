import pandas as pd
import streamlit as st
import plotly.express as px
import numpy as np
from datetime import datetime

from src.dashboard.utils import (
    get_latest_production_audit,
    get_production_audit_models,
    get_production_audit_optimizers,
    get_model_backtest,
    get_portfolio_backtest,
    get_spx_prices_from_date
)


def calculate_metrics_from_backtest(df: pd.DataFrame) -> dict:
    """
    Calculate performance metrics from filtered backtest results DataFrame.
    Not using the backtest metrics df because this dynamically recalculates metrics based on dates

    Args:
        df: DataFrame with columns 'date', 'daily_return', 'cumulative_return', 'drawdown', etc.

    Returns:
        dict: Dictionary of metric names to values
    """
    metrics = {}

    if len(df) == 0:
        return metrics

    # Total return (final cumulative return)
    if 'cumulative_return' in df.columns:
        metrics['total_return'] = df['cumulative_return'].iloc[-1]

    # Calculate time period in years
    if 'date' in df.columns and len(df) > 1:
        days = len(df)
        years = days / 252  # Assuming 252 trading days per year
    else:
        years = 1.0

    # Annualized return
    if 'total_return' in metrics and years > 0:
        total_return = metrics['total_return']
        metrics['annualized_return'] = (1 + total_return) ** (1 / years) - 1

    # Annualized volatility
    if 'daily_return' in df.columns and len(df) > 1:
        daily_vol = df['daily_return'].std()
        metrics['annualized_volatility'] = daily_vol * np.sqrt(252)

    # Sharpe ratio (assuming 0% risk-free rate)
    if 'annualized_return' in metrics and 'annualized_volatility' in metrics:
        if metrics['annualized_volatility'] > 0:
            metrics['sharpe_ratio'] = metrics['annualized_return'] / metrics['annualized_volatility']
        else:
            metrics['sharpe_ratio'] = 0.0

    # Sortino ratio (using downside deviation)
    if 'daily_return' in df.columns and len(df) > 1:
        negative_returns = df[df['daily_return'] < 0]['daily_return']
        if len(negative_returns) > 0:
            downside_vol = negative_returns.std() * np.sqrt(252)
            if downside_vol > 0 and 'annualized_return' in metrics:
                metrics['sortino_ratio'] = metrics['annualized_return'] / downside_vol
            else:
                metrics['sortino_ratio'] = 0.0
        else:
            metrics['sortino_ratio'] = metrics.get('sharpe_ratio', 0.0)

    # Maximum drawdown
    if 'drawdown' in df.columns:
        metrics['max_drawdown'] = df['drawdown'].min()

    # Average drawdown
    if 'drawdown' in df.columns:
        metrics['avg_drawdown'] = df['drawdown'].mean()

    # Win rate
    if 'daily_return' in df.columns and len(df) > 1:
        # Count days with positive returns
        returns_series = df['daily_return']
        winning_days = (returns_series > 0).sum()
        # Only count days where there was actual trading (non-zero returns)
        trading_days = (returns_series != 0).sum()
        if trading_days > 0:
            metrics['win_rate'] = winning_days / trading_days
        else:
            metrics['win_rate'] = 0.0

    # Average daily return
    if 'daily_return' in df.columns:
        metrics['avg_daily_return'] = df['daily_return'].mean()

    return metrics


def app():
    st.title("Backtest Visualization")
    st.markdown(f"Latest Production Audit Date: {get_latest_production_audit()}")

    # Load available models and optimizers from the latest production audit
    models = get_production_audit_models()
    optimizers = get_production_audit_optimizers()

    # Streamlit multiselects
    selected_models = st.multiselect(
        "Select Model Backtests to View",
        options=models,
    )

    # Expose selections for downstream plotting/processing
    st.session_state.setdefault("selected_models", selected_models)

    # Date range selector
    st.subheader("Select Date Range")
    col1, col2 = st.columns(2)

    # We'll determine the actual date range from the data below
    # For now, set some default values
    default_start = datetime(2024, 1, 1).date()
    default_end = datetime.today().date()

    with col1:
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            key="start_date"
        )

    with col2:
        end_date = st.date_input(
            "End Date",
            value=default_end,
            key="end_date"
        )

    show_aggregate_portfolio = st.checkbox(
        "Show Aggregate Portfolio Backtest",
        value=False,
        key="show_aggregate_portfolio"
    )
    # Option to overlay SPX (S&P 500) equity curve normalized to start at 1
    show_spx = st.checkbox(
        "Show S&P 500 Equity Curve",
        value=False,
        key="show_spx"
    )

    # Build and plot combined equity curves for selected models and optimizers
    combined_series = {}
    metrics_data = {}  # Store metrics for table display

    # Load model backtests
    for model in selected_models:
        try:
            df = get_model_backtest(model)
        except Exception as exc:
            st.warning(f"Could not load backtest for model '{model}': {exc}")
            continue

        if 'date' not in df.columns or 'cumulative_return' not in df.columns:
            st.warning(f"Backtest file for '{model}' missing expected columns")
            continue

        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        # Filter by date range
        mask = (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)
        df_filtered = df[mask]

        if len(df_filtered) == 0:
            st.warning(f"No data for model '{model}' in selected date range")
            continue

        daily_returns = df_filtered.set_index('date')['daily_return'].fillna(0)
        daily_returns.iloc[0] = 0 # set initial day to zero for proper cumulative calculation
        series = (1 + daily_returns).cumprod().rename(model)
        combined_series[model] = series

        # Calculate metrics from filtered data
        try:
            metrics = calculate_metrics_from_backtest(df_filtered)
            metrics_data[model] = metrics
        except Exception as exc:
            st.warning(f"Could not calculate metrics for model '{model}': {exc}")

    # Load optimizer (portfolio) backtests
    if show_aggregate_portfolio:
        try:
            opt = optimizers.pop()
            pdf = get_portfolio_backtest(opt)
        except Exception as exc:
            st.warning(f"Could not load portfolio backtest for optimizer '{opt}': {exc}")

        pdf = pdf.copy()
        pdf['date'] = pd.to_datetime(pdf['date'])
        pdf = pdf.sort_values('date')

        # Filter by date range
        mask = (pdf['date'].dt.date >= start_date) & (pdf['date'].dt.date <= end_date)
        pdf_filtered = pdf[mask]

        daily_returns = pdf_filtered.set_index('date')['daily_return'].fillna(0)
        daily_returns.iloc[0] = 0 # set initial day to zero for proper cumulative calculation
        series_title = f"Aggregate Portfolio"
        series = (1 + daily_returns).cumprod().rename(series_title)
        combined_series[series_title] = series

        # Calculate metrics from filtered data
        try:
            metrics = calculate_metrics_from_backtest(pdf_filtered)
            metrics_data[opt] = metrics
        except Exception as exc:
            st.warning(f"Could not calculate metrics for optimizer '{opt}': {exc}")

    # If requested, fetch SPX prices and add its normalized equity series
    if show_spx:
        try:
            # get_spx_prices_from_date expects a start date (datetime.date)
            d = start_date
            spx_df = get_spx_prices_from_date(d)
            spx_df = spx_df.copy()
            spx_df['date'] = pd.to_datetime(spx_df['date'])

            # Filter to user selected date range
            mask = (spx_df['date'].dt.date >= start_date) & (spx_df['date'].dt.date <= end_date)
            spx_filtered = spx_df[mask]

            if len(spx_filtered) == 0:
                st.warning("No SPX data available for the selected date range")
            else:
                # Compute cumulative returns (equity) from SPX adjusted close prices
                spx_prices = spx_filtered.set_index('date')['close'].sort_index()
                # daily pct change, fill first NaN as 0 (no return), then cumulative product to get equity
                spx_returns = spx_prices.pct_change().fillna(0)
                spx_equity = (1 + spx_returns).cumprod().rename('S&P 500')
                combined_series['S&P 500'] = spx_equity
        except Exception as exc:
            st.warning(f"Could not fetch SPX data: {exc}")

    if combined_series:
        combined = pd.concat(combined_series.values(), axis=1)
        # Reset index to have `date` as a column and melt to long form
        combined = combined.reset_index()
        long = combined.melt(id_vars='date', var_name='series', value_name='equity')
        long['date'] = pd.to_datetime(long['date'])

        fig = px.line(
            long,
            x='date',
            y='equity',
            color='series',
            labels={'date': 'Date', 'equity': 'Equity', 'series': 'Series'},
            title='Equity Curves',
        )
        fig.update_traces(mode='lines')
        fig.update_layout(legend_title_text='Series')

        st.subheader('Equity Curves')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info('Select one or more models or check the check boxes above to view their equity curves.')


if __name__ == "__main__":
    app()