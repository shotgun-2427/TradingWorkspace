import pandas as pd
import streamlit as st
import plotly.express as px

from src.dashboard.utils import (
    get_latest_production_audit,
    get_production_audit_models,
    get_production_audit_optimizers,
    get_model_backtest,
    get_portfolio_backtest,
)


def app():
    st.title("Backtest Visualization")
    st.markdown(f"Latest Production Audit Date: {get_latest_production_audit()}")

    # Load available models and optimizers from the latest production audit
    models = get_production_audit_models()
    optimizers = get_production_audit_optimizers()

    # Streamlit multiselects
    selected_models = st.multiselect(
        "Select model backtests to view",
        options=models,
    )

    selected_optimizers = st.multiselect(
        "Select portfolio optimizer backtest to view",
        options=optimizers,
        default=optimizers,
    )

    # Expose selections for downstream plotting/processing
    st.session_state.setdefault("selected_models", selected_models)
    st.session_state.setdefault("selected_optimizers", selected_optimizers)

    # Build and plot combined equity curves for selected models and optimizers
    combined_series = {}

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
        series = (df.set_index('date')['cumulative_return'] + 1).rename(model)
        combined_series[model] = series

    # Load optimizer (portfolio) backtests and prefix names with 'OPT: '
    for opt in selected_optimizers:
        try:
            pdf = get_portfolio_backtest(opt)
        except Exception as exc:
            st.warning(f"Could not load portfolio backtest for optimizer '{opt}': {exc}")
            continue

        if 'date' not in pdf.columns or 'cumulative_return' not in pdf.columns:
            st.warning(f"Portfolio backtest file for '{opt}' missing expected columns")
            continue

        pdf = pdf.copy()
        pdf['date'] = pd.to_datetime(pdf['date'])
        pdf = pdf.sort_values('date')
        series = (pdf.set_index('date')['cumulative_return'] + 1).rename(opt)
        combined_series[opt] = series

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
        st.info('Select one or more models or optimizers to view their equity curves.')


if __name__ == "__main__":
    app()