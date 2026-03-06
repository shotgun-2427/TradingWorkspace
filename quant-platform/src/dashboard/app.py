from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.backtest.backtester import run_backtest
from src.dashboard.charts import drawdown_chart, equity_chart
from src.dashboard.metrics_view import render_metrics


st.title("Quant Platform Dashboard")

results_path = Path("artifacts/simulations/results.parquet")
weights_path = Path("artifacts/simulations/weights.parquet")

if not results_path.exists() or not weights_path.exists():
    st.warning("Run `python scripts/run_backtest.py` first to generate local artifacts.")
    st.stop()

prices = pd.read_parquet(results_path)
weights = pd.read_parquet(weights_path)

models = ["trend", "mean_reversion", "zscore"]
selected_model = st.selectbox("model", models, index=2)
_ = selected_model

start = st.date_input("start date", value=prices["date"].min().date())
end = st.date_input("end date", value=prices["date"].max().date())

filtered = prices[(prices["date"] >= pd.Timestamp(start)) & (prices["date"] <= pd.Timestamp(end))]
out = run_backtest(filtered, weights)

st.plotly_chart(equity_chart(out["equity_curve"]), use_container_width=True)
st.plotly_chart(drawdown_chart(out["drawdown"]), use_container_width=True)
render_metrics(out["metrics"])
st.dataframe(weights)
