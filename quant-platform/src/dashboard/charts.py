from __future__ import annotations

import plotly.express as px
import pandas as pd


def equity_chart(equity_curve: pd.Series):
    frame = equity_curve.reset_index()
    frame.columns = ["date", "equity"]
    return px.line(frame, x="date", y="equity", title="Equity Curve")


def drawdown_chart(drawdown: pd.Series):
    frame = drawdown.reset_index()
    frame.columns = ["date", "drawdown"]
    return px.area(frame, x="date", y="drawdown", title="Drawdown")
