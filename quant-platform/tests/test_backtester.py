import pandas as pd

from src.backtest.backtester import run_backtest


def test_backtest_outputs_expected_keys():
    prices = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"]),
            "symbol": ["SPY", "SPY", "QQQ", "QQQ"],
            "close": [100, 101, 200, 198],
            "volume": [1000, 1000, 1000, 1000],
        }
    )
    weights = pd.DataFrame({"symbol": ["SPY", "QQQ"], "weight": [0.5, 0.5]})
    out = run_backtest(prices, weights)

    assert set(out.keys()) == {"daily_returns", "equity_curve", "drawdown", "metrics"}
    assert "sharpe" in out["metrics"]
