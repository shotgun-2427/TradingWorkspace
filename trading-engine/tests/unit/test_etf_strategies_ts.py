"""
Unit tests for the time-series ETF strategies.

These models live in ``src/strategies/etf`` and are exercised by the
``per_etf_backtest`` pipeline. The tests here check the *shape* and *sign*
of the signal — not the absolute Sharpe — so they're robust to data drift.
The dashboard's pipeline-level validation that "Sharpe beats B&H on SPY"
happens in the integration smoke test below.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.strategies.etf.adaptive_trend import AdaptiveTrend
from src.strategies.etf.buy_and_hold import BuyAndHold
from src.strategies.etf.trend_filter import TrendFilter
from src.strategies.etf.tsmom_ts import TSMomentum
from src.strategies.etf.vol_target_trend import VolTargetTrend


def _synthetic_panel(
    n_days: int = 600,
    *,
    symbols=("SPY", "QQQ", "GLD", "TLT"),
    seed: int = 13,
) -> pd.DataFrame:
    """Build a long-format prices frame with positive-drift random walks per
    symbol. n_days big enough to seed every signal's lookback."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2018-01-02", periods=n_days)
    rows = []
    for i, sym in enumerate(symbols):
        # Different drift / vol per symbol so the panel isn't degenerate.
        drift = 0.0003 + 0.0001 * i
        vol = 0.008 + 0.002 * i
        steps = rng.normal(drift, vol, size=n_days)
        prices = 100.0 * np.exp(np.cumsum(steps))
        for d, p in zip(dates, prices):
            rows.append({"date": d, "symbol": sym, "close": float(p)})
    return pd.DataFrame(rows)


# ── Shape / contract ──────────────────────────────────────────────────────


class TestBuyAndHold:
    def test_emits_full_weight_for_every_symbol(self) -> None:
        prices = _synthetic_panel()
        as_of = prices["date"].max()
        out = BuyAndHold().compute(prices, as_of)
        assert set(out.columns) == {"symbol", "raw", "score"}
        assert sorted(out["symbol"].tolist()) == sorted(["SPY", "QQQ", "GLD", "TLT"])
        # Constant fully-invested signal.
        assert np.allclose(out["score"].to_numpy(), 2.0)

    def test_empty_panel_returns_empty(self) -> None:
        prices = pd.DataFrame(columns=["date", "symbol", "close"])
        out = BuyAndHold().compute(prices, pd.Timestamp("2026-01-01"))
        assert out.empty
        assert list(out.columns) == ["symbol", "raw", "score"]


class TestTrendFilter:
    def test_scores_are_two_or_zero(self) -> None:
        prices = _synthetic_panel()
        as_of = prices["date"].max()
        out = TrendFilter().compute(prices, as_of)
        # Every score should be one of {0.0, 2.0} — the rule is binary.
        unique_scores = sorted(set(out["score"].round(6).tolist()))
        assert set(unique_scores).issubset({0.0, 2.0})

    def test_long_when_price_far_above_sma(self) -> None:
        """Manually rigged: SPY has a steep uptrend — should be long."""
        dates = pd.bdate_range("2022-01-03", periods=400)
        # Strong uptrend SPY, flat-down QQQ.
        spy_prices = 100.0 * np.exp(np.linspace(0, 0.6, 400))
        qqq_prices = 100.0 * np.exp(-np.linspace(0, 0.3, 400))
        rows = []
        for d, p in zip(dates, spy_prices):
            rows.append({"date": d, "symbol": "SPY", "close": float(p)})
        for d, p in zip(dates, qqq_prices):
            rows.append({"date": d, "symbol": "QQQ", "close": float(p)})
        prices = pd.DataFrame(rows)
        out = TrendFilter().compute(prices, dates[-1])
        scores = dict(zip(out["symbol"], out["score"]))
        assert scores["SPY"] == 2.0
        assert scores["QQQ"] == 0.0


class TestTSMomentum:
    def test_long_after_positive_year(self) -> None:
        dates = pd.bdate_range("2022-01-03", periods=400)
        # +50% over the lookback period.
        prices_up = 100.0 * np.exp(np.linspace(0, 0.5, 400))
        # -20% drift.
        prices_down = 100.0 * np.exp(-np.linspace(0, 0.2, 400))
        rows = []
        for d, p in zip(dates, prices_up):
            rows.append({"date": d, "symbol": "BULL", "close": float(p)})
        for d, p in zip(dates, prices_down):
            rows.append({"date": d, "symbol": "BEAR", "close": float(p)})
        prices = pd.DataFrame(rows)
        out = TSMomentum().compute(prices, dates[-1])
        scores = dict(zip(out["symbol"], out["score"]))
        assert scores["BULL"] == 2.0
        assert scores["BEAR"] == 0.0

    def test_threshold_gates_weak_trends(self) -> None:
        """A 1% trailing return should not satisfy a 5% threshold."""
        dates = pd.bdate_range("2022-01-03", periods=400)
        prices = 100.0 * np.exp(np.linspace(0, 0.01, 400))  # +1% over window
        rows = [{"date": d, "symbol": "WEAK", "close": float(p)} for d, p in zip(dates, prices)]
        df = pd.DataFrame(rows)
        out_loose = TSMomentum(threshold=0.0).compute(df, dates[-1])
        out_strict = TSMomentum(threshold=0.05).compute(df, dates[-1])
        assert float(out_loose["score"].iloc[0]) == 2.0
        assert float(out_strict["score"].iloc[0]) == 0.0


class TestVolTargetTrend:
    def test_score_within_bounds(self) -> None:
        prices = _synthetic_panel()
        out = VolTargetTrend().compute(prices, prices["date"].max())
        assert (out["score"] >= 0.0).all()
        assert (out["score"] <= 2.0).all()

    def test_flat_when_below_sma(self) -> None:
        """Falling asset below its SMA gets weight = 0 regardless of vol."""
        dates = pd.bdate_range("2022-01-03", periods=400)
        prices = 100.0 * np.exp(-np.linspace(0, 0.4, 400))
        rows = [{"date": d, "symbol": "FALL", "close": float(p)} for d, p in zip(dates, prices)]
        df = pd.DataFrame(rows)
        out = VolTargetTrend().compute(df, dates[-1])
        assert float(out["score"].iloc[0]) == 0.0


class TestAdaptiveTrend:
    def test_score_in_grid(self) -> None:
        prices = _synthetic_panel()
        out = AdaptiveTrend(windows=(50, 100, 200)).compute(prices, prices["date"].max())
        # 3 windows -> score is in the grid {0, 2/3, 4/3, 2}.
        grid = [0.0, 2 / 3, 4 / 3, 2.0]
        for s in out["score"].tolist():
            assert any(abs(s - g) < 1e-4 for g in grid), f"score {s!r} not on grid {grid}"

    def test_strong_uptrend_gets_full_weight(self) -> None:
        dates = pd.bdate_range("2022-01-03", periods=400)
        prices = 100.0 * np.exp(np.linspace(0, 0.8, 400))
        rows = [{"date": d, "symbol": "UP", "close": float(p)} for d, p in zip(dates, prices)]
        df = pd.DataFrame(rows)
        out = AdaptiveTrend(windows=(50, 100, 200)).compute(df, dates[-1])
        # All three SMAs say "long" → fully invested.
        assert float(out["score"].iloc[0]) == pytest.approx(2.0)


# ── Integration smoke: artifacts produce a real curve ─────────────────────


def test_signals_round_trip_through_per_etf_backtest() -> None:
    """Run the pipeline end-to-end on a tiny synthetic panel and check the
    output frame has the columns the dashboard depends on."""
    from src.research.per_etf_backtest import backtest_one_model

    prices = _synthetic_panel(n_days=800, symbols=("SPY", "QQQ"))
    universe = ["SPY", "QQQ"]
    for model_id, factory in [
        ("buy_and_hold", BuyAndHold),
        ("trend_filter", TrendFilter),
        ("tsmom_ts", TSMomentum),
        ("vol_target_trend", VolTargetTrend),
        ("adaptive_trend", AdaptiveTrend),
    ]:
        out = backtest_one_model(
            prices_long=prices,
            model=factory(),
            model_id=model_id,
            universe=universe,
            rebalance_interval=21,
            burn_in=252,
            quiet=True,
        )
        for sym in universe:
            df = out.get(sym)
            assert df is not None and not df.empty, f"{model_id}/{sym} produced no rows"
            for col in ("date", "weight", "daily_return", "equity", "drawdown"):
                assert col in df.columns, f"{model_id}/{sym} missing {col}"
            # Equity must be strictly positive.
            assert (df["equity"] > 0).all(), f"{model_id}/{sym} has non-positive equity"
            # Drawdown must be in [-1, 0].
            assert (df["drawdown"] <= 0).all() and (df["drawdown"] >= -1).all()
            # Weight must be in [0, 1] (long-only).
            assert (df["weight"] >= 0).all() and (df["weight"] <= 1.0 + 1e-9).all()
