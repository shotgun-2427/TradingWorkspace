"""
Unit tests for the per-ETF backtest pipeline.

Covers:
  * Score → weight conversion (clipping, sign-flip in mirror mode, NaN-safe).
  * Single-model end-to-end run on a tiny synthetic universe — output files
    appear at the expected paths, the equity curve is non-trivial, and no NaNs.
  * Summary metrics are well-formed for a non-trivial run.
  * The dashboard's /api/model/per-etf-backtest endpoint serves a real artifact
    when present and falls back gracefully when absent.
"""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC = PROJECT_ROOT / "src"
for p in (PROJECT_ROOT, SRC):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from src.research.per_etf_backtest import (  # noqa: E402
    _score_to_weight,
    _summary_metrics,
    backtest_one_model,
    MODEL_IDS,
)
from src.strategies.etf.momentum import MomentumSignal  # noqa: E402


# ── 1. Score → weight ─────────────────────────────────────────────────────


def test_score_to_weight_basic():
    assert _score_to_weight(0.0) == 0.0
    assert _score_to_weight(-1.0) == 0.0           # below avg → flat (long-only)
    assert _score_to_weight(1.0) == 0.5            # 1-sigma → half invested
    assert _score_to_weight(2.0) == 1.0            # 2-sigma → fully invested
    assert _score_to_weight(10.0) == 1.0           # clipped at 1.0
    assert _score_to_weight(None) == 0.0
    assert _score_to_weight(float("nan")) == 0.0
    assert _score_to_weight(float("inf")) == 0.0


def test_score_to_weight_mirror_flips_sign():
    assert _score_to_weight(-2.0, mirror=True) == 1.0
    assert _score_to_weight(2.0, mirror=True) == 0.0
    assert _score_to_weight(-1.0, mirror=True) == 0.5


# ── 2. End-to-end run on synthetic data ────────────────────────────────────


def _synthetic_universe(n_days: int = 600, n_tickers: int = 4, seed: int = 0) -> pd.DataFrame:
    """Long-format prices with N tickers and N days of slightly drifting paths."""
    rng = np.random.default_rng(seed)
    start = date(2020, 1, 1)
    rows = []
    for j in range(n_tickers):
        sym = chr(ord("A") + j) + "AA"
        drift = (j - n_tickers / 2) * 0.0005
        px = 100.0
        for i in range(n_days):
            px *= 1.0 + drift + float(rng.normal(0, 0.01))
            rows.append({"date": pd.Timestamp(start + timedelta(days=i)), "symbol": sym, "close": px})
    return pd.DataFrame(rows)


def test_backtest_one_model_produces_non_trivial_output():
    prices = _synthetic_universe(n_days=600, n_tickers=4)
    universe = sorted(prices["symbol"].unique())
    out = backtest_one_model(
        prices_long=prices,
        model=MomentumSignal(lookback=60, skip=2),
        model_id="momentum",
        universe=universe,
        rebalance_interval=21,
        burn_in=70,
        quiet=True,
    )
    assert set(out.keys()) == set(universe), "every ticker must have a result frame"
    for sym, df in out.items():
        assert set(["date", "weight", "daily_return", "equity", "drawdown"]) <= set(df.columns)
        assert df["equity"].is_monotonic_increasing is False  # has some movement
        # Finite everywhere
        for c in ("weight", "daily_return", "equity", "drawdown"):
            assert df[c].astype(float).apply(lambda x: np.isfinite(x)).all(), (
                f"{sym}: non-finite values in {c}"
            )
        # Weight is long-only and in [0, 1]
        assert df["weight"].min() >= 0.0 and df["weight"].max() <= 1.0


def test_summary_metrics_are_well_formed():
    prices = _synthetic_universe(n_days=600, n_tickers=4)
    universe = sorted(prices["symbol"].unique())
    out = backtest_one_model(
        prices_long=prices,
        model=MomentumSignal(lookback=60, skip=2),
        model_id="momentum",
        universe=universe,
        rebalance_interval=21,
        burn_in=70,
        quiet=True,
    )
    # Pick any non-empty result and check the summary shape.
    df = next(d for d in out.values() if not d.empty)
    s = _summary_metrics(df)
    assert set(s.keys()) >= {
        "sharpe_ratio",
        "total_return",
        "annualized_return",
        "max_drawdown",
        "num_signals",
        "years_observed",
    }


# ── 3. MODEL_IDS expected set ─────────────────────────────────────────────


def test_model_ids_match_dashboard_dropdown():
    assert set(MODEL_IDS) == {
        # Cross-sectional / ensemble models.
        "amma",
        "amma_mirror",
        "momentum",
        "natr_mean_reversion",
        "inverse_momentum_mean_reversion",
        "ensemble",
        # Time-series benchmarks + signals that should beat B&H on a single
        # ticker. The dashboard dropdown lists these first since they're the
        # most useful per-ETF view.
        "buy_and_hold",
        "trend_filter",
        "tsmom_ts",
        "vol_target_trend",
        "adaptive_trend",
    }


# ── 4. Endpoint serves real data when artifact present ────────────────────


def test_per_etf_endpoint_returns_real_curve_when_artifact_exists(tmp_path, monkeypatch):
    """Monkey-patch _PER_ETF_ROOT to a tmp dir, drop a synthetic parquet there,
    and verify the endpoint serves it."""
    from src.dashboard.routers import model as model_router

    # Inject a tiny artifact for ZZZ/foo.parquet.
    target = tmp_path / "ZZZ"
    target.mkdir(parents=True)
    pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=10),
        "weight": [0.0] * 5 + [0.5] * 5,
        "daily_return": [0.0] * 5 + [0.01, -0.005, 0.003, 0.002, 0.001],
        "equity": [1.0] * 5 + [1.005, 1.0025, 1.005, 1.007, 1.008],
        "drawdown": [0.0] * 10,
    }).to_parquet(target / "foo.parquet", index=False)
    (target / "summary.json").write_text(json.dumps({"foo": {"sharpe_ratio": 1.5}}))

    monkeypatch.setattr(model_router, "_PER_ETF_ROOT", tmp_path)

    payload = model_router.per_etf_backtest(ticker="ZZZ", model="foo")
    assert payload["available"] is True
    assert payload["ticker"] == "ZZZ"
    assert payload["model"] == "foo"
    assert payload["summary"]["sharpe_ratio"] == 1.5
    assert payload["series"]
    pts = payload["series"][0]["points"]
    # Burn-in (rows with weight=0) was trimmed → starts at the first active rebalance.
    assert pts[0]["date"] == "2024-01-06"
    assert pts[0]["value"] == 1.005


def test_per_etf_endpoint_returns_available_false_when_missing(tmp_path, monkeypatch):
    from src.dashboard.routers import model as model_router
    monkeypatch.setattr(model_router, "_PER_ETF_ROOT", tmp_path)
    payload = model_router.per_etf_backtest(ticker="NONE", model="missing")
    assert payload["available"] is False
    assert "ticker" in payload and "model" in payload


# ── 5. Buy-and-hold overlay ───────────────────────────────────────────────


def test_per_etf_endpoint_overlays_buy_hold_from_artifact(tmp_path, monkeypatch):
    """When a ``buy_and_hold.parquet`` artifact exists alongside the model
    artifact, the endpoint should overlay it as ``buy_hold_points`` aligned
    to the model's window."""
    from src.dashboard.routers import model as model_router

    target = tmp_path / "ZZZ"
    target.mkdir(parents=True)
    # Model artifact: 10 rows.
    pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=10),
        "weight": [0.0] * 5 + [0.5] * 5,
        "daily_return": [0.0] * 5 + [0.01, -0.005, 0.003, 0.002, 0.001],
        "equity": [1.0] * 5 + [1.005, 1.0025, 1.005, 1.007, 1.008],
        "drawdown": [0.0] * 10,
    }).to_parquet(target / "model.parquet", index=False)
    # B&H artifact: same dates, different equity curve.
    pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=10),
        "weight": [1.0] * 10,
        "daily_return": [0.0, 0.01, -0.005, 0.002, 0.003, 0.001, 0.004, -0.002, 0.001, 0.002],
        "equity": [1.0, 1.01, 1.00495, 1.0069, 1.0099, 1.011, 1.0151, 1.013, 1.014, 1.016],
        "drawdown": [0.0] * 10,
    }).to_parquet(target / "buy_and_hold.parquet", index=False)
    (target / "summary.json").write_text(
        json.dumps(
            {
                "model": {"sharpe_ratio": 1.5, "total_return": 0.008},
                "buy_and_hold": {"sharpe_ratio": 0.9, "total_return": 0.016},
            }
        )
    )
    monkeypatch.setattr(model_router, "_PER_ETF_ROOT", tmp_path)

    payload = model_router.per_etf_backtest(ticker="ZZZ", model="model")
    # Series + buy-and-hold overlay both present.
    assert payload["available"] is True
    assert payload["buy_hold_points"]
    # The B&H curve is rebased to 1.0 at the model's first active date.
    bh = payload["buy_hold_points"]
    assert bh[0]["value"] == pytest.approx(1.0)
    # B&H summary copied through so the KPI row can show the benchmark.
    assert payload["buy_hold_summary"]["sharpe_ratio"] == 0.9


def test_per_etf_endpoint_skips_buy_hold_overlay_for_buy_hold_model(tmp_path, monkeypatch):
    """Don't overlay B&H on top of itself — silly and confusing."""
    from src.dashboard.routers import model as model_router

    target = tmp_path / "ZZZ"
    target.mkdir(parents=True)
    pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=10),
        "weight": [1.0] * 10,
        "daily_return": [0.001] * 10,
        "equity": np.cumprod([1.001] * 10).tolist(),
        "drawdown": [0.0] * 10,
    }).to_parquet(target / "buy_and_hold.parquet", index=False)

    monkeypatch.setattr(model_router, "_PER_ETF_ROOT", tmp_path)
    payload = model_router.per_etf_backtest(ticker="ZZZ", model="buy_and_hold")
    assert payload["available"] is True
    assert "buy_hold_points" not in payload


def test_per_etf_endpoint_omits_buy_hold_when_flag_off(tmp_path, monkeypatch):
    from src.dashboard.routers import model as model_router

    target = tmp_path / "ZZZ"
    target.mkdir(parents=True)
    pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=10),
        "weight": [0.5] * 10,
        "daily_return": [0.001] * 10,
        "equity": np.cumprod([1.001] * 10).tolist(),
        "drawdown": [0.0] * 10,
    }).to_parquet(target / "model.parquet", index=False)
    pd.DataFrame({
        "date": pd.date_range("2024-01-01", periods=10),
        "weight": [1.0] * 10,
        "daily_return": [0.001] * 10,
        "equity": np.cumprod([1.001] * 10).tolist(),
        "drawdown": [0.0] * 10,
    }).to_parquet(target / "buy_and_hold.parquet", index=False)
    monkeypatch.setattr(model_router, "_PER_ETF_ROOT", tmp_path)

    payload = model_router.per_etf_backtest(ticker="ZZZ", model="model", include_buy_hold=False)
    assert payload["available"] is True
    assert "buy_hold_points" not in payload
