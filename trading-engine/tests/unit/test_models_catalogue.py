"""Smoke tests for every model factory in trading_engine.models.catalogue.

Each model gets:
  * a "build with sane params" constructor test
  * a "runs end-to-end on a synthetic LazyFrame" smoke test
  * an output-shape contract check (date column + ticker weight column)
  * a basic "no NaN / no infinity" guarantee

The goal is to make a regression in any one model fail loudly with a
clear message, without claiming that the model is *correct* — that's
the orchestrator's job.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT) + "/src")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Ensure both `src.` and bare `common.`/`trading_engine.` imports work.
SRC = PROJECT_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from trading_engine.models.catalogue import (  # noqa: E402
    AMMA,
    BollingerBandModel,
    DonchianBreakoutModel,
    DualMACrossoverModel,
    InverseMomentumMeanReversionModel,
    LowVolatilityModel,
    MACDModel,
    MomentumModel,
    NATRMeanReversionModel,
    QualitySharpeModel,
    RelativeStrengthModel,
    RSIMeanReversionModel,
    TrendStrengthModel,
    VolatilityTargetModel,
    validate_weight_frame,
)


# ── Synthetic feature panel ────────────────────────────────────────────────


def _synthetic_panel(tickers: Iterable[str] = ("AAA", "BBB"), n_days: int = 60) -> pl.LazyFrame:
    """Build a deterministic feature panel covering every column any
    model in the catalogue might read."""
    start = date(2025, 1, 1)
    rows: list[dict] = []
    for t in tickers:
        for i in range(n_days):
            d = start + timedelta(days=i)
            # deterministic synthetic series
            px = 100.0 + 0.1 * i + (1.0 if t == "BBB" else 0.0)
            rows.append({
                "date": d,
                "ticker": t,
                "adjusted_close_1d": px,
                # momentum (synthetic, just sin-ish)
                "close_momentum_1": 0.001 * ((i % 7) - 3),
                "close_momentum_10": 0.01 * ((i % 5) - 2),
                "close_momentum_14": 0.02 * ((i % 5) - 2),
                "close_momentum_20": 0.02 * ((i % 5) - 2),
                "close_momentum_30": 0.02 * ((i % 5) - 2),
                "close_momentum_32": 0.02 * ((i % 5) - 2),
                "close_momentum_60": 0.05 * ((i % 3) - 1),
                "close_momentum_64": 0.05 * ((i % 3) - 1),
                "close_momentum_90": 0.05 * ((i % 3) - 1),
                "close_momentum_120": 0.05 * ((i % 3) - 1),
                "close_momentum_240": 0.05 * ((i % 3) - 1),
                # ma columns
                "close_ma_10": px - 0.5,
                "close_ma_50": px - 1.0,
                # bollinger
                "close_sma_20": px,
                "close_std_20": 1.5,
                # donchian
                "close_high_20": px + 1.0,
                "close_low_20": px - 1.0,
                # macd
                "macd_line": 0.2,
                "macd_signal": 0.1,
                # natr
                "natr_7": 1.0,
                "natr_14": 1.2,
                # rsi
                "rsi_14": 25.0 + (i % 50),
                # vol target
                "annual_vol_60d": 0.15,
            })
    df = pl.DataFrame(rows)
    return df.lazy()


# ── Per-model factory tests ────────────────────────────────────────────────


def _expect_clean_weights(out_lf: pl.LazyFrame, ticker: str) -> None:
    df = out_lf.collect()
    assert "date" in df.columns
    assert ticker in df.columns
    col = df.get_column(ticker)
    assert col.dtype == pl.Float64
    assert col.is_finite().all(), f"{ticker} has non-finite weights"


def test_momentum_model_runs():
    f = MomentumModel(trade_ticker="AAA", signal_ticker="BBB",
                      momentum_column="close_momentum_10", inverse=False)
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_inverse_momentum_mean_reversion_runs():
    f = InverseMomentumMeanReversionModel(tickers=["AAA"],
                                          momentum_column="close_momentum_10",
                                          threshold=0.01)
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_natr_mean_reversion_runs():
    f = NATRMeanReversionModel(trade_ticker="AAA")
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_bollinger_band_runs():
    f = BollingerBandModel(trade_ticker="AAA",
                           price_column="adjusted_close_1d",
                           sma_column="close_sma_20",
                           std_column="close_std_20")
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_donchian_breakout_runs():
    f = DonchianBreakoutModel(trade_ticker="AAA",
                              price_column="adjusted_close_1d",
                              high_column="close_high_20",
                              low_column="close_low_20")
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_dual_ma_crossover_runs():
    f = DualMACrossoverModel(trade_ticker="AAA",
                             fast_ma_column="close_ma_10",
                             slow_ma_column="close_ma_50")
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_low_volatility_runs():
    f = LowVolatilityModel(trade_ticker="AAA", natr_column="natr_14",
                           target_natr=1.0, max_weight=1.0)
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_low_volatility_rejects_nonpositive_target():
    with pytest.raises(ValueError):
        LowVolatilityModel(trade_ticker="AAA", natr_column="natr_14",
                           target_natr=0.0)


def test_macd_runs():
    f = MACDModel(trade_ticker="AAA",
                  macd_column="macd_line", signal_column="macd_signal")
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_quality_sharpe_runs():
    f = QualitySharpeModel(trade_ticker="AAA",
                           return_column="close_momentum_60",
                           vol_column="natr_14")
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_relative_strength_runs():
    f = RelativeStrengthModel(trade_ticker="AAA",
                              benchmark_ticker="BBB",
                              momentum_column="close_momentum_60")
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_rsi_mean_reversion_runs():
    f = RSIMeanReversionModel(trade_ticker="AAA", rsi_column="rsi_14")
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_rsi_mean_reversion_validates_thresholds():
    with pytest.raises(ValueError):
        RSIMeanReversionModel(trade_ticker="AAA", rsi_column="rsi_14",
                              oversold=70.0, overbought=30.0)  # swapped


def test_trend_strength_runs():
    f = TrendStrengthModel(trade_ticker="AAA",
                           momentum_column="close_momentum_60",
                           vol_column="natr_14")
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_volatility_target_runs_long_only():
    f = VolatilityTargetModel(trade_ticker="AAA", vol_column="annual_vol_60d",
                              target_vol=0.10)
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


def test_volatility_target_validates_target():
    with pytest.raises(ValueError):
        VolatilityTargetModel(trade_ticker="AAA", vol_column="natr_14",
                              target_vol=0.0)


def test_volatility_target_directional():
    f = VolatilityTargetModel(trade_ticker="AAA", vol_column="annual_vol_60d",
                              target_vol=0.10,
                              momentum_column="close_momentum_60",
                              allow_short=True)
    _expect_clean_weights(f(_synthetic_panel()), "AAA")


# ── AMMA needs a ModelStateBundle, not a flat LazyFrame ─────────────────────


def test_amma_runs_on_bundle():
    from common.bundles import ModelStateBundle
    panel = _synthetic_panel(("TLT-US",), n_days=80).collect()
    bundle = ModelStateBundle(model_state=panel,
                              supplemental_model_state=pl.DataFrame())
    f = AMMA(ticker="TLT-US",
             momentum_weights={10: 0.3, 60: 0.7})
    out = f(bundle).collect()
    assert "date" in out.columns and "TLT-US" in out.columns
    assert out.get_column("TLT-US").is_finite().all()


# ── Output-validator contract ──────────────────────────────────────────────


def test_validate_weight_frame_passes_clean_output():
    f = MomentumModel(trade_ticker="AAA", signal_ticker="BBB",
                      momentum_column="close_momentum_10", inverse=False)
    out = f(_synthetic_panel())
    validate_weight_frame(out, ["AAA"])


def test_validate_weight_frame_rejects_missing_ticker():
    f = MomentumModel(trade_ticker="AAA", signal_ticker="BBB",
                      momentum_column="close_momentum_10", inverse=False)
    out = f(_synthetic_panel())
    with pytest.raises(ValueError):
        validate_weight_frame(out, ["NOT_THERE"])


# ── Registry loads ─────────────────────────────────────────────────────────


def test_registry_imports_cleanly():
    from trading_engine.models.registry import MODELS
    assert isinstance(MODELS, dict)
    assert len(MODELS) > 0
    # Each entry has the canonical fields.
    for name, cfg in MODELS.items():
        assert "tickers" in cfg, f"{name}: missing 'tickers'"
        assert "function" in cfg, f"{name}: missing 'function'"
        assert callable(cfg["function"]), f"{name}: function is not callable"
