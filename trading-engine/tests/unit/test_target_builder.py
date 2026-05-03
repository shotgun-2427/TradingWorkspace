"""Unit tests for src/portfolio/target_builder.py + sizing.py + hedging_overlay.py."""
from __future__ import annotations

import math
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.portfolio.target_builder import (  # noqa: E402
    TargetBookConfig,
    build_target_book,
)
from src.portfolio.sizing import size_basket, size_position  # noqa: E402
from src.portfolio.hedging_overlay import (  # noqa: E402
    HedgeOverlayConfig,
    apply_overlay,
)


def test_build_target_book_drops_nan_inf():
    out = build_target_book({"A": 0.2, "B": float("nan"), "C": float("inf")})
    assert "A" in out and "B" not in out and "C" not in out


def test_build_target_book_caps_per_name():
    out = build_target_book({"A": 0.99}, config=TargetBookConfig(max_per_name=0.25))
    assert math.isclose(out["A"], 0.25, abs_tol=1e-9)


def test_build_target_book_renormalises_l1():
    out = build_target_book(
        {"A": 0.40, "B": 0.40, "C": 0.40},
        config=TargetBookConfig(max_per_name=0.40, l1_budget=1.0),
    )
    l1 = sum(abs(v) for v in out.values())
    assert math.isclose(l1, 1.0, abs_tol=1e-9)


def test_build_target_book_drops_noise():
    out = build_target_book({"A": 0.001, "B": 0.5},
                            config=TargetBookConfig(noise_threshold=0.01))
    assert "A" not in out and "B" in out


def test_build_target_book_idempotent():
    once = build_target_book({"A": 0.4, "B": 0.4})
    twice = build_target_book(once)
    assert once == twice


def test_size_position_basic():
    p = size_position("SPY", weight=0.10, nav=100_000.0, price=400.0)
    # 100k * 0.10 = 10k → 10000/400 = 25 shares
    assert p.shares == 25
    assert math.isclose(p.notional, 10_000.0, abs_tol=1.0)


def test_size_position_handles_bad_inputs():
    p = size_position("SPY", weight=float("nan"), nav=100_000.0, price=400.0)
    assert p.shares == 0


def test_size_basket_skips_missing_prices():
    sized = size_basket({"A": 0.1, "B": 0.1}, prices={"A": 100.0}, nav=10_000.0)
    syms = [s.symbol for s in sized]
    assert syms == ["A"]


def test_overlay_disabled_passthrough():
    weights = {"SPY-US": 0.5, "GLD-US": 0.2}
    out = apply_overlay(weights, macro_signal=-1.0)
    assert out == weights and out is not weights  # fresh dict


def test_overlay_enabled_triggers_on_negative_macro():
    cfg = HedgeOverlayConfig(enabled=True, hedge_symbol="TLT-US",
                              hedge_weight=0.10, trigger_signal_threshold=-0.5)
    weights = {"SPY-US": 0.5}
    out = apply_overlay(weights, macro_signal=-0.7, config=cfg)
    assert out["TLT-US"] == 0.10


def test_overlay_enabled_no_trigger_on_neutral():
    cfg = HedgeOverlayConfig(enabled=True, trigger_signal_threshold=-0.5)
    out = apply_overlay({"SPY-US": 0.5}, macro_signal=0.0, config=cfg)
    assert "TLT-US" not in out
