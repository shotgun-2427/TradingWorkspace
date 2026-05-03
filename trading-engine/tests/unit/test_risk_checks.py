"""Unit tests for src/runtime/risk_checks.py."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.runtime.risk_checks import run_pre_trade_risk_checks  # noqa: E402


@pytest.fixture
def sample_basket() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": "SPY", "target_weight": 0.30, "estimated_trade_dollars": 30_000, "side": "buy"},
            {"symbol": "TLT", "target_weight": 0.20, "estimated_trade_dollars": 20_000, "side": "buy"},
            {"symbol": "GLD", "target_weight": 0.10, "estimated_trade_dollars": 10_000, "side": "buy"},
        ]
    )


def test_blocks_when_basket_empty():
    res = run_pre_trade_risk_checks(basket_df=pd.DataFrame(), nav=100_000.0)
    assert not res.ok
    assert any("empty" in r.lower() for r in res.blocking_reasons)


def test_blocks_on_nonpositive_nav(sample_basket):
    res = run_pre_trade_risk_checks(basket_df=sample_basket, nav=0.0)
    assert not res.ok
    assert any("nav" in r.lower() for r in res.blocking_reasons)


def test_blocks_on_missing_columns():
    bad = pd.DataFrame([{"foo": 1, "bar": 2}])
    res = run_pre_trade_risk_checks(basket_df=bad, nav=100_000.0)
    assert not res.ok


def test_report_contains_pass_or_block_marker(sample_basket):
    res = run_pre_trade_risk_checks(basket_df=sample_basket, nav=100_000.0)
    text = res.report()
    assert "Pre-trade risk:" in text
    assert ("PASS" in text) or ("BLOCK" in text)
