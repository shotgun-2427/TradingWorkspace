"""Unit tests for src/execution/partial_fill_handler.py."""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.execution.partial_fill_handler import (  # noqa: E402
    Action,
    decide_follow_up,
)


def test_full_fill_lets_rest_work():
    d = decide_follow_up(side="buy", qty_total=10, qty_filled=10, last_price=100.0)
    assert d.action == Action.LET_REST_WORK


def test_partial_above_threshold_lets_work():
    d = decide_follow_up(side="buy", qty_total=10, qty_filled=8, last_price=100.0,
                         cancel_below_pct=0.5)
    assert d.action == Action.LET_REST_WORK


def test_partial_below_threshold_reprices_buy_up():
    d = decide_follow_up(side="buy", qty_total=10, qty_filled=2, last_price=100.0,
                         cancel_below_pct=0.5, tick=0.01)
    assert d.action == Action.CANCEL_AND_REPRICE
    assert d.new_price == 100.01


def test_partial_below_threshold_reprices_sell_down():
    d = decide_follow_up(side="sell", qty_total=10, qty_filled=2, last_price=100.0,
                         cancel_below_pct=0.5, tick=0.01)
    assert d.action == Action.CANCEL_AND_REPRICE
    assert d.new_price == 99.99


def test_repricing_off_cancels_only():
    d = decide_follow_up(side="buy", qty_total=10, qty_filled=2, last_price=100.0,
                         cancel_below_pct=0.5, repricing_enabled=False)
    assert d.action == Action.CANCEL_ONLY


def test_zero_total_qty_cancels():
    d = decide_follow_up(side="buy", qty_total=0, qty_filled=0, last_price=100.0)
    assert d.action == Action.CANCEL_ONLY
