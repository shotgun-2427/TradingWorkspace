"""Unit tests for src/execution/order_policy.py."""
from __future__ import annotations

import math
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.execution.order_policy import (  # noqa: E402
    DEFAULT_ORDER_POLICY,
    OrderPolicy,
    OrderTicket,
)


def _t(symbol="SPY", qty=10, price=400.0):
    return OrderTicket(symbol=symbol, qty=qty, price=price)


def test_accepts_normal_ticket():
    d = DEFAULT_ORDER_POLICY.evaluate(_t())
    assert d.accept and d.reason == "ok"


def test_rejects_zero_qty():
    d = DEFAULT_ORDER_POLICY.evaluate(_t(qty=0))
    assert not d.accept and "zero" in d.reason


def test_rejects_negative_price():
    d = DEFAULT_ORDER_POLICY.evaluate(_t(price=-1.0))
    assert not d.accept


def test_rejects_nan_qty():
    d = DEFAULT_ORDER_POLICY.evaluate(_t(qty=float("nan")))  # type: ignore[arg-type]
    assert not d.accept


def test_rejects_below_min_notional():
    p = OrderPolicy(min_notional_usd=1_000.0)
    d = p.evaluate(_t(qty=1, price=10.0))   # $10 notional
    assert not d.accept and "minimum" in d.reason


def test_rejects_above_max_notional():
    p = OrderPolicy(max_notional_usd=1_000.0)
    d = p.evaluate(_t(qty=10, price=200.0))  # $2000 notional
    assert not d.accept and "maximum" in d.reason


def test_allowlist_blocks_unknown_symbols():
    p = OrderPolicy(allowlist=("SPY",))
    assert p.evaluate(_t(symbol="SPY")).accept is True
    assert p.evaluate(_t(symbol="QQQ")).accept is False


def test_filter_batch_splits_correctly():
    tickets = [_t(symbol="SPY"), _t(symbol="GOOD", qty=0), _t(symbol="OK")]
    accepted, rejected = DEFAULT_ORDER_POLICY.filter_batch(tickets)
    accepted_syms = [t.symbol for t in accepted]
    rejected_syms = [t.symbol for t, _r in rejected]
    assert "SPY" in accepted_syms and "OK" in accepted_syms
    assert "GOOD" in rejected_syms


def test_finite_required():
    assert DEFAULT_ORDER_POLICY.evaluate(_t(price=math.inf)).accept is False
