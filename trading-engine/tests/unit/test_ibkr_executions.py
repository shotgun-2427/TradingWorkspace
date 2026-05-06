"""Unit tests for ``src.broker.ibkr.executions.list_executions``.

Uses fake fill/contract/execution objects (duck-typed, no IBKR round
trip). Verifies the dict shape contract that ``execution.fill_monitor``
relies on, plus normalization, signing, and ``since`` filtering.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.broker.ibkr.executions import (  # noqa: E402
    _normalize_fill,
    _parse_iso_ts,
    list_executions,
)


def _fake_fill(*, symbol, shares, price, side="BOT", time=None, order_id=42, exec_id="x1"):
    """Build a duck-typed Fill compatible with the normalizer."""
    execution = SimpleNamespace(
        orderId=order_id,
        execId=exec_id,
        shares=shares,
        price=price,
        side=side,
        time=time or datetime(2026, 5, 1, 14, 30, 0, tzinfo=timezone.utc),
    )
    contract = SimpleNamespace(symbol=symbol)
    return SimpleNamespace(execution=execution, contract=contract)


def _fake_client(fills):
    """Build a duck-typed IBKRClient that returns ``fills`` from ib.fills()."""
    ib = SimpleNamespace(fills=lambda: fills)
    return SimpleNamespace(ib=ib, disconnect=MagicMock(), connect=MagicMock())


# ── _normalize_fill ────────────────────────────────────────────────────────


def test_normalize_buy_fill_keeps_positive_qty():
    fill = _fake_fill(symbol="spy", shares=10, price=425.5, side="BOT")
    out = _normalize_fill(fill)
    assert out is not None
    assert out["symbol"] == "SPY"
    assert out["qty"] == 10
    assert out["price"] == 425.5
    assert out["order_id"] == "42"
    assert out["exec_id"] == "x1"


def test_normalize_sell_fill_signs_qty_negative():
    fill = _fake_fill(symbol="QQQ", shares=5, price=400.0, side="SLD")
    out = _normalize_fill(fill)
    assert out is not None
    assert out["qty"] == -5  # SLD → negative


def test_normalize_handles_missing_execution():
    fill = SimpleNamespace(execution=None, contract=SimpleNamespace(symbol="SPY"))
    assert _normalize_fill(fill) is None


def test_normalize_handles_missing_contract():
    fill = SimpleNamespace(
        execution=SimpleNamespace(orderId=1, shares=1, price=1.0, side="BOT", time=None),
        contract=None,
    )
    assert _normalize_fill(fill) is None


def test_normalize_handles_non_numeric_shares():
    fill = _fake_fill(symbol="SPY", shares="not-a-number", price=100.0)
    assert _normalize_fill(fill) is None


def test_normalize_falls_back_to_now_for_missing_ts():
    fill = _fake_fill(symbol="SPY", shares=1, price=1.0, time=None)
    out = _normalize_fill(fill)
    assert out is not None
    # Either an aware or naive ISO string — just verify parseability.
    assert _parse_iso_ts(out["ts"]) is not None


# ── list_executions ────────────────────────────────────────────────────────


def test_list_executions_returns_required_keys():
    fills = [_fake_fill(symbol="SPY", shares=10, price=425.0, side="BOT")]
    out = list_executions(client=_fake_client(fills))
    assert len(out) == 1
    row = out[0]
    # fill_monitor depends on these exact keys.
    for key in ("order_id", "symbol", "qty", "price", "ts"):
        assert key in row, f"missing key: {key}"


def test_list_executions_filters_by_since():
    old = _fake_fill(
        symbol="SPY", shares=10, price=425.0, side="BOT",
        time=datetime(2026, 4, 1, 14, 0, 0, tzinfo=timezone.utc),
    )
    new = _fake_fill(
        symbol="QQQ", shares=20, price=400.0, side="BOT",
        time=datetime(2026, 5, 1, 14, 0, 0, tzinfo=timezone.utc),
    )
    cutoff = datetime(2026, 4, 15, 0, 0, 0, tzinfo=timezone.utc)
    out = list_executions(client=_fake_client([old, new]), since=cutoff)
    syms = [r["symbol"] for r in out]
    assert syms == ["QQQ"]


def test_list_executions_skips_unparseable_fills():
    bad = SimpleNamespace(execution=None, contract=None)
    good = _fake_fill(symbol="SPY", shares=1, price=1.0, side="BOT")
    out = list_executions(client=_fake_client([bad, good]))
    assert len(out) == 1
    assert out[0]["symbol"] == "SPY"


def test_list_executions_returns_empty_when_ib_fills_raises():
    class ExplodingIB:
        def fills(self):
            raise RuntimeError("gateway disconnected")

    client = SimpleNamespace(ib=ExplodingIB(), disconnect=MagicMock(), connect=MagicMock())
    out = list_executions(client=client)
    assert out == []


def test_list_executions_does_not_disconnect_injected_client():
    fills = [_fake_fill(symbol="SPY", shares=1, price=1.0)]
    client = _fake_client(fills)
    list_executions(client=client)
    # We didn't open the connection → we must not close it.
    client.disconnect.assert_not_called()


def test_list_executions_naive_since_handles_aware_fills():
    """Mixing aware fill ts with naive `since` shouldn't crash."""
    aware = _fake_fill(
        symbol="SPY", shares=1, price=1.0, side="BOT",
        time=datetime(2026, 5, 1, 14, 0, 0, tzinfo=timezone.utc),
    )
    naive_cutoff = datetime(2026, 4, 1, 0, 0, 0)  # no tzinfo
    out = list_executions(client=_fake_client([aware]), since=naive_cutoff)
    assert len(out) == 1


def test_parse_iso_ts_round_trips_z_suffix():
    parsed = _parse_iso_ts("2026-05-01T14:00:00Z")
    assert parsed is not None
    assert parsed.year == 2026 and parsed.month == 5 and parsed.day == 1


def test_parse_iso_ts_returns_none_for_garbage():
    assert _parse_iso_ts("not-a-date") is None
    assert _parse_iso_ts("") is None
    assert _parse_iso_ts(None) is None  # type: ignore[arg-type]
