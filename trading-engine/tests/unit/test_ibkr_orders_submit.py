"""Unit tests for ``src.broker.ibkr.orders.submit_one``.

Exercises the module-level ``submit_one`` shim that ``order_router``
calls. Uses a mocked ``IBKROrderManager`` so no IB connection is
opened.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.broker.ibkr.orders import submit_one  # noqa: E402
from src.execution.order_policy import OrderTicket  # noqa: E402


def _fake_trade(*, symbol="SPY", order_id=99, perm_id=1234,
                action="BUY", qty=10, status="Filled",
                fills=()):
    return SimpleNamespace(
        contract=SimpleNamespace(symbol=symbol),
        order=SimpleNamespace(
            orderId=order_id,
            permId=perm_id,
            action=action,
            orderType="MKT",
            totalQuantity=qty,
        ),
        orderStatus=SimpleNamespace(status=status),
        fills=list(fills),
    )


def _fake_manager(trade):
    """Manager with place_order returning ``trade`` and a mock client."""
    manager = MagicMock()
    manager.place_order = MagicMock(return_value=trade)
    manager.client = SimpleNamespace(disconnect=MagicMock())
    return manager


# ── happy path ─────────────────────────────────────────────────────────────


def test_buy_ticket_routes_to_place_order_and_returns_ok():
    trade = _fake_trade(symbol="SPY", order_id=42, action="BUY", qty=10)
    manager = _fake_manager(trade)

    ticket = OrderTicket(symbol="SPY", qty=10, price=425.0, side="auto")
    result = submit_one(ticket, manager=manager)

    assert result["ok"] is True
    assert result["symbol"] == "SPY"
    assert result["action"] == "BUY"
    assert result["qty"] == 10
    assert result["order_id"] == 42
    assert result["perm_id"] == 1234
    assert result["status"] == "Filled"
    # We injected the manager — submit_one must NOT have closed it.
    manager.client.disconnect.assert_not_called()


def test_negative_qty_routes_as_sell():
    trade = _fake_trade(symbol="QQQ", action="SELL", qty=5)
    manager = _fake_manager(trade)

    ticket = OrderTicket(symbol="QQQ", qty=-5, price=400.0, side="auto")
    result = submit_one(ticket, manager=manager)

    assert result["ok"] is True
    assert result["action"] == "SELL"
    assert result["qty"] == 5
    # Verify the OrderRequest passed in had action=SELL and qty=5 (positive abs)
    call_request = manager.place_order.call_args.args[0]
    assert call_request.action == "SELL"
    assert call_request.quantity == 5.0


def test_symbol_normalized_to_upper():
    trade = _fake_trade(symbol="SPY")
    manager = _fake_manager(trade)
    ticket = OrderTicket(symbol="  spy  ", qty=1, price=400.0, side="auto")
    result = submit_one(ticket, manager=manager)
    assert result["ok"] is True
    assert result["symbol"] == "SPY"
    call_request = manager.place_order.call_args.args[0]
    assert call_request.symbol == "SPY"


# ── reject paths ───────────────────────────────────────────────────────────


def test_zero_qty_returns_ok_false_without_calling_broker():
    manager = _fake_manager(_fake_trade())
    ticket = OrderTicket(symbol="SPY", qty=0, price=425.0, side="auto")
    result = submit_one(ticket, manager=manager)
    assert result["ok"] is False
    assert "zero quantity" in result["error"]
    manager.place_order.assert_not_called()


def test_broker_exception_captured_in_result():
    manager = MagicMock()
    manager.place_order = MagicMock(side_effect=RuntimeError("Pacing violation"))
    manager.client = SimpleNamespace(disconnect=MagicMock())

    ticket = OrderTicket(symbol="SPY", qty=10, price=425.0, side="auto")
    result = submit_one(ticket, manager=manager)
    assert result["ok"] is False
    assert "Pacing violation" in result["error"]
    assert result["symbol"] == "SPY"


def test_malformed_ticket_returns_audit_dict_not_raises():
    bad = SimpleNamespace()  # no symbol/qty/price attrs at all
    manager = _fake_manager(_fake_trade())
    result = submit_one(bad, manager=manager)
    assert result["ok"] is False
    assert "ticket build failed" in result["error"]
    manager.place_order.assert_not_called()


# ── order_router contract ──────────────────────────────────────────────────


def test_result_dict_is_json_serializable():
    """order_router writes the result to a JSON audit file."""
    import json
    trade = _fake_trade()
    manager = _fake_manager(trade)
    ticket = OrderTicket(symbol="SPY", qty=10, price=425.0, side="auto")
    result = submit_one(ticket, manager=manager)
    json.dumps(result)  # must not raise
