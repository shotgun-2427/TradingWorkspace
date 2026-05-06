"""Unit tests for ``src.broker.ibkr.reconciler.diff_book_vs_account``.

Distinct from ``tests/unit/test_reconciler.py`` (which tests the
runtime EOD orchestrator). This file pins down the diff function the
broker module exposes — its dict shape, signing, and the
material-threshold-friendly ``notional_diff_usd`` field.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.broker.ibkr.reconciler import diff_book_vs_account  # noqa: E402


def _fake_position(symbol: str, qty: float, avg_cost: float = 0.0):
    """Build a duck-typed IB Position object."""
    return SimpleNamespace(
        contract=SimpleNamespace(symbol=symbol),
        position=qty,
        avgCost=avg_cost,
    )


def _fake_client(positions, *, account=None):
    cfg = SimpleNamespace(account=account)
    client = SimpleNamespace(
        ib=SimpleNamespace(),
        config=cfg,
        positions=lambda account=None: positions,
        disconnect=MagicMock(),
        connect=MagicMock(),
    )
    return client


# ── perfect-match cases ────────────────────────────────────────────────────


def test_no_diff_when_book_matches_broker():
    book = {"SPY": 100, "QQQ": 50}
    client = _fake_client([
        _fake_position("SPY", 100, 425.0),
        _fake_position("QQQ", 50, 400.0),
    ])
    diffs = diff_book_vs_account(client=client, book=book)
    assert diffs == []


def test_empty_book_and_empty_broker_yields_no_diff():
    diffs = diff_book_vs_account(client=_fake_client([]), book={})
    assert diffs == []


# ── diff detection ─────────────────────────────────────────────────────────


def test_book_long_broker_short_flagged():
    book = {"SPY": 100}
    client = _fake_client([_fake_position("SPY", 90, 425.0)])
    diffs = diff_book_vs_account(client=client, book=book)
    assert len(diffs) == 1
    d = diffs[0]
    assert d["symbol"] == "SPY"
    assert d["book_qty"] == 100
    assert d["broker_qty"] == 90
    assert d["qty_diff"] == -10  # broker - book
    assert d["ref_price"] == 425.0
    assert d["notional_diff_usd"] == 10 * 425.0


def test_symbol_only_at_broker_appears_as_diff():
    """Position the engine doesn't know about → must show up."""
    client = _fake_client([_fake_position("XLF", 25, 50.0)])
    diffs = diff_book_vs_account(client=client, book={})
    assert len(diffs) == 1
    assert diffs[0]["symbol"] == "XLF"
    assert diffs[0]["book_qty"] == 0
    assert diffs[0]["broker_qty"] == 25
    assert diffs[0]["notional_diff_usd"] == 25 * 50.0


def test_symbol_only_in_book_appears_as_diff():
    """Engine thinks it has it; broker says nothing → flag it."""
    client = _fake_client([])
    diffs = diff_book_vs_account(client=client, book={"GLD": 10})
    assert len(diffs) == 1
    assert diffs[0]["symbol"] == "GLD"
    assert diffs[0]["book_qty"] == 10
    assert diffs[0]["broker_qty"] == 0


def test_diff_has_required_keys_for_eod_caller():
    """``runtime.end_of_day_reconcile`` reads ``notional_diff_usd``."""
    client = _fake_client([_fake_position("SPY", 95, 425.0)])
    diffs = diff_book_vs_account(client=client, book={"SPY": 100})
    assert len(diffs) == 1
    d = diffs[0]
    for key in (
        "symbol", "book_qty", "broker_qty", "qty_diff",
        "ref_price", "notional_diff_usd", "as_of",
    ):
        assert key in d, f"missing key: {key}"


def test_diffs_sorted_by_symbol():
    client = _fake_client([
        _fake_position("ZZZ", 1, 1.0),
        _fake_position("AAA", 1, 1.0),
        _fake_position("MMM", 1, 1.0),
    ])
    diffs = diff_book_vs_account(client=client, book={})
    assert [d["symbol"] for d in diffs] == ["AAA", "MMM", "ZZZ"]


def test_as_of_propagated_to_each_diff():
    client = _fake_client([_fake_position("SPY", 95, 100.0)])
    target = date(2026, 5, 6)
    diffs = diff_book_vs_account(client=client, book={"SPY": 100}, as_of=target)
    assert diffs[0]["as_of"] == "2026-05-06"


def test_as_of_none_yields_none_in_diff():
    client = _fake_client([_fake_position("SPY", 95, 100.0)])
    diffs = diff_book_vs_account(client=client, book={"SPY": 100})
    assert diffs[0]["as_of"] is None


# ── defensive coercion ────────────────────────────────────────────────────


def test_handles_position_with_non_numeric_qty():
    bad = SimpleNamespace(
        contract=SimpleNamespace(symbol="SPY"),
        position="not-a-number",
        avgCost=100.0,
    )
    good = _fake_position("QQQ", 10, 50.0)
    client = _fake_client([bad, good])
    # Bad position becomes qty=0 which won't equal book entries → still yields a diff
    diffs = diff_book_vs_account(client=client, book={"SPY": 5, "QQQ": 10})
    syms = {d["symbol"] for d in diffs}
    # SPY: book=5 broker=0 → diff; QQQ: matches → no diff
    assert syms == {"SPY"}


def test_position_without_symbol_is_skipped():
    no_symbol = SimpleNamespace(
        contract=SimpleNamespace(symbol=None),
        position=10,
        avgCost=100.0,
    )
    client = _fake_client([no_symbol])
    diffs = diff_book_vs_account(client=client, book={})
    assert diffs == []


def test_lowercase_symbol_in_position_is_normalized():
    pos = SimpleNamespace(
        contract=SimpleNamespace(symbol="spy"),
        position=10,
        avgCost=100.0,
    )
    client = _fake_client([pos])
    diffs = diff_book_vs_account(client=client, book={"SPY": 5})
    assert len(diffs) == 1
    assert diffs[0]["symbol"] == "SPY"
