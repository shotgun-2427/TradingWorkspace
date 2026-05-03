"""Unit tests for end-of-day reconciliation + auto-rebalance decision."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.runtime.auto_rebalance import (  # noqa: E402
    decide_rebalance,
    is_month_end_window,
)
from src.runtime.end_of_day_reconcile import reconcile_end_of_day  # noqa: E402


# ── auto_rebalance ─────────────────────────────────────────────────────────


def test_force_flag_always_rebalances():
    d = decide_rebalance(latest_signal_date=date(2026, 4, 1), today=date(2026, 4, 30), force=True)
    assert d.rebalance is True
    assert "force" in d.reason.lower()


def test_no_signal_means_rebalance():
    d = decide_rebalance(latest_signal_date=None, today=date(2026, 4, 30))
    assert d.rebalance is True


def test_signal_in_prior_month_triggers_rebalance():
    d = decide_rebalance(latest_signal_date=date(2026, 3, 28), today=date(2026, 4, 5))
    assert d.rebalance is True


def test_signal_in_current_month_skips():
    d = decide_rebalance(latest_signal_date=date(2026, 4, 1), today=date(2026, 4, 30))
    assert d.rebalance is False


def test_month_end_window():
    assert is_month_end_window(date(2026, 4, 30)) is True
    assert is_month_end_window(date(2026, 4, 26)) is True
    assert is_month_end_window(date(2026, 4, 1)) is False


# ── end_of_day_reconcile ───────────────────────────────────────────────────


def test_reconcile_handles_unavailable_broker_gracefully(tmp_path, monkeypatch):
    # Without a real IBKR connection the reconciler should return ok=True
    # with an "unavailable" error string rather than raising.
    monkeypatch.setattr(
        "src.runtime.end_of_day_reconcile.RECON_DIR", tmp_path
    )
    res = reconcile_end_of_day(profile="paper", as_of=date(2026, 4, 30))
    assert res.ok is True
    assert res.material_mismatch is False
    # diffs is either empty or a list (depending on whether the module
    # is importable in this env).
    assert isinstance(res.diffs, list)
