"""
auto_rebalance.py — Decide if today is a rebalance day.

A small, intentionally pure module: takes the latest targets DataFrame
and a date, returns ``True`` / ``False`` plus a reason string.  No I/O.
The caller is responsible for loading the targets and acting on the
decision.

The rules below mirror what ``production/daily_runner._needs_rebalance``
does. Lifted into a separate module here so:

  * Tests can hammer the decision function with edge cases without
    monkeypatching daily_runner.
  * The dashboard can call it to render "next rebalance in N days".
  * A future cron / lambda layer can import it without pulling the IBKR
    bits in daily_runner.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True, slots=True)
class RebalanceDecision:
    rebalance: bool
    reason: str
    latest_signal_date: Optional[date]


def _last_calendar_day(d: date) -> int:
    return calendar.monthrange(d.year, d.month)[1]


def is_month_end_window(d: date, *, days: int = 5) -> bool:
    """True if ``d`` is in the last ``days`` calendar days of its month."""
    if not isinstance(d, date):
        raise TypeError(f"d must be a datetime.date, got {type(d).__name__}")
    if not isinstance(days, int) or days < 1:
        raise ValueError(f"days must be a positive int, got {days!r}")
    if days >= 31:
        # Any day of any month is in the window.
        return True
    return d.day >= _last_calendar_day(d) - (days - 1)


def decide_rebalance(
    latest_signal_date: Optional[date],
    today: date,
    *,
    force: bool = False,
) -> RebalanceDecision:
    """Decide whether to rebalance.

    Rules (any one triggers rebalance):
      * ``force=True``
      * No signal date at all (cold start)
      * Signal date is in a strictly earlier month (missed cycle)
      * Signal date is in the future (clock skew or replay)

    Otherwise we skip — the position should already be on target.
    """
    if not isinstance(today, date):
        raise TypeError(f"today must be a datetime.date, got {type(today).__name__}")
    if latest_signal_date is not None and not isinstance(latest_signal_date, date):
        raise TypeError(
            f"latest_signal_date must be date or None, got {type(latest_signal_date).__name__}"
        )

    if force:
        return RebalanceDecision(True, "force flag set", latest_signal_date)
    if latest_signal_date is None:
        return RebalanceDecision(True, "no prior signal — cold start", None)
    if latest_signal_date > today:
        # Future date is a sign of clock skew / replay attack / corrupted
        # data. Refuse to skip rebalance silently — surface the anomaly.
        return RebalanceDecision(
            True,
            f"signal {latest_signal_date} is AFTER today {today} — clock anomaly, "
            f"forcing rebalance to refresh",
            latest_signal_date,
        )
    if (latest_signal_date.year, latest_signal_date.month) < (today.year, today.month):
        return RebalanceDecision(
            True,
            f"signal {latest_signal_date} is before current month "
            f"{today.year}-{today.month:02d}",
            latest_signal_date,
        )
    return RebalanceDecision(
        False,
        f"signal {latest_signal_date} already current — skip",
        latest_signal_date,
    )


__all__ = [
    "RebalanceDecision",
    "decide_rebalance",
    "is_month_end_window",
]
