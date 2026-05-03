"""
schedule.py — Static schedule definitions used by the daily scheduler.

This module owns the *what runs when* table.  The actual scheduler lives
in ``src.production.scheduler`` (APScheduler / launchd plist on macOS).
Putting the table in its own file means humans can read the schedule
without grokking APScheduler.

All times are US/Eastern. Cron expressions follow the standard 5-field
format (minute hour day month dow).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ScheduledJob:
    name: str
    cron: str
    entrypoint: str
    description: str


# Times chosen relative to NYSE 09:30–16:00 ET.
DAILY_SCHEDULE: list[ScheduledJob] = [
    ScheduledJob(
        name="pre_market_healthcheck",
        cron="0 8 * * 1-5",  # 08:00 ET, weekdays
        entrypoint="src.runtime.healthcheck:main",
        description="Verify IBKR gateway up, kill switch state, recent prices.",
    ),
    ScheduledJob(
        name="daily_run",
        cron="30 16 * * 1-5",  # 16:30 ET, weekdays
        entrypoint="src.runtime.daily_run:main",
        description="Append bars → regenerate targets → maybe rebalance → submit paper orders.",
    ),
    ScheduledJob(
        name="end_of_day_reconcile",
        cron="0 17 * * 1-5",  # 17:00 ET, weekdays
        entrypoint="src.runtime.end_of_day_reconcile:reconcile_end_of_day",
        description="Reconcile account vs internal book; arm kill switch on mismatch.",
    ),
]


def schedule_table() -> list[dict]:
    """Return the schedule as plain dicts (for dashboards / docs)."""
    return [
        {
            "name": j.name,
            "cron": j.cron,
            "entrypoint": j.entrypoint,
            "description": j.description,
        }
        for j in DAILY_SCHEDULE
    ]


__all__ = ["DAILY_SCHEDULE", "ScheduledJob", "schedule_table"]
