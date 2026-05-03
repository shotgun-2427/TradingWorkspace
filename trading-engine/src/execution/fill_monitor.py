"""
fill_monitor.py — Watch open orders for status changes.

Tiny polling helper. Used by the dashboard's Trades tab and by the EOD
reconciler. Brain-dead by design: pull executions, diff against the last
snapshot, return the new ones.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

log = logging.getLogger(__name__)


@dataclass(slots=True)
class Fill:
    order_id: str
    symbol: str
    qty: int
    price: float
    ts: str  # iso8601


def list_recent_fills(*, profile: str = "paper", since: datetime | None = None) -> list[Fill]:
    """Return executions since ``since``. Pulls from ``broker.ibkr.executions``.

    Returns ``[]`` if the broker module is unavailable (unit test env).
    """
    try:
        from src.broker.ibkr import executions as ibkr_execs  # type: ignore
    except ImportError as exc:
        log.warning("fill_monitor: executions module unavailable: %s", exc)
        return []

    fn = getattr(ibkr_execs, "list_executions", None)
    if fn is None:
        return []

    raw: Iterable[dict[str, Any]] = fn(profile=profile, since=since)  # type: ignore[misc]
    return [
        Fill(
            order_id=str(r.get("order_id", "")),
            symbol=str(r.get("symbol", "")),
            qty=int(r.get("qty", 0)),
            price=float(r.get("price", 0.0)),
            ts=str(r.get("ts", datetime.utcnow().isoformat() + "Z")),
        )
        for r in raw
    ]


__all__ = ["Fill", "list_recent_fills"]
