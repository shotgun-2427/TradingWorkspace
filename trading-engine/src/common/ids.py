"""
ids.py — Deterministic ID generation.

Two requirements drive the design:

  1. **Reproducible** — given the same inputs (basket fingerprint, date,
     symbol), we get the same ID. That makes idempotency checks trivial:
     "did I already submit this exact order?" → recompute the ID, look up,
     done.

  2. **Sortable** — IDs lead with a timestamp prefix so a directory of
     order_ref-named files lists in chronological order. Useful for forensic
     replay.

ID shape (everything human-readable, no opaque UUIDs):

    run     :   r-YYYYMMDD-HHMMSS-<6-hex>
    basket  :   b-YYYYMMDD-<6-hex>           (one per rebalance day)
    order   :   o-YYYYMMDD-HHMMSS-<symbol>-<6-hex>
"""
from __future__ import annotations

import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

NYSE_TZ = ZoneInfo("America/New_York")


def deterministic_id(*parts: object, length: int = 6) -> str:
    """Stable hash of the input parts. Output is `length` lowercase hex chars."""
    payload = "|".join(str(p) for p in parts)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest[:length]


def new_run_id(at: datetime | None = None) -> str:
    """Run identifier — uses wall-clock millisecond resolution to dedupe
    runs that fire close together."""
    now = (at or datetime.now(NYSE_TZ)).astimezone(NYSE_TZ)
    suffix = deterministic_id(now.timestamp())
    return f"r-{now.strftime('%Y%m%d-%H%M%S')}-{suffix}"


def new_basket_id(rebalance_date: object, fingerprint: str) -> str:
    """One basket per rebalance_date + fingerprint. Re-running the same
    rebalance produces the same basket id (idempotent)."""
    suffix = deterministic_id(rebalance_date, fingerprint)
    if isinstance(rebalance_date, datetime):
        date_str = rebalance_date.strftime("%Y%m%d")
    else:
        # Accept date-like or string.
        s = str(rebalance_date)[:10].replace("-", "")
        date_str = s if len(s) == 8 and s.isdigit() else "00000000"
    return f"b-{date_str}-{suffix}"


def new_order_ref(
    symbol: str,
    side: str,
    quantity: float,
    basket_id: str,
    *,
    at: datetime | None = None,
) -> str:
    """One order ref per (symbol, side, qty, basket_id). Re-submitting the
    same order from the same basket gets the same ref — IBKR will reject
    duplicate refs, which is exactly the behavior we want."""
    now = (at or datetime.now(NYSE_TZ)).astimezone(NYSE_TZ)
    suffix = deterministic_id(symbol, side, quantity, basket_id)
    sym = str(symbol).upper().strip()
    return f"o-{now.strftime('%Y%m%d-%H%M%S')}-{sym}-{suffix}"
