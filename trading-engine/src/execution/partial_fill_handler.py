"""
partial_fill_handler.py — Decide what to do when an order fills partially.

Brain-dead policy:
  * If filled_pct >= ``cancel_below_pct``, leave the rest working.
  * Otherwise, cancel the remainder and emit a follow-on order at a
    slightly more aggressive price (limit ± 1 tick).

Stateless. The caller passes in the order + fill state and we return
the recommended next action.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

DEFAULT_CANCEL_BELOW_PCT = 0.50      # cancel if < 50% filled
DEFAULT_TICK = 0.01                   # $0.01 default tick


class Action(str, Enum):
    LET_REST_WORK = "let_rest_work"
    CANCEL_AND_REPRICE = "cancel_and_reprice"
    CANCEL_ONLY = "cancel_only"


@dataclass(frozen=True, slots=True)
class FollowUp:
    action: Action
    new_price: float | None = None
    reason: str = ""


def decide_follow_up(
    *,
    side: str,                  # "buy" or "sell"
    qty_total: int,
    qty_filled: int,
    last_price: float,
    cancel_below_pct: float = DEFAULT_CANCEL_BELOW_PCT,
    tick: float = DEFAULT_TICK,
    repricing_enabled: bool = True,
) -> FollowUp:
    """Pure decision function. No I/O.

    Edge-case behaviour (frequently asked):
      * qty_total <= 0  -> CANCEL_ONLY (cannot reason about ratio).
      * qty_filled < 0  -> CANCEL_ONLY (broker bug; never reprice).
      * qty_filled > qty_total  -> LET_REST_WORK (over-fill; nothing to do).
      * non-finite price/tick -> CANCEL_ONLY (cannot compute new price).
      * unknown side string -> CANCEL_ONLY (no direction).
    """
    import math as _m

    if not isinstance(qty_total, int) or qty_total <= 0:
        return FollowUp(Action.CANCEL_ONLY, None, "qty_total non-positive")
    if not isinstance(qty_filled, int) or qty_filled < 0:
        return FollowUp(Action.CANCEL_ONLY, None, f"qty_filled invalid ({qty_filled!r})")
    if qty_filled > qty_total:
        return FollowUp(Action.LET_REST_WORK, None, "over-filled — nothing to follow up")
    if not (isinstance(last_price, (int, float)) and _m.isfinite(float(last_price)) and last_price > 0):
        return FollowUp(Action.CANCEL_ONLY, None, f"last_price invalid ({last_price!r})")
    if not (isinstance(tick, (int, float)) and _m.isfinite(float(tick)) and tick > 0):
        return FollowUp(Action.CANCEL_ONLY, None, f"tick invalid ({tick!r})")
    if not (isinstance(cancel_below_pct, (int, float))
            and _m.isfinite(float(cancel_below_pct))
            and 0.0 <= cancel_below_pct <= 1.0):
        return FollowUp(Action.CANCEL_ONLY, None, f"cancel_below_pct invalid ({cancel_below_pct!r})")

    pct = qty_filled / qty_total
    if pct >= cancel_below_pct:
        return FollowUp(Action.LET_REST_WORK, None, f"filled {pct:.0%} ≥ threshold")
    if not repricing_enabled:
        return FollowUp(Action.CANCEL_ONLY, None, f"filled {pct:.0%} < threshold; repricing off")

    side_norm = side.lower() if isinstance(side, str) else ""
    if side_norm not in ("buy", "sell"):
        return FollowUp(Action.CANCEL_ONLY, None, f"unknown side {side!r}")

    direction = 1.0 if side_norm == "buy" else -1.0
    new_price = round(last_price + direction * tick, 6)
    if not _m.isfinite(new_price) or new_price <= 0:
        return FollowUp(Action.CANCEL_ONLY, None, f"reprice produced invalid price {new_price}")
    return FollowUp(
        Action.CANCEL_AND_REPRICE,
        new_price,
        f"filled {pct:.0%} < threshold; repricing to {new_price}",
    )


__all__ = [
    "Action",
    "FollowUp",
    "decide_follow_up",
    "DEFAULT_CANCEL_BELOW_PCT",
    "DEFAULT_TICK",
]
