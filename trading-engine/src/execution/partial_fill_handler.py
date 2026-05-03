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
    """Pure decision function. No I/O."""
    if qty_total <= 0:
        return FollowUp(Action.CANCEL_ONLY, None, "qty_total non-positive")
    pct = qty_filled / qty_total
    if pct >= cancel_below_pct:
        return FollowUp(Action.LET_REST_WORK, None, f"filled {pct:.0%} ≥ threshold")
    if not repricing_enabled:
        return FollowUp(Action.CANCEL_ONLY, None, f"filled {pct:.0%} < threshold; repricing off")

    direction = 1.0 if side.lower() == "buy" else -1.0
    new_price = round(last_price + direction * tick, 6)
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
