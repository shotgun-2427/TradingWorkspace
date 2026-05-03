"""
order_policy.py — Pre-submit policy filter on individual orders.

Decides "is this single order allowed to leave our process?".  Sits one
step before ``order_router`` and one step after the basket builder.
Stateless and pure — perfect for unit tests.

Default policy:
  * Reject orders below ``min_notional_usd`` (noise filter).
  * Reject orders above ``max_notional_usd`` (catastrophic-fat-finger).
  * Reject orders for symbols not in an explicit allowlist (when one is set).
  * Reject orders with non-finite quantity / price.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable, Sequence

DEFAULT_MIN_NOTIONAL_USD = 50.0
DEFAULT_MAX_NOTIONAL_USD = 250_000.0


@dataclass(frozen=True, slots=True)
class OrderTicket:
    symbol: str
    qty: int           # signed; negative = sell / short
    price: float       # limit reference price
    side: str = "auto"  # "buy", "sell", or "auto" → derived from sign(qty)


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    accept: bool
    reason: str


@dataclass(frozen=True, slots=True)
class OrderPolicy:
    min_notional_usd: float = DEFAULT_MIN_NOTIONAL_USD
    max_notional_usd: float = DEFAULT_MAX_NOTIONAL_USD
    allowlist: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if not (math.isfinite(self.min_notional_usd) and self.min_notional_usd >= 0):
            raise ValueError(
                f"min_notional_usd must be finite >= 0, got {self.min_notional_usd!r}"
            )
        if not (math.isfinite(self.max_notional_usd) and self.max_notional_usd > 0):
            raise ValueError(
                f"max_notional_usd must be finite > 0, got {self.max_notional_usd!r}"
            )
        if self.min_notional_usd > self.max_notional_usd:
            raise ValueError(
                f"min_notional_usd ({self.min_notional_usd}) > "
                f"max_notional_usd ({self.max_notional_usd})"
            )
        # Allowlist must be a tuple of non-empty strings if provided.
        for s in self.allowlist:
            if not isinstance(s, str) or not s:
                raise ValueError(f"allowlist entries must be non-empty strings; got {s!r}")

    def evaluate(self, ticket: OrderTicket) -> PolicyDecision:
        if not isinstance(ticket.symbol, str) or not ticket.symbol:
            return PolicyDecision(False, "symbol missing")
        # Coerce qty/price to floats for finiteness check (qty is typed
        # int but defenders catch fuzzed inputs).
        try:
            qty_f = float(ticket.qty)
            price_f = float(ticket.price)
        except (TypeError, ValueError):
            return PolicyDecision(False, "qty/price not numeric")
        if not (math.isfinite(qty_f) and math.isfinite(price_f)):
            return PolicyDecision(False, "non-finite qty/price")
        if price_f <= 0:
            return PolicyDecision(False, f"non-positive price {ticket.price}")
        if qty_f == 0:
            return PolicyDecision(False, "zero quantity")
        # Side, if explicitly set, must agree with sign(qty).
        if ticket.side not in ("auto", "buy", "sell"):
            return PolicyDecision(False, f"unknown side {ticket.side!r}")
        if ticket.side == "buy" and qty_f < 0:
            return PolicyDecision(False, "side=buy with negative qty")
        if ticket.side == "sell" and qty_f > 0:
            return PolicyDecision(False, "side=sell with positive qty")
        notional = abs(ticket.qty) * ticket.price
        if notional < self.min_notional_usd:
            return PolicyDecision(
                False, f"notional ${notional:,.2f} below minimum ${self.min_notional_usd:,.2f}"
            )
        if notional > self.max_notional_usd:
            return PolicyDecision(
                False, f"notional ${notional:,.2f} above maximum ${self.max_notional_usd:,.2f}"
            )
        if self.allowlist and ticket.symbol not in self.allowlist:
            return PolicyDecision(False, f"symbol {ticket.symbol!r} not in allowlist")
        return PolicyDecision(True, "ok")

    def filter_batch(self, tickets: Iterable[OrderTicket]) -> tuple[list[OrderTicket], list[tuple[OrderTicket, str]]]:
        """Split a batch into (accepted, rejected_with_reason)."""
        accepted: list[OrderTicket] = []
        rejected: list[tuple[OrderTicket, str]] = []
        for t in tickets:
            d = self.evaluate(t)
            (accepted if d.accept else rejected).append(t if d.accept else (t, d.reason))  # type: ignore[arg-type]
        return accepted, rejected


DEFAULT_ORDER_POLICY = OrderPolicy()


__all__ = [
    "OrderTicket",
    "OrderPolicy",
    "PolicyDecision",
    "DEFAULT_ORDER_POLICY",
    "DEFAULT_MIN_NOTIONAL_USD",
    "DEFAULT_MAX_NOTIONAL_USD",
]
