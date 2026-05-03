"""
sizing.py — Convert weights → share / contract counts.

Brain-dead arithmetic on purpose. The clever stuff (lot rounding,
short-side margin, options multipliers) lives in ``portfolio.target_builder``;
this file is just ``shares = nav * weight / price``.

The reason it lives separately: it's the smallest, most-tested unit in
the chain, and we want to be able to stare at it for ten seconds and
believe it.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True, slots=True)
class SizedPosition:
    symbol: str
    weight: float
    target_dollars: float
    price: float
    shares: int  # signed; negative = short
    notional: float  # signed dollars


def size_position(
    symbol: str,
    weight: float,
    nav: float,
    price: float,
    *,
    contract_multiplier: int = 1,
    round_lots: int = 1,
) -> SizedPosition:
    """Size one position.

    Parameters
    ----------
    weight : target portfolio weight in {-1..+1} (or larger if leveraged).
    nav : current portfolio NAV in $.
    price : last price per share/contract.
    contract_multiplier : 100 for equity options, 1 for shares.
    round_lots : round share count to this lot size (1 = no rounding).

    Returns a ``SizedPosition``. Always finite. Returns shares=0 cleanly
    on bad inputs rather than raising — the caller is the right place to
    decide what to do with degenerate cases.
    """
    # Empty/invalid symbol → degenerate result; caller decides what to do.
    if not isinstance(symbol, str) or not symbol:
        return SizedPosition("", 0.0, 0.0, 0.0, 0, 0.0)

    if not (math.isfinite(weight) and math.isfinite(nav) and math.isfinite(price)):
        return SizedPosition(symbol, 0.0, 0.0, 0.0, 0, 0.0)
    if nav <= 0 or price <= 0 or contract_multiplier <= 0 or round_lots <= 0:
        return SizedPosition(symbol, 0.0, 0.0, 0.0, 0, 0.0)

    target_dollars = float(weight) * float(nav)
    denom = price * contract_multiplier
    if denom == 0 or not math.isfinite(denom):
        return SizedPosition(symbol, float(weight), 0.0, float(price), 0, 0.0)

    raw_shares = target_dollars / denom
    if not math.isfinite(raw_shares):
        # Overflow (e.g. nav=1e308, price=1e-308). Refuse to size.
        return SizedPosition(symbol, float(weight), 0.0, float(price), 0, 0.0)

    try:
        rounded = int(round(raw_shares / round_lots)) * round_lots
    except (OverflowError, ValueError):
        return SizedPosition(symbol, float(weight), 0.0, float(price), 0, 0.0)
    notional = rounded * price * contract_multiplier
    if not math.isfinite(notional):
        return SizedPosition(symbol, float(weight), 0.0, float(price), 0, 0.0)
    return SizedPosition(symbol, float(weight), target_dollars, float(price), rounded, notional)


def size_basket(
    weights: Mapping[str, float],
    prices: Mapping[str, float],
    nav: float,
    *,
    round_lots: int = 1,
) -> list[SizedPosition]:
    """Size every position in a target-weights mapping."""
    out: list[SizedPosition] = []
    for sym, w in weights.items():
        px = prices.get(sym)
        if px is None or not math.isfinite(px) or px <= 0:
            # Skip rather than fail the basket; risk_checks will flag.
            continue
        out.append(size_position(sym, w, nav=nav, price=px, round_lots=round_lots))
    return out


def total_gross_exposure(sized: Iterable[SizedPosition]) -> float:
    return sum(abs(p.notional) for p in sized)


def total_net_exposure(sized: Iterable[SizedPosition]) -> float:
    return sum(p.notional for p in sized)


__all__ = [
    "SizedPosition",
    "size_position",
    "size_basket",
    "total_gross_exposure",
    "total_net_exposure",
]
