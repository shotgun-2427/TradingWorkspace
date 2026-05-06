"""
hedging_overlay.py — Optional defensive overlay on top of the target book.

Brain-dead default: if portfolio gross exposure > ``threshold`` AND a
configured macro signal (e.g. VIX shift, news-engine sentiment) crosses
into "risk-off", buy a small TLT / SHY position scaled to a fraction of
gross.  This is a stub — the v0.1 implementation just exposes the shape
so callers can wire it in and a unit test can pin down the contract.

The fully-fledged version belongs after the news-engine integration
lands (see the integration plan in PROJECT_SNAPSHOT.docx); until then we
keep this neutral so the orchestrator can always call ``apply_overlay``
without an ``if overlay_enabled:`` branch downstream.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Mapping


import math


@dataclass(frozen=True, slots=True)
class HedgeOverlayConfig:
    enabled: bool = False
    hedge_symbol: str = "TLT-US"
    hedge_weight: float = 0.10            # 10% of gross
    trigger_signal_threshold: float = -0.5  # e.g. mean news sentiment < -0.5

    def __post_init__(self) -> None:
        if not isinstance(self.hedge_symbol, str) or not self.hedge_symbol:
            raise ValueError(f"hedge_symbol must be a non-empty string, got {self.hedge_symbol!r}")
        if not (math.isfinite(self.hedge_weight) and 0 < self.hedge_weight <= 5.0):
            raise ValueError(
                f"hedge_weight must be in (0, 5], got {self.hedge_weight!r}"
            )
        if not math.isfinite(self.trigger_signal_threshold):
            raise ValueError(
                f"trigger_signal_threshold must be finite, got {self.trigger_signal_threshold!r}"
            )


def apply_overlay(
    target_weights: Mapping[str, float],
    *,
    macro_signal: float | None = None,
    config: HedgeOverlayConfig | None = None,
) -> Dict[str, float]:
    """Return weights with hedge overlay applied if triggered.

    Always returns a fresh dict; the input mapping is not mutated.

    Default behaviour with ``config.enabled=False`` (the current default)
    is to return ``dict(target_weights)`` unchanged.
    """
    cfg = config or HedgeOverlayConfig()
    # Filter out non-finite / non-numeric weights from the input — caller
    # mistakes shouldn't propagate into the trade book.
    out: Dict[str, float] = {}
    for s, w in target_weights.items():
        if not isinstance(s, str) or not s:
            continue
        try:
            f = float(w)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            out[s] = f

    if not cfg.enabled:
        return out
    if macro_signal is None:
        return out
    try:
        sig = float(macro_signal)
    except (TypeError, ValueError):
        return out
    if not math.isfinite(sig):
        return out
    if sig >= cfg.trigger_signal_threshold:
        return out

    # Trigger met. Add or top-up the hedge position.
    out[cfg.hedge_symbol] = out.get(cfg.hedge_symbol, 0.0) + cfg.hedge_weight
    return out


__all__ = ["HedgeOverlayConfig", "apply_overlay"]
