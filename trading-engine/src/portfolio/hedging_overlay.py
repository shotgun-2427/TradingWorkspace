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


@dataclass(frozen=True, slots=True)
class HedgeOverlayConfig:
    enabled: bool = False
    hedge_symbol: str = "TLT-US"
    hedge_weight: float = 0.10            # 10% of gross
    trigger_signal_threshold: float = -0.5  # e.g. mean news sentiment < -0.5


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
    out: Dict[str, float] = dict(target_weights)

    if not cfg.enabled:
        return out
    if macro_signal is None or macro_signal >= cfg.trigger_signal_threshold:
        return out

    # Trigger met. Add or top-up the hedge position.
    out[cfg.hedge_symbol] = out.get(cfg.hedge_symbol, 0.0) + cfg.hedge_weight
    return out


__all__ = ["HedgeOverlayConfig", "apply_overlay"]
