"""
build_live_basket.py — Live (real-money) basket builder.

For now this is a thin wrapper that delegates to the paper-mode basket
builder in ``src.production.runtime.build_paper_basket`` while the live
side is still gated behind the kill switch and operator sign-off.

Differences from paper that will be added when this lights up:
  * stricter exposure limits in ``portfolio.risk.exposure_limits``
  * mandatory dry-run preview shown in CLI before any real submit
  * per-symbol execution algo selection (TWAP vs MKT vs LMT)
"""
from __future__ import annotations

from src.execution.kill_switch import KillSwitchTripped, require_kill_switch_clear


def build_live_basket(*args, **kwargs):
    """Defer to paper basket builder, but require the kill switch to be
    explicitly armed-then-disarmed by an operator since last live run.

    For v0 we simply re-raise to make the live path conspicuously
    unfinished.
    """
    require_kill_switch_clear()
    raise NotImplementedError(
        "Live basket builder not enabled in v0. Use build_paper_basket "
        "for now and run the live side only after the EOD reconcile job "
        "has run a cycle in the live profile."
    )


__all__ = ["build_live_basket"]
