"""
submit_live_orders.py — Live-mode order submission entrypoint.

Intentionally not implemented in v0. The system trades paper-only until:

  1. The EOD reconciler has run a full cycle without flagging a material
     mismatch.
  2. Live exposure limits are tightened in
     ``portfolio.risk.exposure_limits`` (smaller per-name cap, gross cap).
  3. A two-step confirm is added (dry-run preview + explicit "go" flag).

Once those are in place this module will route through
``execution.order_router.submit_orders`` exactly like paper, with a
``profile="live"`` parameter.
"""
from __future__ import annotations


def submit_live_orders(*args, **kwargs):
    raise NotImplementedError(
        "Live order submission disabled in v0 — see module docstring "
        "for the gating list."
    )


__all__ = ["submit_live_orders"]
