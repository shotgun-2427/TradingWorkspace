"""
target_builder.py — Combine model signals into a final target-weight book.

Pipeline:
  raw signals (per-model weights)
    → aggregate (sum / equal-weight / mean-variance — see aggregators/)
    → clip to per-name cap
    → renormalise to L1 budget
    → drop near-zero noise

The aggregation algorithm is selected by the orchestrator's config; this
module is the "after aggregation" stage that turns the aggregated signal
into the book the basket builder actually trades.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Mapping

DEFAULT_MAX_PER_NAME = 0.25         # 25% cap per asset
DEFAULT_L1_BUDGET = 1.0             # gross leverage = 1x
DEFAULT_NOISE_THRESHOLD = 0.005     # drop weights with |w| < 0.5%


@dataclass(frozen=True, slots=True)
class TargetBookConfig:
    max_per_name: float = DEFAULT_MAX_PER_NAME
    l1_budget: float = DEFAULT_L1_BUDGET
    noise_threshold: float = DEFAULT_NOISE_THRESHOLD

    def __post_init__(self) -> None:
        # Frozen dataclass — bypass setattr through object to validate.
        if not (math.isfinite(self.max_per_name) and self.max_per_name > 0):
            raise ValueError(f"max_per_name must be finite > 0, got {self.max_per_name!r}")
        if not (math.isfinite(self.l1_budget) and self.l1_budget > 0):
            raise ValueError(f"l1_budget must be finite > 0, got {self.l1_budget!r}")
        if not (math.isfinite(self.noise_threshold) and self.noise_threshold >= 0):
            raise ValueError(
                f"noise_threshold must be finite >= 0, got {self.noise_threshold!r}"
            )


def _sanitize(weights: Mapping[str, float]) -> Dict[str, float]:
    """Drop NaN / inf, drop non-numeric, drop empty/non-string keys."""
    out: Dict[str, float] = {}
    for s, w in weights.items():
        if not isinstance(s, str) or not s:
            continue
        if w is None:
            continue
        try:
            f = float(w)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            out[s] = f
    return out


def build_target_book(
    aggregated_weights: Mapping[str, float],
    *,
    config: TargetBookConfig | None = None,
) -> Dict[str, float]:
    """Apply per-name cap + L1 renormalisation + noise drop.

    Idempotent: ``build_target_book(build_target_book(w)) == build_target_book(w)``.
    """
    cfg = config or TargetBookConfig()
    w = _sanitize(aggregated_weights)
    if not w:
        return {}

    # 1. Per-name clip (preserve sign). copysign treats 0.0 sign as +,
    # so explicit guard for zero entries.
    def _clip(v: float) -> float:
        if v == 0.0:
            return 0.0
        magnitude = min(abs(v), cfg.max_per_name)
        return math.copysign(magnitude, v)

    clipped = {s: _clip(v) for s, v in w.items()}

    # 2. Renormalise to L1 budget if we'd otherwise exceed it.
    l1 = sum(abs(v) for v in clipped.values())
    if l1 > cfg.l1_budget and l1 > 0:
        scale = cfg.l1_budget / l1
        clipped = {s: v * scale for s, v in clipped.items()}

    # 3. Noise drop.
    pruned = {s: v for s, v in clipped.items() if abs(v) >= cfg.noise_threshold}

    return pruned


__all__ = [
    "TargetBookConfig",
    "DEFAULT_MAX_PER_NAME",
    "DEFAULT_L1_BUDGET",
    "DEFAULT_NOISE_THRESHOLD",
    "build_target_book",
]
