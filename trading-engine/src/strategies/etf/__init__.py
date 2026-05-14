"""
ETF cross-sectional signals.

Importing this module gives you a ready-to-use signal registry:

    from src.strategies.etf import REGISTRY
    REGISTRY["momentum"]                       # MomentumSignal()
    REGISTRY["natr_mean_reversion"]            # NATRMeanReversion()
    ...

The registry is populated on first import.
"""
from __future__ import annotations

from typing import Any

REGISTRY: dict[str, Any] = {}


def _register(name: str, signal: Any) -> None:
    REGISTRY[name] = signal


# Import side-effects populate REGISTRY.
from src.strategies.etf.momentum import MomentumSignal  # noqa: E402
from src.strategies.etf.natr_mean_reversion import NATRMeanReversion  # noqa: E402
from src.strategies.etf.inverse_momentum_mean_reversion import (  # noqa: E402
    InverseMomentumMeanReversion,
)
from src.strategies.etf.amma import AMMATrend  # noqa: E402
from src.strategies.etf.buy_and_hold import BuyAndHold  # noqa: E402
from src.strategies.etf.trend_filter import TrendFilter  # noqa: E402
from src.strategies.etf.tsmom_ts import TSMomentum  # noqa: E402
from src.strategies.etf.vol_target_trend import VolTargetTrend  # noqa: E402
from src.strategies.etf.adaptive_trend import AdaptiveTrend  # noqa: E402

_register("momentum", MomentumSignal())
_register("natr_mean_reversion", NATRMeanReversion())
_register("inverse_momentum_mean_reversion", InverseMomentumMeanReversion())
_register("amma", AMMATrend())
_register("buy_and_hold", BuyAndHold())
_register("trend_filter", TrendFilter())
_register("tsmom_ts", TSMomentum())
_register("vol_target_trend", VolTargetTrend())
_register("adaptive_trend", AdaptiveTrend())


def list_signals() -> list[str]:
    return sorted(REGISTRY.keys())


__all__ = ["REGISTRY", "list_signals"]
