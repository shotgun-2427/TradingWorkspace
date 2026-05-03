"""
Signal ensemble combiner.

Each component signal returns z-scored cross-sectional scores. The ensemble
produces a weighted average:

    composite_score(asset) = Σ_i w_i × signal_i.score(asset)

`w_i` are user-supplied non-negative weights that sum to 1. If a signal
returns NaN for an asset (e.g. insufficient history), that signal's weight
is redistributed across the remaining signals for that asset only — so a
short-history asset still gets a fair score from whatever signals can rate
it.

The default ensemble (`default_ensemble()`) holds all four ETF signals at
equal weight, which is a reasonable starting point for diversification
across factor types.
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class WeightedSignal:
    signal: object        # any class implementing the Signal protocol
    weight: float


class SignalEnsemble:
    name = "ensemble"

    def __init__(self, components: list[WeightedSignal]):
        if not components:
            raise ValueError("Ensemble must have at least one component")
        total = sum(c.weight for c in components)
        if total <= 0:
            raise ValueError("Sum of ensemble weights must be > 0")
        # Normalize weights to sum to 1.
        self.components = [
            WeightedSignal(c.signal, c.weight / total) for c in components
        ]
        self.lookback_days_required = max(
            int(getattr(c.signal, "lookback_days_required", 0))
            for c in self.components
        )

    def compute(
        self,
        prices: pd.DataFrame,
        as_of: pd.Timestamp,
    ) -> pd.DataFrame:
        per_signal: list[pd.DataFrame] = []
        for comp in self.components:
            sig_name = getattr(comp.signal, "name", "?")
            try:
                df = comp.signal.compute(prices, as_of)
            except Exception as exc:
                # A broken signal should NOT take down the whole ensemble.
                # Log via stderr-ish (printing keeps this dependency-free).
                df = pd.DataFrame(columns=["symbol", "raw", "score"])
                print(f"[ensemble] signal '{sig_name}' raised {type(exc).__name__}: {exc}")

            if df is None or df.empty:
                continue
            df = df[["symbol", "score"]].copy()
            df["weight"] = comp.weight
            df["signal"] = sig_name
            per_signal.append(df)

        if not per_signal:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        long = pd.concat(per_signal, ignore_index=True)

        # Weighted mean per symbol, with NaN-aware redistribution: each
        # symbol's composite is Σ(w_i × score_i) / Σ(w_i where score_i is
        # non-NaN). That way an asset with only 2 of 4 signals available
        # still gets a clean composite.
        long = long.dropna(subset=["score"])
        if long.empty:
            return pd.DataFrame(columns=["symbol", "raw", "score"])

        grouped = long.groupby("symbol")
        composite = grouped.apply(
            lambda g: (g["score"] * g["weight"]).sum() / g["weight"].sum()
        )
        composite.name = "score"
        out = composite.reset_index().rename(columns={"score": "raw"})
        # Re-z-score the composite so its values stay comparable across runs.
        from src.strategies import zscore
        out["score"] = zscore(out["raw"])
        return out


def default_ensemble() -> SignalEnsemble:
    """All-four-signals equal-weight ensemble. Good starting point."""
    from src.strategies.etf.amma import AMMATrend
    from src.strategies.etf.inverse_momentum_mean_reversion import (
        InverseMomentumMeanReversion,
    )
    from src.strategies.etf.momentum import MomentumSignal
    from src.strategies.etf.natr_mean_reversion import NATRMeanReversion

    return SignalEnsemble(
        [
            WeightedSignal(MomentumSignal(), 1.0),
            WeightedSignal(NATRMeanReversion(), 1.0),
            WeightedSignal(InverseMomentumMeanReversion(), 1.0),
            WeightedSignal(AMMATrend(), 1.0),
        ]
    )


def momentum_heavy_ensemble() -> SignalEnsemble:
    """Tilted toward momentum + AMMA trend (more aggressive)."""
    from src.strategies.etf.amma import AMMATrend
    from src.strategies.etf.inverse_momentum_mean_reversion import (
        InverseMomentumMeanReversion,
    )
    from src.strategies.etf.momentum import MomentumSignal
    from src.strategies.etf.natr_mean_reversion import NATRMeanReversion

    return SignalEnsemble(
        [
            WeightedSignal(MomentumSignal(), 2.0),
            WeightedSignal(AMMATrend(), 2.0),
            WeightedSignal(InverseMomentumMeanReversion(), 1.0),
            WeightedSignal(NATRMeanReversion(), 1.0),
        ]
    )


def reversion_heavy_ensemble() -> SignalEnsemble:
    """Tilted toward mean reversion (more contrarian)."""
    from src.strategies.etf.amma import AMMATrend
    from src.strategies.etf.inverse_momentum_mean_reversion import (
        InverseMomentumMeanReversion,
    )
    from src.strategies.etf.momentum import MomentumSignal
    from src.strategies.etf.natr_mean_reversion import NATRMeanReversion

    return SignalEnsemble(
        [
            WeightedSignal(MomentumSignal(), 1.0),
            WeightedSignal(AMMATrend(), 1.0),
            WeightedSignal(InverseMomentumMeanReversion(), 2.0),
            WeightedSignal(NATRMeanReversion(), 2.0),
        ]
    )
