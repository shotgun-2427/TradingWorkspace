"""
exposure_limits.py — Pre-trade portfolio risk constraints.

This module enforces hard limits on what a basket is allowed to look like.
Run BEFORE submitting orders. Any breach blocks submission.

Limits checked:

  1. **Single-position weight cap** — no asset can exceed N% of NAV.
     Default 25%. Catches optimizer bugs that concentrate wildly.

  2. **Gross exposure cap** — sum of |weight| ≤ M% of NAV.
     Default 100% (fully invested, no leverage). Set to 200% to allow
     long-short up to 2x.

  3. **Net exposure floor / cap** — net_long - net_short stays in [floor, cap].
     Default [50%, 100%] — keeps net long.

  4. **Min number of holdings** — minimum K positions held with weight > floor.
     Default 5. Prevents accidental concentration.

  5. **Max single-trade $ size** — no single order can move more than N%
     of NAV in one go. Default 30%. Forces big rebalances to be reviewed.

  6. **Min cash buffer** — keep at least N% of NAV in cash post-rebalance.
     Default 0% (the optimizer can spend it all if it wants). Set higher to
     keep liquidity for fees / margin calls.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

import pandas as pd


@dataclass
class ExposureLimits:
    max_single_position_pct: float = 0.25
    max_gross_exposure_pct: float = 1.00
    min_net_exposure_pct: float = 0.50
    max_net_exposure_pct: float = 1.00
    min_holdings: int = 5
    max_single_trade_pct_of_nav: float = 0.30
    min_cash_buffer_pct: float = 0.00


DEFAULT_EXPOSURE_LIMITS = ExposureLimits()


@dataclass
class LimitBreach:
    name: str
    detail: str
    severity: str = "fail"   # "fail" or "warn"


@dataclass
class ExposureCheckResult:
    ok: bool
    breaches: list[LimitBreach] = field(default_factory=list)
    summary: dict[str, float] = field(default_factory=dict)

    def report(self) -> str:
        out = [f"Exposure check: {'PASS' if self.ok else 'FAIL'}"]
        for k, v in self.summary.items():
            out.append(f"  {k}: {v:.4f}")
        for b in self.breaches:
            sym = "✗" if b.severity == "fail" else "⚠"
            out.append(f"  {sym} {b.name}: {b.detail}")
        return "\n".join(out)


def check_exposure_limits(
    target_weights: dict[str, float] | pd.Series,
    *,
    nav: float,
    trades_dollars: pd.Series | None = None,
    cash_after_pct: float | None = None,
    limits: ExposureLimits | None = None,
) -> ExposureCheckResult:
    """Run all configured limit checks against a proposed basket.

    Parameters
    ----------
    target_weights : symbol → weight (signed; negative for shorts).
                     Weights are interpreted as fractions of NAV (0.30 = 30%).
    nav : portfolio net liquidation value.
    trades_dollars : optional pd.Series of |trade $| per symbol (for the
                     max-single-trade check). If None, that check is skipped.
    cash_after_pct : optional fraction of NAV that will remain in cash post-
                     rebalance. If None, the cash-buffer check is skipped.
    limits : ExposureLimits to use; defaults to DEFAULT_EXPOSURE_LIMITS.
    """
    limits = limits or DEFAULT_EXPOSURE_LIMITS
    res = ExposureCheckResult(ok=True)

    if not isinstance(target_weights, pd.Series):
        target_weights = pd.Series(dict(target_weights), dtype=float)
    target_weights = target_weights.dropna()

    if target_weights.empty:
        res.ok = False
        res.breaches.append(
            LimitBreach("empty_basket", "No target weights supplied", "fail")
        )
        return res

    gross = float(target_weights.abs().sum())
    net = float(target_weights.sum())
    longs = target_weights[target_weights > 0]
    shorts = target_weights[target_weights < 0]
    n_held = int((target_weights.abs() > 1e-6).sum())
    max_pos = float(target_weights.abs().max())
    max_pos_sym = target_weights.abs().idxmax() if not target_weights.empty else "—"

    res.summary.update(
        {
            "gross_exposure": gross,
            "net_exposure": net,
            "long_exposure": float(longs.sum()) if not longs.empty else 0.0,
            "short_exposure": float(shorts.sum()) if not shorts.empty else 0.0,
            "max_single_position": max_pos,
            "n_holdings": float(n_held),
        }
    )

    # 1. Max single position
    if max_pos > limits.max_single_position_pct + 1e-9:
        res.breaches.append(
            LimitBreach(
                "max_single_position",
                f"{max_pos_sym} = {max_pos*100:.1f}% > limit "
                f"{limits.max_single_position_pct*100:.1f}%",
            )
        )

    # 2. Gross exposure
    if gross > limits.max_gross_exposure_pct + 1e-6:
        res.breaches.append(
            LimitBreach(
                "max_gross_exposure",
                f"gross {gross*100:.1f}% > limit "
                f"{limits.max_gross_exposure_pct*100:.1f}%",
            )
        )

    # 3. Net exposure window
    if net < limits.min_net_exposure_pct - 1e-6:
        res.breaches.append(
            LimitBreach(
                "min_net_exposure",
                f"net {net*100:.1f}% < floor "
                f"{limits.min_net_exposure_pct*100:.1f}%",
            )
        )
    if net > limits.max_net_exposure_pct + 1e-6:
        res.breaches.append(
            LimitBreach(
                "max_net_exposure",
                f"net {net*100:.1f}% > cap "
                f"{limits.max_net_exposure_pct*100:.1f}%",
            )
        )

    # 4. Minimum holdings
    if n_held < limits.min_holdings:
        res.breaches.append(
            LimitBreach(
                "min_holdings",
                f"only {n_held} holdings (min {limits.min_holdings})",
            )
        )

    # 5. Max single-trade dollar size
    if trades_dollars is not None and not trades_dollars.empty and nav > 0:
        max_trade = float(trades_dollars.abs().max())
        max_trade_sym = trades_dollars.abs().idxmax()
        max_trade_pct = max_trade / nav
        res.summary["max_single_trade_pct"] = max_trade_pct
        if max_trade_pct > limits.max_single_trade_pct_of_nav + 1e-9:
            res.breaches.append(
                LimitBreach(
                    "max_single_trade",
                    f"{max_trade_sym} trade ${max_trade:,.0f} "
                    f"= {max_trade_pct*100:.1f}% of NAV > "
                    f"limit {limits.max_single_trade_pct_of_nav*100:.1f}%",
                    severity="warn",
                )
            )

    # 6. Cash buffer (post-rebalance)
    if cash_after_pct is not None:
        res.summary["cash_after_pct"] = cash_after_pct
        if cash_after_pct < limits.min_cash_buffer_pct - 1e-9:
            res.breaches.append(
                LimitBreach(
                    "min_cash_buffer",
                    f"cash {cash_after_pct*100:.2f}% < min "
                    f"{limits.min_cash_buffer_pct*100:.2f}%",
                )
            )

    res.ok = not any(b.severity == "fail" for b in res.breaches)
    return res
