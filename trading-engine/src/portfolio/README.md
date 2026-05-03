# `src/portfolio/` — portfolio construction + risk constraints

Pre-trade portfolio shaping. Where weights become orders and where
**hard limits** prevent insane allocations from ever reaching IBKR.

## Layout

```
portfolio/
├── aggregators/             ← (scaffolded; see trading_engine/aggregators)
├── optimizers/              ← (scaffolded; see trading_engine/optimizers)
├── risk/
│   ├── exposure_limits.py   ← single-position cap, gross/net bounds, min holdings
│   └── derivatives_limits.py ← (scaffolded; for options/futures legs)
├── hedging_overlay.py       ← (scaffolded; for options/futures hedge logic)
├── target_builder.py        ← (scaffolded; weight → share-count translator)
└── sizing.py                ← (scaffolded; cash-buffer, lot-size logic)
```

## What's implemented today

**`risk/exposure_limits.py`** — `ExposureLimits` dataclass + `check_exposure_limits()`. Enforces:

- **Max single position** ≤ 25% of NAV (catches optimizer concentration bugs)
- **Gross exposure** ≤ 100% (no leverage) — set to 200% to allow 2x long-short
- **Net exposure** ∈ [50%, 100%] (forces net-long bias)
- **Min holdings** ≥ 5 (prevents accidental concentration)
- **Max single trade** ≤ 30% of NAV (forces big rebalances to be reviewed)
- **Min cash buffer** ≥ 0% (default; raise to keep liquidity for fees)

Each breach produces a structured `LimitBreach(name, detail, severity)`
so the operator can see exactly what fired and by how much.

```python
from src.portfolio.risk import check_exposure_limits, ExposureLimits

# Use the defaults
result = check_exposure_limits(weights, nav=1_000_000)
if not result.ok:
    for b in result.breaches:
        print(f"  ✗ {b.name}: {b.detail}")
    abort()

# Tweak limits
custom = ExposureLimits(max_single_position_pct=0.10, min_holdings=20)
check_exposure_limits(weights, nav=1_000_000, limits=custom)
```

`run_pre_trade_risk_checks()` in `src/runtime/risk_checks.py` wraps this
plus the kill switch and price validators into one gate that the daily
runner calls before submitting.

## What's planned

- **`hedging_overlay.py`** — futures + options overlay logic. Computes
  beta-adjusted equity exposure and decides when a hedge is needed.
- **`derivatives_limits.py`** — vega / gamma / delta exposure caps for
  the options book. Enforced alongside `exposure_limits` in the gate.
- **`sizing.py`** — translates target weights into share counts with
  cash-buffer reserve, round-lot constraints, and min-trade thresholds.
- **`target_builder.py`** — wraps `sizing` + `aggregators` for the
  end-to-end "weights → reconciliation file" step.

## Reference

- See [`src/runtime/`](../runtime/README.md) for how exposure_limits is
  wired into the pre-trade gate.
- See [`src/execution/kill_switch.py`](../execution/README.md) for the
  emergency-stop mechanism that complements limit checks.
