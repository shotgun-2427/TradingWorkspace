# `src/portfolio/risk/` — pre-trade portfolio risk

Hard limits on basket shape that have to pass before any order goes to
IBKR. Two files:

| File | What it does |
|---|---|
| `exposure_limits.py` | Position concentration, gross / net exposure, holdings count, single-trade size, cash buffer |
| `derivatives_limits.py` | (scaffolded) options / futures greeks limits — vega, gamma, delta, contract count |

## How a check produces a structured result

```python
from src.portfolio.risk import check_exposure_limits, ExposureLimits

weights = pd.Series({"QQQ": 0.30, "SPY": 0.30, "BIL": 0.20, "GLD": 0.10, "SLV": 0.10})
result = check_exposure_limits(weights, nav=1_000_000)
print(result.report())
# Exposure check: FAIL
#   gross_exposure: 1.0000
#   net_exposure: 1.0000
#   max_single_position: 0.3000
#   ✗ max_single_position: QQQ = 30.0% > limit 25.0%
```

`result.ok` is False → the runner aborts before connecting to IBKR.

## Default limits

```python
ExposureLimits(
    max_single_position_pct=0.25,
    max_gross_exposure_pct=1.00,
    min_net_exposure_pct=0.50,
    max_net_exposure_pct=1.00,
    min_holdings=5,
    max_single_trade_pct_of_nav=0.30,
    min_cash_buffer_pct=0.00,
)
```

These are intentionally conservative for a paper-trading default.
Override via the `limits=` kwarg if you have a stricter mandate or
want to relax a bound for research.

## Adding a new check

If you want to enforce something the dataclass doesn't capture (e.g.
sector concentration, beta neutrality, ADV-relative trade size), add a
field to `ExposureLimits` and a corresponding block in
`check_exposure_limits()`. Keep the structured-breach pattern so the
operator gets a clear message.
