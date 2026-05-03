# `src/trading_engine/risk/` — covariance / risk-model providers

Risk models supply the **covariance matrix** that mean-variance and
risk-parity optimizers consume. The contract is intentionally minimal so
new estimators slot in cleanly.

## Layout

```
risk/
├── catalogue/
│   ├── sample.py           ← textbook sample covariance + ridge variant
│   ├── naive_dcc.py        ← time-varying via separately EWMA'd vol & corr
│   └── stat_model.py       ← Ledoit-Wolf, constant-correlation shrinkage, EWMA
└── registry.py             ← name → callable mapping
```

## The contract

```python
def MyRiskModel(...) -> Callable[[np.ndarray], np.ndarray]:
    def run(window_returns: np.ndarray) -> np.ndarray:
        # window_returns: (T rows × N tickers)
        # returns: (N × N) covariance matrix
        return cov
    return run
```

That's it. The orchestrator in `core.py` slices the price panel into a
trailing window of returns, hands it off, and uses the result anywhere a
covariance matrix is needed.

## Estimator choice cheat-sheet

| Estimator | Best when | Caveat |
|---|---|---|
| `sample` | Lots of history (T ≫ N) | Singular when T ≤ N |
| `sample_with_ridge` | Need a quick PSD guarantee | Adds bias |
| `ewma_60`, `ewma_21` | Vol regime shifts (recent matters more) | Ignores correlation regime |
| `ledoit_wolf` | N close to T (the canonical small-sample fix) | Single shrinkage intensity |
| `constant_correlation_shrinkage` | You have a strong prior on avg correlation | You picked ρ̄ by hand |
| `naive_dcc` | Strong vol-cluster + correlation regime | Slowest of the bunch |

## Registry shape

```python
RISK_MODELS = {
    "sample": {"function": SampleCovariance(), "lookback": 0},
    "ledoit_wolf": {"function": LedoitWolf(ridge=1e-8), "lookback": 0},
    "ewma_21": {"function": ExponentiallyWeightedSample(half_life=21), "lookback": 0},
    ...
}
```

## Adding a risk model

1. Drop a file in `catalogue/`.
2. Make sure it's a factory returning `Callable[[np.ndarray],
   np.ndarray]` — same shape as the existing ones.
3. Always force-symmetrize and add a small ridge to the diagonal to
   guarantee positive semi-definiteness — the helpers in
   `stat_model._force_psd` do this.
4. Register in `registry.py`. Optimizers reference your new estimator
   by name from YAML.

## Why we don't use `pandas-ta` / `arch`

These libraries pull in heavy deps (cython, scipy with extras) that
aren't worth it for a one-window estimate. The estimators here are all
pure-numpy, ~20 lines each, and run in microseconds.
