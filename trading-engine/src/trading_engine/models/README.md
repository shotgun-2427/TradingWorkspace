# `src/trading_engine/models/` — production model factories + registry

This is where models that drive the **daily runner** live. Heavier and
more rigid than the prototype-friendly `src/strategies/etf/`, but plugs
straight into the orchestrator in `src/trading_engine/core.py`.

## Layout

```
models/
├── catalogue/              ← model factory implementations
│   ├── momentum.py
│   ├── inverse_momentum_mean_reversion.py
│   ├── natr_mean_reversion.py
│   ├── amma.py
│   ├── dual_ma_crossover.py
│   ├── macd.py
│   ├── rsi_mean_reversion.py
│   ├── bollinger_band.py
│   ├── donchian_breakout.py
│   ├── relative_strength.py
│   ├── low_volatility.py
│   ├── quality_sharpe.py
│   ├── trend_strength.py
│   └── volatility_target.py
└── registry.py             ← name → factory invocation mapping
```

## The model contract

A **factory function** takes parameters and returns a runner:

```python
def MomentumModel(
    trade_ticker: str,
    signal_ticker: str,
    momentum_column: str,
    inverse: bool = True,
    threshold: float = 0.0,
) -> Callable[[LazyFrame], LazyFrame]:
    def run_model(lf: LazyFrame) -> LazyFrame:
        # … Polars expressions …
        return weights_lazy_frame  # ["date", trade_ticker]
    return run_model
```

Output **must** be a Polars LazyFrame with columns `["date",
trade_ticker]`. The orchestrator handles padding to the full universe,
clamping to [-1, 1], and L1 budget enforcement — your model just needs
to produce a clean weight stream for the asset(s) it cares about.

## Registry shape

```python
MODELS = {
    "name_for_yaml_to_reference": {
        "tickers": ["SPY-US"],                    # tickers the runner reads
        "columns": ["close_momentum_60"],         # feature columns it needs
        "function": MomentumModel(trade_ticker="SPY-US", ...),
        "lookback": 0,                            # extra warmup rows beyond features
        # optional:
        "input_mode": "bundle",   # set if your runner takes ModelStateBundle
                                   # instead of a slim filtered LazyFrame
    },
    ...
}
```

## Adding a new model

1. **Pick a factor type.** Look at the catalogue first — there might
   already be one (e.g. RSI mean-reversion exists; you don't need a
   second one unless your variant is materially different).

2. **Write a factory** in `catalogue/my_new_model.py`. Follow the
   single-asset weight stream contract above. Keep all
   parameterizable values as factory args, not magic numbers in the
   body — that way the registry can spawn many parameterizations.

3. **Re-export it** from `catalogue/__init__.py`.

4. **Register parameterizations** in `registry.py`:

   ```python
   "SPY_my_signal_60": {
       "tickers": ["SPY-US"],
       "columns": ["close_momentum_60", "natr_14"],
       "function": MyNewModel(
           trade_ticker="SPY-US",
           lookback=60,
           threshold=0.05,
       ),
       "lookback": 0,
   },
   ```

5. **Add the YAML reference** to your production config at
   `src/production/pipeline/configs/paper.yaml` so the daily runner
   picks it up.

6. **Smoke-test:**

   ```bash
   python3 -c "
   from trading_engine.models import MODELS
   spec = MODELS['SPY_my_signal_60']
   print(spec)
   "
   ```

## How models become a portfolio

```
each model → 1 weight stream (one ticker)
     │
     ▼
aggregator → 1 portfolio (all tickers in universe)
     │
     ▼
optimizer → risk-aware refinement (optional)
     │
     ▼
basket build + risk gate + IBKR submit
```

So you can register 20 single-ticker models and let the aggregator
combine them into a portfolio. Or register multi-ticker models that
already produce a portfolio and skip the aggregator. Both work.

## Catalogue overview (14 factory types)

| Family | Files |
|---|---|
| **Trend** | `momentum`, `dual_ma_crossover`, `donchian_breakout`, `amma` |
| **Momentum-reversion** | `inverse_momentum_mean_reversion`, `macd`, `rsi_mean_reversion` |
| **Volatility-driven** | `natr_mean_reversion`, `bollinger_band`, `low_volatility`, `volatility_target` |
| **Cross-asset** | `relative_strength` |
| **Quality / risk-adjusted** | `quality_sharpe`, `trend_strength` |

Each factory can be parameterized into many concrete models. The current
registry has 28 concrete entries; nothing stops you from registering 200.
