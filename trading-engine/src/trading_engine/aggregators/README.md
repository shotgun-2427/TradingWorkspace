# `src/trading_engine/aggregators/` — combine model signals into a portfolio

Models produce per-ticker weight streams. Aggregators turn N model
outputs into ONE portfolio. Without aggregators, the runner would have
to pick a single model — they're how multi-model strategies become a
single allocation.

## Layout

```
aggregators/
├── catalogue/
│   ├── equal_weight.py        ← simple average across models
│   ├── manual_weight.py       ← user-supplied per-model weights
│   ├── min_avg_drawdown.py    ← weight inversely proportional to recent avg DD
│   └── mvo_aggregator.py      ← mean-variance optimization across models
└── registry.py                ← name → callable mapping
```

## The contract

```python
def MyAggregator(...) -> Callable[[Dict[str, LazyFrame], Dict[str, dict]], LazyFrame]:
    def run(model_insights, full_backtest_results):
        # model_insights: {model_name: LazyFrame of weights}
        # full_backtest_results: {model_name: backtest output dict}
        return combined_lazy_frame  # one LazyFrame, all tickers
    return run
```

The orchestrator handles universe padding, clamping, and L1 budget
afterwards — your aggregator just needs to produce a sane combined
LazyFrame.

## Picking an aggregator

| Aggregator | When to use it |
|---|---|
| `equal_weight` | You believe all models are roughly equally informative — most defensive choice. |
| `manual_weight` | You have prior beliefs about each model's quality and want to bake them in via YAML. |
| `min_avg_drawdown` | Weight models inversely to their recent drawdown — favors steady-Eddie models. |
| `mvo_aggregator` | Optimal in-sample weights based on each model's historical Sharpe and correlation. Most aggressive. |

## Registry shape

```python
AGGREGATORS = {
    "equal_weight": {"function": EqualWeight(), "lookback": 0},
    "min_avg_drawdown_60d": {
        "function": MinAvgDrawdown(window=60),
        "lookback": 60,  # need 60d of model history before it can score
    },
    ...
}
```

## Adding an aggregator

1. Implement in `catalogue/my_aggregator.py`.
2. Register a parameterization in `registry.py`.
3. Reference it in YAML configs by name.

## Reference

See [`README` at the trading_engine root](../README.md) for the full
pipeline diagram (model → aggregator → optimizer → simulation).
