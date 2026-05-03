# `src/trading_engine/model_state/` — feature engineering registry

The features (momentum-N, NATR-N, RSI-N, …) that models read from the
"model state" — a per-(ticker, date) feature panel computed once at the
start of a pipeline run.

## Layout

```
model_state/
├── catalogue/
│   └── features.py     ← moving_average, momentum, rsi, natr factories
└── registry.py         ← name → {func, mode, lookback} mapping
```

## The feature contract

A feature factory takes an output column name + window + source column
and returns a transform:

```python
def momentum(source_col: str, dest_col: str, window: int) -> Callable[[LazyFrame], LazyFrame]:
    def transform(df: LazyFrame) -> LazyFrame:
        return df.with_columns(
            (pl.col(source_col) - pl.col(source_col).shift(window))
            .over("ticker")
            .alias(dest_col)
        )
    return transform
```

The transform takes a Polars LazyFrame (or DataFrame for eager features)
and returns one with the new column added. The orchestrator in
[`core.py`](../../trading_engine/core.py) chains these transforms in
dependency order during `create_model_state`.

## Registry shape

```python
FEATURES = {
    "close_momentum_60": {
        "func": momentum("adjusted_close_1d", "close_momentum_60", window=60),
        "mode": ProcessingMode.LAZY,
        "lookback": 60,             # rows of history needed before this is meaningful
    },
    ...
}
```

`mode` is `LAZY` for cheap per-row transforms (windowed numerics) and
`EAGER` for transforms that need the full collected DataFrame (RSI's
state-machine, complex group-by joins). The orchestrator collects once
between the lazy and eager passes.

## Currently registered features

```
moving_average     close_ma_10
momentum           close_momentum_{1, 5, 10, 14, 20, 30, 32, 60, 64, 90, 120, 240}
rsi                close_rsi_14
natr               natr_7, natr_14
```

## Adding a new feature

1. Add a factory in `catalogue/features.py` if you want a new class of
   feature. Otherwise reuse an existing factory with new params.
2. Register a parameterization in `registry.py` with the right `mode`
   and `lookback`. **Set `lookback` correctly** — the orchestrator
   uses it to pull warmup rows from before `start_date`.
3. Reference the feature name from any model registry entry's
   `columns` list.

## Why this matters

The orchestrator calculates a single max-lookback across every
configured feature, model, aggregator, and optimizer — and that drives
how much warmup data gets pulled before the actual run window. If you
register a feature with `lookback=240` and forget to also need ≥ 240
warmup days, the first 240 rows of your output will have NaN scores
and the daily runner will silently produce garbage allocations. Set
`lookback` honestly.
