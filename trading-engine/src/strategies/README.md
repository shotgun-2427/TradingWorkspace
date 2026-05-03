# `src/strategies/` — lightweight signal protocol

This is the **prototype-friendly** signal architecture. If you want to
sketch a new factor without dealing with the full Polars
registry-driven trading_engine machinery, this is the place.

For production work that actually drives the daily runner, see
[`src/trading_engine/`](../trading_engine/) — that's the heavier-weight,
registry-driven path.

## The contract

```python
class Signal(Protocol):
    name: str
    lookback_days_required: int

    def compute(
        self,
        prices: pd.DataFrame,
        as_of: pd.Timestamp,
    ) -> pd.DataFrame:
        """Return columns: symbol, raw, score
        score is cross-sectionally z-scored so signals on different scales
        (returns vs. NATR vs. RSI) compose cleanly in an ensemble.
        """
```

That's it. Anything obeying this protocol can be mixed and matched in
`etf/ensemble.py`.

## Asset-class trees

| Tree | What's there | Status |
|---|---|---|
| [`etf/`](etf/) | 4 signals + ensemble: momentum, NATR mean reversion, inverse-momentum × MR, AMMA trend | **populated** |
| [`futures/`](futures/) | carry, trend_following, hedge_trigger | scaffolded |
| [`options/`](options/) | overlay_signal, vol_regime, hedge_trigger | scaffolded |

## Adding a signal

1. Create `etf/my_signal.py` with a class that implements the `Signal`
   protocol.
2. Use the helpers in `etf/_panel.py` (to_wide, slice_history,
   eligible_columns) so you don't reinvent date alignment.
3. Add it to `etf/__init__.py`'s `REGISTRY`.
4. Reference it from `etf/ensemble.py` if you want it in the default
   ensemble.

## When to use this vs `trading_engine/`

| Use this when… | Use `trading_engine/` when… |
|---|---|
| Sketching a new factor | Wiring it into the daily runner |
| Quick research notebook | YAML-driven production config |
| Pandas, simple loops | Polars, lazy execution, multi-stage pipeline |
| One-off backtest | Aggregator + optimizer + risk model |

The two systems are deliberately parallel. A signal you sketch here can
be ported to `trading_engine/models/catalogue/` once it earns its keep.
