# `src/strategies/futures/` — futures signals (scaffolded)

Placeholder folder for the futures-leg signals. The `Signal` protocol
from [`src/strategies/__init__.py`](../__init__.py) carries over —
implement these the same way as the ETF signals.

## Planned signals

| File | What it'll do |
|---|---|
| `carry.py` | Roll-yield / cash-and-carry signal — long contracts in backwardation, short ones in contango. |
| `trend_following.py` | Classic CTA-style breakout / Donchian channel signal across multiple lookbacks. |
| `hedge_trigger.py` | Conditional signal that only fires when the equity overlay needs a hedge (e.g. SPX drawdown > X%). |

## Why this is empty

Futures need an additional data feed (continuous front-month + roll
schedules) which the local-first workspace doesn't have yet. The
moment that's wired up via `src/data/ingest/append_futures_daily.py`,
these signals can land here.

When you implement these, copy the structure from `src/strategies/etf/`:
- One file per factor type
- Each exports a class implementing `Signal`
- A small `_panel.py` for shared data-shape helpers
- A registry in `__init__.py`

## See also

- [Roadmap Phase 2](../../README.md#going-beyond-etfs) — where this fits in.
- [`src/strategies/etf/`](../etf/README.md) — reference implementation pattern to follow.
