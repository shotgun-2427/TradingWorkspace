# `src/strategies/options/` — options signals (scaffolded)

Placeholder folder for the options-overlay signals. The `Signal`
protocol from [`src/strategies/__init__.py`](../__init__.py) carries
over — same shape as the ETF signals, just different inputs (greeks,
implied vol surface, term structure).

## Planned signals

| File | What it'll do |
|---|---|
| `overlay_signal.py` | Decide when to put on a put-spread / call-overwrite overlay on the equity book. |
| `vol_regime.py` | Classify VIX / IV-rank regimes (low / mid / high / extreme) and gate other strategies. |
| `hedge_trigger.py` | Conditional buy-protection signal — fires when realized vs. implied vol diverges. |

## Why this is empty

Options need an EOD options chain feed (vendors: ORATS, OptionMetrics,
or scrape via IBKR's reqOptionChain). Until that's wired up, signals
here would have nothing to score.

When you implement these:
- Same `Signal` protocol contract as ETFs
- One file per factor type
- Helper `_chain.py` for the chain → factor exposure transformation
- Registry in `__init__.py`

## See also

- [Roadmap Phase 2](../../README.md#going-beyond-etfs)
- [`src/strategies/etf/README.md`](../etf/README.md) — reference pattern.
