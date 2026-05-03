# `src/common/` — shared utilities

Building blocks every other subsystem can depend on. Keep this folder
**dependency-free** apart from numpy / pandas / polars — anything heavier
(streamlit, ib_async) breaks the rest of the codebase when the optional
dep is missing.

## Modules

| File | What it does |
|---|---|
| `clock.py` | NYSE-aware time. `now_et`, `is_market_open`, `next_market_open`, `is_us_business_day` — uses an embedded NYSE holiday calendar (no `pandas-market-calendars` dep). |
| `ids.py` | Deterministic ID generation. `new_run_id`, `new_basket_id`, `new_order_ref` — same inputs always give the same hash, so retrying a basket is idempotent. |
| `bundles.py` | Data-shape contracts: `RawDataBundle`, `ModelStateBundle`. The formal interface between data ingest and model orchestration. |
| `constants.py` | `ProcessingMode` enum (lazy / eager) used by the feature registry. |
| `logging.py` | `setup_logger(name)` — unified stdout logger. |
| `exceptions.py` | Custom exception types. |
| `model.py` | `IBGatewayConfig`, `Config` dataclasses for production YAML configs. |
| `utils.py` | Tiny one-off helpers (typed dict access, etc.). |

## Examples

```python
from src.common import (
    is_us_business_day, now_et, next_market_open,
    new_basket_id, new_order_ref,
)

# Should we even be running today?
if not is_us_business_day(now_et().date()):
    print("Market closed today, skipping.")
    sys.exit(0)

# Build a deterministic basket ID — re-running with same inputs is idempotent.
basket = new_basket_id(rebalance_date="2026-04-24", fingerprint="sha256_of_targets")
order = new_order_ref("SPY", "BUY", 100, basket)
print(order)
# o-20260427-013908-SPY-00f9f2
```

## Adding a new utility

Drop a new module here. Re-export it from `__init__.py` if it's worth a
public name. Add a row to the table above. Don't add anything that
requires a third-party dep beyond pandas/numpy/polars.
