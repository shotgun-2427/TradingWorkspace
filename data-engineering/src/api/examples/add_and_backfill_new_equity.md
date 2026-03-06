# Add And Backfill A New Equity

This example shows how to:

1. List existing hawk IDs
2. Find the next available equity ID from rows where `asset_class = equities`
3. Add a new ticker (`TEST-US`)
4. Trigger the equities backfill job

## Example

```python
from api import DataAPI, BackfillPipeline
from pipeline.common.enums import Environment

# Use PRODUCTION because backfill is typically run there
api = DataAPI(Environment.PRODUCTION)

# 1) List all current hawk IDs
hawk_df = api.read_hawk_ids()
existing_hawk_ids = sorted(hawk_df["hawk_id"].astype(int).tolist())
print(f"Existing hawk IDs ({len(existing_hawk_ids)}):")
print(existing_hawk_ids)

# 2) Find next available equity ID based on explicit asset_class
equity_rows = api.read_hawk_ids(asset_class="equities")
equity_ids = sorted(equity_rows["hawk_id"].astype(int).tolist())
next_equity_id = (max(equity_ids) + 1) if equity_ids else ((max(existing_hawk_ids) + 1) if existing_hawk_ids else 1)
print(f"Next available equity hawk_id: {next_equity_id}")

# 3) Add the new equity identifier
api.add_hawk_id(
    hawk_id=next_equity_id,
    asset_class="equities",
    id_type="TICKER",
    value="TEST-US",
)
print(f"Inserted TEST-US with hawk_id={next_equity_id}")

# 4) Trigger backfill for that new ID
backfill_output = api.trigger_backfill(
    hawk_id=next_equity_id,
    pipeline=BackfillPipeline.EQUITIES,
)
print("Backfill triggered successfully.")
print(backfill_output)
```

## Notes

- `DataAPI` already contains duplicate safety checks:
  - duplicate `hawk_id` is blocked
  - duplicate ticker values (for `TICKER`) are blocked
- If `TEST-US` already exists, `add_hawk_id` will raise a `ValueError`.
- `trigger_backfill` returns the `gcloud` stdout on success and raises `RuntimeError` on failure.
