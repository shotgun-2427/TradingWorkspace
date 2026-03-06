---
name: data-api-python
description: Use the repository Python DataAPI (`src/api/data_api.py`) for scripted API workflows, table reads/writes, and Cloud Run backfill execution via `DataAPI` and `BackfillPipeline`. Trigger when the user wants Python snippets, script-level automation, notebooks, or programmatic API usage instead of CLI commands.
---

# Data API Python

Use `DataAPI` for programmatic access to `fields` and `hawk_identifiers` and for triggering backfills.

## Imports and setup

```python
from api import DataAPI, BackfillPipeline
from pipeline.common.enums import Environment

api = DataAPI(Environment.PRODUCTION)
```

## Read workflows

```python
fields_df = api.read_fields()
hawk_df = api.read_hawk_ids(id_type="TICKER", asset_class="equities")
```

## Write workflows

```python
api.add_field(field_id=9999, field_name="my_field_name", field_type="double")
api.update_field(field_id=9999, field_name="my_field_name_v2", field_type="char")
api.delete_field(field_id=9999)

api.add_hawk_id(hawk_id=123456, asset_class="equities", id_type="TICKER", value="AAPL")
api.update_hawk_id(hawk_id=123456, value="AAPL.O")
api.delete_hawk_id(hawk_id=123456, id_type="TICKER")
```

## Backfill workflow

```python
execution_url = api.trigger_backfill(
    hawk_id=123456,
    pipeline=BackfillPipeline.EQUITIES,
    wait_for_completion=True,
)
print(execution_url)
```

## Behavioral Notes

- `field_type` must be one of `int`, `double`, `char`.
- `asset_class` is normalized to lowercase and validated.
- `id_type` is normalized to uppercase.
- Empty strings are rejected for names/identifier values.
- Duplicate checks enforce:
  - unique `field_id`
  - unique `field_name` (case-insensitive, trimmed)
  - unique `hawk_id`
  - unique `TICKER` value (case-insensitive, trimmed)
- `trigger_backfill` requires positive `hawk_id` and shells out to `gcloud run jobs execute`.

## Execution Guidance

- Wrap mutating calls in try/except and report clear remediation when `ValueError` or `RuntimeError` occurs.
- For backfill usage, ensure gcloud auth and project access are already configured.
- Use this skill when Python orchestration or multi-step logic is needed; otherwise prefer the CLI skill.
