# API Module (`src/api`)

This module provides a simple programmatic API for:

- Reading from `fields` and `hawk_identifiers`
- Adding, updating, and deleting records in those tables
- Triggering the Cloud Run backfill job

## Classes and Enums

- `DataAPI` (`src/api/data_api.py`)
- `BackfillPipeline` (`src/api/enums.py`)

## Quick Start

```python
from api import DataAPI
from pipeline.common.enums import Environment

api = DataAPI(Environment.PRODUCTION)
```

## Read Methods

### `read_fields() -> pandas.DataFrame`

Returns:
- `field_id`
- `field_name`
- `field_type`

### `read_hawk_ids(id_type: str | None = None, asset_class: str | None = None) -> pandas.DataFrame`

Returns:
- `hawk_id`
- `asset_class`
- `id_type`
- `value`

If `id_type` and/or `asset_class` is provided, rows are filtered.

## Write Methods

### Fields table

- `add_field(field_id: int, field_name: str, field_type: str) -> None`
- `update_field(field_id: int, field_name: str | None = None, field_type: str | None = None, new_field_id: int | None = None) -> None`
- `delete_field(field_id: int) -> None`

Safety checks:
- No duplicate `field_id`
- No duplicate `field_name` (case-insensitive, trimmed)
- `field_type` must be one of: `int`, `double`, `char`

### Hawk identifiers table

- `add_hawk_id(hawk_id: int, asset_class: str, id_type: str, value: str) -> None`
- `update_hawk_id(hawk_id: int, asset_class: str | None = None, id_type: str | None = None, value: str | None = None, new_hawk_id: int | None = None) -> None`
- `delete_hawk_id(hawk_id: int, id_type: str | None = None) -> None`

Safety checks:
- No duplicate `hawk_id`
- `asset_class` must be one of: `equities`, `futures`, `ice_bofa_bond_indices`, `global_indices`, `other`
- No duplicate ticker value for `id_type='TICKER'` (case-insensitive, trimmed)
- Empty identifiers are rejected

## Backfill Method

### `trigger_backfill(...) -> str`

```python
from api import BackfillPipeline
from pipeline.common.enums import Environment

output = api.trigger_backfill(
    hawk_id=10039,
    pipeline=BackfillPipeline.EQUITIES,
    wait_for_completion=True,
)
```

This executes:

```bash
gcloud run jobs execute hawk-backfill \
  --region us-central1 \
  --args="production,10039,equities"
```

Parameters:
- `hawk_id`: target hawk id
- `pipeline`: enum (currently only `EQUITIES`)
- `wait_for_completion`: if `True`, adds `--wait`; if `False`, adds `--async` for immediate return
- `job_name`: default `"hawk-backfill"`
- `region`: default `"us-central1"`

`trigger_backfill` automatically uses the environment from `DataAPI(...)`.

Return value:
- On success: returns a Cloud Console execution URL when available
- Fallback: returns `gcloud` stdout (or a submission message) if URL resolution fails
- On failure: raises `RuntimeError` with command error details

## Notes

- `DataAPI` uses the same BigQuery config loading approach as the rest of the project (`pipeline.common.utils.read_gcp_config`).
- Ensure your environment has GCP authentication configured and `gcloud` is installed for backfill execution.
- Backfill Cloud Run job source now lives at `src/api/backfill_job/` with entrypoint `api.backfill_job.run`.
