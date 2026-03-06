# Backfill Job

This package stores the Cloud Run backfill job code close to the API layer.

## Entrypoint

- Module: `api.backfill_job.run`
- CLI args:
  1. `environment` (`production` or `development`)
  2. `hawk_id` (int)
  3. `asset_class` (currently only `equities`)

Example:

```bash
python -m api.backfill_job.run production 10039 equities
```

## Deploy

Use:

```bash
bash src/api/backfill_job/deploy.sh
```

Required env vars:
- `FACTSET_API_KEY`
- `FACTSET_USERNAME`

