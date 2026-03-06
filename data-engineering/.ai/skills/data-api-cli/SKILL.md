---
name: data-api-cli
description: Use the repository DataAPI CLI for read/write operations on `fields` and `hawk_identifiers`, plus backfill triggering, through `poetry run data-api`. Trigger when the user asks for API operations that should be executed as terminal commands with JSON output, quick CRUD changes, filtered reads, or agent-friendly command responses.
---

# Data API CLI

Execute API operations with the project CLI entrypoint:

```bash
poetry run data-api --environment <PRODUCTION|DEVELOPMENT> <command> [args]
```

Prefer this skill when the task can be solved without writing Python code.

## Preconditions

- Run from repository root.
- Ensure Poetry environment is available.
- Use the correct environment: `PRODUCTION` or `DEVELOPMENT`.

## Core Command Patterns

### Read tables

```bash
poetry run data-api --environment PRODUCTION read-fields --pretty
poetry run data-api --environment PRODUCTION read-hawk-ids --id-type TICKER --asset-class equities --pretty
```

### Fields CRUD

```bash
poetry run data-api --environment PRODUCTION add-field 9999 my_field_name double
poetry run data-api --environment PRODUCTION update-field 9999 --field-name my_field_name_v2 --field-type char
poetry run data-api --environment PRODUCTION delete-field 9999
```

### Hawk identifiers CRUD

```bash
poetry run data-api --environment PRODUCTION add-hawk-id 123456 equities TICKER AAPL
poetry run data-api --environment PRODUCTION update-hawk-id 123456 --value AAPL.O
poetry run data-api --environment PRODUCTION delete-hawk-id 123456 --id-type TICKER
```

### Trigger backfill

```bash
poetry run data-api --environment PRODUCTION trigger-backfill 123456 --pipeline equities --wait-for-completion
```

## Behavioral Notes

- CLI returns JSON payloads with `ok` and either result fields or `error`.
- `asset_class` is validated (`equities`, `futures`, `ice_bofa_bond_indices`, `global_indices`, `other`).
- `field_type` is validated (`int`, `double`, `char`).
- `TICKER` uniqueness checks are case-insensitive and trim whitespace.
- `trigger-backfill` defaults to job `hawk-backfill` in `us-central1`.

## Execution Guidance

- For destructive commands, run a read first to confirm target rows.
- Preserve and report CLI JSON responses exactly when the caller needs machine-readable output.
- If a command fails, surface the exact error and propose the smallest correction.
