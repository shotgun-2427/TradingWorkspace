# Scripts

This directory now contains only BigQuery table initialization scripts.

## Directory Structure

```
scripts/
└── create_tables/
    ├── setup_all_tables.py
    ├── setup_fields_table.py
    ├── setup_hawk_identifiers_table.py
    ├── setup_records_table.py
    ├── setup_supplemental_series_table.py
    ├── setup_supplemental_records_table.py
    └── table_utils.py
```

## Purpose

Each setup script recreates the target table from scratch.

- Existing table is deleted (if present)
- Table is created with the canonical schema
- Seed data is inserted when required

## Configuration Inputs

- `config/tables/fields.json`
- `config/tables/hawk_identifiers.json`

## Usage

All scripts require:

- `--mode production`
- or `--mode development`

### Initialize all tables

```bash
python3 src/scripts/create_tables/setup_all_tables.py --mode production
```

### Initialize individual tables

```bash
python3 src/scripts/create_tables/setup_fields_table.py --mode production
python3 src/scripts/create_tables/setup_hawk_identifiers_table.py --mode production
python3 src/scripts/create_tables/setup_records_table.py --mode production
python3 src/scripts/create_tables/setup_supplemental_series_table.py --mode production
python3 src/scripts/create_tables/setup_supplemental_records_table.py --mode production
```

## Notes

- `setup_hawk_identifiers_table.py` expects explicit `asset_class` values in hawk identifier config.
- Re-running setup scripts is destructive for the target table by design.
