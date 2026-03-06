"""
Initialize the supplemental_series table and seed known series metadata.
"""
from google.cloud import bigquery

from pipeline.common.constants import SUPPLEMENTAL_SERIES_TABLE_NAME
from scripts.create_tables.table_utils import (
    parse_mode_args,
    load_gcp_config,
    build_table_id,
    recreate_table,
    insert_rows,
)

# Initial series metadata for EIA petroleum data
EIA_PETROLEUM_SERIES = [
    {
        "source": "eia_petroleum",
        "series_id": "WCESTUS1",
        "name": "U.S. Ending Stocks excl. SPR of Crude Oil",
        "description": "Weekly U.S. Ending Stocks excluding SPR of Crude Oil",
        "frequency": "weekly",
        "unit": "Thousand Barrels"
    },
    {
        "source": "eia_petroleum",
        "series_id": "WCRNTUS2",
        "name": "U.S. Net Imports of Crude Oil",
        "description": "Weekly U.S. Net Imports of Crude Oil",
        "frequency": "weekly",
        "unit": "Thousand Barrels per Day"
    },
    {
        "source": "eia_petroleum",
        "series_id": "WCRFPUS2",
        "name": "U.S. Field Production of Crude Oil",
        "description": "Weekly U.S. Field Production of Crude Oil",
        "frequency": "weekly",
        "unit": "Thousand Barrels per Day"
    }
]


def main() -> None:
    args = parse_mode_args("Initialize supplemental_series table")
    cfg = load_gcp_config(args.mode)
    client = bigquery.Client(project=cfg.project)
    table_id = build_table_id(cfg, SUPPLEMENTAL_SERIES_TABLE_NAME)

    schema = [
        bigquery.SchemaField("source", "STRING", mode="REQUIRED", description="Source identifier"),
        bigquery.SchemaField("series_id", "STRING", mode="REQUIRED", description="Series identifier"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED", description="Series name"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE", description="Series description"),
        bigquery.SchemaField("frequency", "STRING", mode="REQUIRED", description="Series frequency"),
        bigquery.SchemaField("unit", "STRING", mode="REQUIRED", description="Series unit"),
    ]
    recreate_table(client, table_id, schema)
    insert_rows(client, table_id, EIA_PETROLEUM_SERIES)


if __name__ == "__main__":
    main()
