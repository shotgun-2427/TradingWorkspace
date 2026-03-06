"""
Initialize the fields table and seed from config.
"""
from google.cloud import bigquery

from pipeline.common.constants import FIELDS_TABLE_NAME
from pipeline.common.gData import fields_table
from pipeline.common.models import FieldsConfig
from scripts.create_tables.table_utils import (
    parse_mode_args,
    load_gcp_config,
    build_table_id,
    read_json,
    recreate_table,
    insert_rows,
)


def main() -> None:
    args = parse_mode_args("Initialize fields table")
    cfg = load_gcp_config(args.mode)
    client = bigquery.Client(project=cfg.project)
    table_id = build_table_id(cfg, FIELDS_TABLE_NAME)

    schema = [
        bigquery.SchemaField("field_id", "INT64", mode="REQUIRED", description="Unique field identifier"),
        bigquery.SchemaField("field_name", "STRING", mode="REQUIRED", description="Canonical field name"),
        bigquery.SchemaField("field_type", "STRING", mode="REQUIRED", description="Field value type"),
    ]
    recreate_table(client, table_id, schema)

    config_data = read_json(fields_table)
    validated = FieldsConfig(**config_data)
    seed_rows = [
        {
            "field_id": field.field_id,
            "field_name": field.field_name,
            "field_type": field.field_type,
        }
        for field in validated.fields
    ]
    insert_rows(client, table_id, seed_rows)


if __name__ == "__main__":
    main()
