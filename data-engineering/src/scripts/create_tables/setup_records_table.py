"""
Initialize the records fact table.
"""
from google.cloud import bigquery

from pipeline.common.constants import RECORDS_TABLE_NAME
from scripts.create_tables.table_utils import (
    parse_mode_args,
    load_gcp_config,
    build_table_id,
    recreate_table,
)


def main() -> None:
    args = parse_mode_args("Initialize records table")
    cfg = load_gcp_config(args.mode)
    client = bigquery.Client(project=cfg.project)
    table_id = build_table_id(cfg, RECORDS_TABLE_NAME)

    schema = [
        bigquery.SchemaField("hawk_id", "INT64", mode="REQUIRED", description="Hawk identifier"),
        bigquery.SchemaField("record_timestamp", "TIMESTAMP", mode="REQUIRED", description="Record timestamp"),
        bigquery.SchemaField("field_id", "INT64", mode="REQUIRED", description="Field identifier"),
        bigquery.SchemaField("char_value", "STRING", mode="NULLABLE", description="String value"),
        bigquery.SchemaField("int_value", "INT64", mode="NULLABLE", description="Integer value"),
        bigquery.SchemaField("double_value", "FLOAT64", mode="NULLABLE", description="Float value"),
    ]
    recreate_table(
        client,
        table_id,
        schema,
        clustering_fields=["hawk_id", "field_id"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="record_timestamp",
            expiration_ms=None,
        ),
    )


if __name__ == "__main__":
    main()
