"""
Initialize the supplemental_records fact table.
"""
from google.cloud import bigquery

from pipeline.common.constants import SUPPLEMENTAL_RECORDS_TABLE_NAME
from scripts.create_tables.table_utils import (
    parse_mode_args,
    load_gcp_config,
    build_table_id,
    recreate_table,
)


def main() -> None:
    args = parse_mode_args("Initialize supplemental_records table")
    cfg = load_gcp_config(args.mode)
    client = bigquery.Client(project=cfg.project)
    table_id = build_table_id(cfg, SUPPLEMENTAL_RECORDS_TABLE_NAME)

    schema = [
        bigquery.SchemaField("source", "STRING", mode="REQUIRED", description="Source identifier"),
        bigquery.SchemaField("series_id", "STRING", mode="REQUIRED", description="Series identifier"),
        bigquery.SchemaField("record_timestamp", "TIMESTAMP", mode="REQUIRED", description="Record timestamp"),
        bigquery.SchemaField("value", "FLOAT64", mode="NULLABLE", description="Numeric value"),
        bigquery.SchemaField("char_value", "STRING", mode="NULLABLE", description="String value"),
    ]
    recreate_table(
        client,
        table_id,
        schema,
        clustering_fields=["source", "series_id"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="record_timestamp",
            expiration_ms=None,
        ),
    )


if __name__ == "__main__":
    main()
