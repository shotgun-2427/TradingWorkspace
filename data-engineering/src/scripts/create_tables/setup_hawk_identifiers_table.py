"""
Initialize the hawk_identifiers table and seed from config.
"""
from google.cloud import bigquery

from pipeline.common.constants import HAWK_IDENTIFIERS_TABLE_NAME
from pipeline.common.gData import hawk_identifiers_table
from pipeline.common.models import HawkIdentifiersConfig
from scripts.create_tables.table_utils import (
    parse_mode_args,
    load_gcp_config,
    build_table_id,
    read_json,
    recreate_table,
    insert_rows,
)


def main() -> None:
    args = parse_mode_args("Initialize hawk_identifiers table")
    cfg = load_gcp_config(args.mode)
    client = bigquery.Client(project=cfg.project)
    table_id = build_table_id(cfg, HAWK_IDENTIFIERS_TABLE_NAME)

    schema = [
        bigquery.SchemaField("hawk_id", "INT64", mode="REQUIRED", description="Unique Hawk identifier"),
        bigquery.SchemaField("asset_class", "STRING", mode="REQUIRED", description="Asset class label"),
        bigquery.SchemaField("id_type", "STRING", mode="REQUIRED", description="Identifier type"),
        bigquery.SchemaField("value", "STRING", mode="REQUIRED", description="Identifier value"),
    ]
    recreate_table(client, table_id, schema)

    config_data = read_json(hawk_identifiers_table)
    validated = HawkIdentifiersConfig(**config_data)
    seed_rows = [
        {
            "hawk_id": identifier.hawk_id,
            "asset_class": identifier.asset_class,
            "id_type": identifier.id_type,
            "value": identifier.value,
        }
        for identifier in validated.hawk_identifiers
    ]
    insert_rows(client, table_id, seed_rows)


if __name__ == "__main__":
    main()
