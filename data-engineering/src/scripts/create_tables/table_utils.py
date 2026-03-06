"""
Shared utilities for BigQuery table initialization scripts.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from pipeline.common.enums import Environment
from pipeline.common.models import GCPConfig
from pipeline.common.utils import read_gcp_config


def parse_mode_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--mode",
        required=True,
        choices=["production", "development"],
        help="Target dataset mode",
    )
    return parser.parse_args()


def environment_from_mode(mode: str) -> Environment:
    return Environment.PRODUCTION if mode == "production" else Environment.DEVELOPMENT


def load_gcp_config(mode: str) -> GCPConfig:
    return read_gcp_config(environment_from_mode(mode))


def build_table_id(config: GCPConfig, table_name: str) -> str:
    return f"{config.project}.{config.dataset}.{table_name}"


def read_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as file:
        return json.load(file)


def recreate_table(
    client: bigquery.Client,
    table_id: str,
    schema: list[bigquery.SchemaField],
    clustering_fields: list[str] | None = None,
    time_partitioning: bigquery.TimePartitioning | None = None,
) -> None:
    client.delete_table(table_id, not_found_ok=True)
    print(f"Deleted table if exists: {table_id}")

    table = bigquery.Table(table_id, schema=schema)
    if clustering_fields:
        table.clustering_fields = clustering_fields
    if time_partitioning:
        table.time_partitioning = time_partitioning

    client.create_table(table, exists_ok=False)
    print(f"Created table: {table_id}")


def insert_rows(client: bigquery.Client, table_id: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        print(f"No seed rows to insert for: {table_id}")
        return

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"Failed to insert seed rows for {table_id}: {errors}")

    print(f"Inserted {len(rows)} rows into: {table_id}")
