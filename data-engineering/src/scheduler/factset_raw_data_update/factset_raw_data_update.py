"""
Cloud Run job – export Universal EOD (final + snapshot) to Parquet
Author: Rithwik Babu
"""
import logging
import math
import os
import sys
import time
import traceback
from datetime import datetime, date

import pandas as pd
from hawk_sdk.api import Universal
from opentelemetry import trace

from pipeline.common.bigquery_client import BigQueryClient
from pipeline.common.enums import Environment
from pipeline.common.otel import setup_otel, flush_otel
from pipeline.common.utils import read_gcp_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],  # send to stdout
    force=True,  # <-- overrides any prior config so INFO logs actually show
)

BUCKET = "wsb-hc-qasap-bucket-1"
PREFIX = "hcf/raw_data/universal"
INTERVAL = "1d"

# Fields to export - EOD fields for all asset classes
EOD_FIELDS = [
    # Equities - adjusted OHLCV
    "adjusted_open_1d",
    "adjusted_high_1d",
    "adjusted_low_1d",
    "adjusted_close_1d",
    "volume_1d",
    # BofA Bond Indices
    "total_return_1d",
    "oas_1d",
    "duration_modified_1d",
    "duration_effective_1d",
    "convexity_1d",
    # Global Indices
    "price_1d",
]

# Snapshot fields (equities only - bonds and indices don't have snapshots)
SNAPSHOT_FIELDS = [
    "adjusted_open_snapshot",
    "adjusted_high_snapshot",
    "adjusted_low_snapshot",
    "adjusted_close_snapshot",
    "volume_snapshot",
]


def is_local_mode(environment: Environment) -> bool:
    """
    Determine if running in local mode (print output) or production mode (write to GCS).
    
    - WRITE_MODE=LOG forces local mode
    - WRITE_MODE=GCS or WRITE_MODE=BIGQUERY forces production mode
    - Default: local mode for dev, production mode for prod
    """
    write_mode_str = os.environ.get("WRITE_MODE", "").upper()

    if write_mode_str == "LOG":
        return True
    elif write_mode_str in ("GCS", "BIGQUERY"):
        return False

    # Default based on environment
    return environment != Environment.PRODUCTION


def run_snapshot_export() -> None:
    tracer = trace.get_tracer(__name__)
    env_str = os.environ.get("environment", "dev")
    environment = Environment.PRODUCTION if env_str == "prod" else Environment.DEVELOPMENT
    local_mode = is_local_mode(environment)

    logging.info(f"Running with environment={environment.value}, local_mode={local_mode}")

    with tracer.start_as_current_span("factset_raw_data_update") as span:
        try:
            config = read_gcp_config(environment)
            bq_client = BigQueryClient(config)

            # Load all hawk_ids across all asset classes
            categorized_ids = bq_client.load_categorized_hawk_ids()
            all_hawk_ids = []
            for category, ids in categorized_ids.items():
                if category in ["equities", "ice_bofa_bond_indices", "global_indices"]:
                    all_hawk_ids.extend(ids)

            # Equities hawk_ids are needed separately for snapshot (only equities have snapshots)
            equities_hawk_ids = categorized_ids.get("equities", [])

            logging.info(f"Loaded {len(all_hawk_ids)} hawk_ids across {len(categorized_ids)} categories")
            logging.info(f"  - Equities (for snapshot): {len(equities_hawk_ids)}")

        except Exception as e:
            span.set_status(trace.StatusCode.ERROR, str(traceback.format_exc().splitlines()[0]))
            span.record_exception(e)
            logging.error(f"Error loading Hawk IDs from BigQuery: {e}")

            raise

        try:
            parquet_uri = f"gs://{BUCKET}/{PREFIX}/parquet/part-*.parquet"

            logging.info("Fetching snapshot + EOD data%s",
                         " (local mode - will print instead of write)" if local_mode else f" for export to {parquet_uri}")

            universal = Universal(
                environment="production" if environment == Environment.PRODUCTION else "development"
            )

            # Get field IDs for EOD and snapshot fields (they have different IDs)
            eod_field_lookup = universal.get_field_ids(EOD_FIELDS).to_df()
            eod_field_ids = eod_field_lookup["field_id"].tolist()

            snapshot_field_lookup = universal.get_field_ids(SNAPSHOT_FIELDS).to_df()
            snapshot_field_ids = snapshot_field_lookup["field_id"].tolist()

            logging.info(f"Resolved EOD field IDs: {dict(zip(EOD_FIELDS, eod_field_ids))}")
            logging.info(f"Resolved snapshot field IDs: {dict(zip(SNAPSHOT_FIELDS, snapshot_field_ids))}")

            eod_df = universal.get_data(
                hawk_ids=all_hawk_ids,
                field_ids=eod_field_ids,
                start_date="1960-01-01",
                end_date=datetime.now().strftime("%Y-%m-%d"),
                interval=INTERVAL,
            ).to_df()

            # Snapshot only for equities (bonds and indices don't have intraday snapshots)
            snapshot_df = universal.get_latest_snapshot(
                hawk_ids=equities_hawk_ids,
                field_ids=snapshot_field_ids,
            ).to_df()

            # Normalize snapshot column names to match EOD (remove _snapshot suffix if present)
            snapshot_df = snapshot_df.rename(columns=lambda c: c.replace('_snapshot', f'_{INTERVAL}'))

            eod_df["date"] = pd.to_datetime(eod_df["date"]).dt.date
            snapshot_df["date"] = pd.to_datetime(snapshot_df["date"]).dt.date
            max_eod_date = eod_df["date"].max() if not eod_df.empty else date(1900, 1, 1)
            snapshot_df = snapshot_df[snapshot_df["date"] > max_eod_date]

            # Use outer join to keep all columns (EOD has bond/index fields, snapshot has equities fields)
            combined_df = pd.concat([eod_df, snapshot_df], join='outer', ignore_index=True)

            if combined_df.empty:
                logging.warning("No data to write.")
                return

            # ---- Local mode: print summary and sample data ----
            if local_mode:
                logging.info(f"Combined DataFrame shape: {combined_df.shape}")
                logging.info(f"Columns: {list(combined_df.columns)}")
                logging.info(f"Date range: {combined_df['date'].min()} to {combined_df['date'].max()}")
                print("\n=== Sample Data (first 100 rows) ===")
                print(combined_df.head(25).to_string())
                print(combined_df.tail(25).to_string())
                print(f"\n=== Data Summary ===")
                print(combined_df.describe())
                logging.info("Local mode completed - data NOT written to GCS")
                return

            # ---- Write to Parquet ----
            start_time = time.perf_counter()
            TARGET_MB = 128
            TARGET_B = TARGET_MB * 2 ** 20  # 134_217_728 bytes

            # Estimate rows per 128MB shard
            total_bytes = combined_df.memory_usage(deep=True).sum()
            bytes_per_row = total_bytes / len(combined_df)
            rows_per_chunk = max(1, int(TARGET_B / bytes_per_row))
            est_files = math.ceil(len(combined_df) / rows_per_chunk)

            logging.info(
                "Writing %d parquet shards (~%d rows each)",
                est_files, rows_per_chunk
            )

            # Loop over row‑sized chunks and write each as part‑NNNN.parquet
            i = 0
            for i, start in enumerate(range(0, len(combined_df), rows_per_chunk)):
                df_chunk = combined_df.iloc[start: start + rows_per_chunk]
                file_path = f"gs://{BUCKET}/{PREFIX}/parquet/part-{i:05d}.parquet"

                df_chunk.to_parquet(
                    file_path,
                    engine="pyarrow",
                    index=False,
                )

            end_time = time.perf_counter()

            span.set_attribute("export.parquet.duration", end_time - start_time)
            logging.info("Parquet export completed: %d file(s) in %d seconds", i + 1, (end_time - start_time) / 1000)

            # ---- Write to CSV ----
            start_time = time.perf_counter()
            run_date = datetime.now().strftime("%Y-%m-%d")
            csv_path = f"gs://{BUCKET}/{PREFIX}/csv/{run_date}.csv"

            logging.info("Writing single‑file CSV to %s…", csv_path)
            combined_df.to_csv(csv_path, index=False)
            end_time = time.perf_counter()

            span.set_attribute("export.csv.duration", end_time - start_time)
            logging.info("CSV export completed in %.2f seconds", (end_time - start_time) / 1000)
        except Exception as e:
            logging.error(f"Error during FactSet snapshot export: {e}")
            raise


if __name__ == "__main__":
    setup_otel('factset_raw_data_update')
    run_snapshot_export()
    flush_otel()
