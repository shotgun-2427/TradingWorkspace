"""
Cloud Run job – export Universal Supplemental data (range + latest) to Parquet
Author: Rithwik Babu
"""
import logging
import math
import os
import sys
import time
import traceback
from datetime import datetime

import pandas as pd
from hawk_sdk.api import UniversalSupplemental
from opentelemetry import trace

from pipeline.common.enums import Environment
from pipeline.common.otel import setup_otel, flush_otel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)

BUCKET = "wsb-hc-qasap-bucket-1"
PREFIX = "hcf/raw_data/universal_supplemental"

# --- Supplemental series to export (the 3 from the docs example) ---
SOURCES = ["eia_petroleum"]
SERIES_IDS = ["WCESTUS1", "WCRNTUS2", "WCRFPUS2"]


def is_local_mode(environment: Environment) -> bool:
    """
    Determine if running in local mode (print output) or production mode (write to GCS).

    - WRITE_MODE=LOG forces local mode
    - WRITE_MODE=GCS forces production mode
    - Default: local mode for dev, production mode for prod
    """
    write_mode_str = os.environ.get("WRITE_MODE", "").upper()

    if write_mode_str == "LOG":
        return True
    elif write_mode_str in ("GCS",):
        return False

    return environment != Environment.PRODUCTION


def run_supplemental_export() -> None:
    tracer = trace.get_tracer(__name__)

    env_str = os.environ.get("environment", "dev")
    environment = Environment.PRODUCTION if env_str == "prod" else Environment.DEVELOPMENT
    local_mode = is_local_mode(environment)

    # Date range (overrideable)
    start_date = os.environ.get("START_DATE", "1960-01-01")
    end_date = os.environ.get("END_DATE", datetime.now().strftime("%Y-%m-%d"))

    logging.info(
        "Running with environment=%s, local_mode=%s, start_date=%s, end_date=%s",
        environment.value,
        local_mode,
        start_date,
        end_date,
    )
    logging.info("Supplemental query: sources=%s series_ids=%s", SOURCES, SERIES_IDS)

    with tracer.start_as_current_span("universal_supplemental_export") as span:
        try:
            supplemental = UniversalSupplemental(
                environment="production" if environment == Environment.PRODUCTION else "development"
            )

            # ---- Pull historical range ----
            logging.info("Fetching supplemental range data...")
            range_df = supplemental.get_data(
                sources=SOURCES,
                series_ids=SERIES_IDS,
                start_date=start_date,
                end_date=end_date,
            ).to_df()

            # ---- Pull latest point for each series ----
            logging.info("Fetching supplemental latest data...")
            latest_df = supplemental.get_latest_data(
                sources=SOURCES,
                series_ids=SERIES_IDS,
            ).to_df()

            # Normalize timestamps
            if not range_df.empty and "record_timestamp" in range_df.columns:
                range_df["record_timestamp"] = pd.to_datetime(range_df["record_timestamp"], utc=True, errors="coerce")
            if not latest_df.empty and "record_timestamp" in latest_df.columns:
                latest_df["record_timestamp"] = pd.to_datetime(latest_df["record_timestamp"], utc=True, errors="coerce")

            # Append only genuinely-new latest records (avoid duplicates if end_date already includes them)
            if not range_df.empty and not latest_df.empty and "record_timestamp" in range_df.columns:
                max_ts = range_df["record_timestamp"].max()
                latest_append = latest_df[latest_df["record_timestamp"] > max_ts]
            else:
                latest_append = latest_df

            combined_df = pd.concat([range_df, latest_append], join="outer", ignore_index=True)

            if combined_df.empty:
                logging.warning("No supplemental data to write.")
                return

            # Optional: stable ordering
            sort_cols = [c for c in ["source", "series_id", "record_timestamp"] if c in combined_df.columns]
            if sort_cols:
                combined_df = combined_df.sort_values(sort_cols).reset_index(drop=True)

            # ---- Local mode: print summary and sample data ----
            if local_mode:
                logging.info("Combined DataFrame shape: %s", combined_df.shape)
                logging.info("Columns: %s", list(combined_df.columns))
                if "record_timestamp" in combined_df.columns:
                    logging.info(
                        "Timestamp range: %s to %s",
                        combined_df["record_timestamp"].min(),
                        combined_df["record_timestamp"].max(),
                    )
                print("\n=== Sample Data (first 25 rows) ===")
                print(combined_df.head(25).to_string())
                print("\n=== Sample Data (last 25 rows) ===")
                print(combined_df.tail(25).to_string())
                logging.info("Local mode completed - data NOT written to GCS")
                return

            # ---- Write to Parquet (sharded) ----
            run_date = datetime.now().strftime("%Y-%m-%d")
            parquet_prefix = f"gs://{BUCKET}/{PREFIX}/parquet"
            logging.info("Writing Parquet shards to %s ...", parquet_prefix)

            start_time = time.perf_counter()
            TARGET_MB = 128
            TARGET_B = TARGET_MB * 2 ** 20

            total_bytes = combined_df.memory_usage(deep=True).sum()
            bytes_per_row = total_bytes / max(1, len(combined_df))
            rows_per_chunk = max(1, int(TARGET_B / max(1.0, bytes_per_row)))
            est_files = math.ceil(len(combined_df) / rows_per_chunk)

            logging.info("Writing %d parquet shards (~%d rows each)", est_files, rows_per_chunk)

            file_count = 0
            for i, start in enumerate(range(0, len(combined_df), rows_per_chunk)):
                df_chunk = combined_df.iloc[start: start + rows_per_chunk]
                file_path = f"{parquet_prefix}/part-{i:05d}.parquet"
                df_chunk.to_parquet(file_path, engine="pyarrow", index=False)
                file_count += 1

            end_time = time.perf_counter()
            span.set_attribute("export.parquet.duration_s", end_time - start_time)
            logging.info("Parquet export completed: %d file(s) in %.2f seconds", file_count, end_time - start_time)

            # ---- Write to CSV (single file) ----
            csv_path = f"gs://{BUCKET}/{PREFIX}/csv/{run_date}.csv"
            logging.info("Writing single-file CSV to %s ...", csv_path)

            start_time = time.perf_counter()
            combined_df.to_csv(csv_path, index=False)
            end_time = time.perf_counter()

            span.set_attribute("export.csv.duration_s", end_time - start_time)
            logging.info("CSV export completed in %.2f seconds", end_time - start_time)

        except Exception as e:
            span.set_status(trace.StatusCode.ERROR, str(traceback.format_exc().splitlines()[0]))
            span.record_exception(e)
            logging.error("Error during Universal Supplemental export: %s", e)
            raise


if __name__ == "__main__":
    setup_otel("universal_supplemental_export")
    run_supplemental_export()
    flush_otel()
