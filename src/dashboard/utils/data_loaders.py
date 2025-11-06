"""
Data loading utilities for the dashboard.
"""
from datetime import datetime
import json
from google.cloud import storage
import streamlit as st
import pandas as pd
from io import BytesIO


# Constants
BUCKET_NAME = "wsb-hc-qasap-bucket-1"
SIMULATIONS_AUDIT_PREFIX = "hcf/paper/simulations_audit/"
PRODUCTION_AUDIT_PREFIX = "hcf/paper/production_audit/"


@st.cache_resource
def get_gcs_bucket() -> storage.Bucket:
    """Return a cached Google Cloud Storage `Bucket` for reuse across the app.

    This returns the configured `BUCKET_NAME` as a cached resource so other
    functions can reuse the same bucket object instead of recreating clients
    or buckets on every call.
    """
    client = storage.Client()
    return client.bucket(BUCKET_NAME)


@st.cache_data(ttl=3600)
def get_latest_simulations_audit() -> datetime.date:
    """
    Get the most recent audit date from the production simulations directory in GCS.
    
    Returns:
        datetime.date: The most recent audit date found in the simulations_audit directory.
    """
    bucket = get_gcs_bucket()
    
    # List all blobs with the prefix and delimiter to get "directory" listing
    blobs = bucket.list_blobs(prefix=SIMULATIONS_AUDIT_PREFIX)
    # Get all dates from directory prefixes
    raw_dates = set(blob.name.split('/')[3] for blob in blobs)
    return max(datetime.strptime(prefix, '%Y-%m-%d').date() for prefix in raw_dates)


@st.cache_data(ttl=3600)
def get_latest_production_audit() -> datetime.date:
    """
    Get the most recent audit date from the production directory in GCS.
    
    Returns:
        datetime.date: The most recent audit date found in the production_audit directory.
    """
    bucket = get_gcs_bucket()
    
    # List all blobs with the prefix and delimiter to get "directory" listing
    blobs = bucket.list_blobs(prefix=PRODUCTION_AUDIT_PREFIX)
    
    raw_dates = set(blob.name.split('/')[3] for blob in blobs)
    return max(datetime.strptime(prefix, '%Y-%m-%d').date() for prefix in raw_dates)


@st.cache_data(ttl=3600)
def get_production_audit_config() -> dict:
    """
    Read `config.json` from the latest production audit folder and return its contents as a dictionary.

    Returns:
        dict: The contents of `config.json` as a dictionary.
    """
    latest_date = get_latest_production_audit()
    config_blob_path = f"{PRODUCTION_AUDIT_PREFIX}{latest_date.strftime('%Y-%m-%d')}/config.json"

    bucket = get_gcs_bucket()
    blob = bucket.blob(config_blob_path)

    content = blob.download_as_text()
    data = json.loads(content)

    return data


def get_production_audit_models() -> list:
    """
    Read `config.json` from the latest production audit folder and return the
    value of its "models" field.

    Returns:
        list: The list from the `models` field in `config.json` (or an empty
        list if the field is missing).
    """
    return get_production_audit_config().get("models", [])


def get_production_audit_optimizers() -> list:
    """
    Read `config.json` from the latest production audit folder and return the
    value of its "optimizers" field.

    Returns:
        list: The list from the `optimizers` field in `config.json` (or an empty
        list if the field is missing).
    """
    return get_production_audit_config().get("optimizers", [])


@st.cache_data(ttl=3600)
def get_model_backtest(model_name: str) -> pd.DataFrame:
    """
    Load a model's backtest results CSV from the latest production audit into a
    pandas DataFrame.

    The CSV path format is:
        hcf/paper/production_audit/YYYY-MM-DD/model_backtests_{model_name}_backtest_results.csv

    Args:
        model_name: Name of the model to load (used in the filename).

    Returns:
        pd.DataFrame: The backtest results as a pandas DataFrame.
    """
    latest_date = get_latest_production_audit()
    csv_blob_path = (
        f"{PRODUCTION_AUDIT_PREFIX}{latest_date.strftime('%Y-%m-%d')}/"
        f"model_backtests_{model_name}_backtest_results.csv"
    )

    bucket = get_gcs_bucket()
    blob = bucket.blob(csv_blob_path)

    data_bytes = blob.download_as_bytes()
    df = pd.read_csv(BytesIO(data_bytes))

    return df


@st.cache_data(ttl=3600)
def get_portfolio_backtest(optimizer_name: str) -> pd.DataFrame:
    """
    Load a portfolio backtest CSV for the given optimizer from the latest
    production audit into a pandas DataFrame.

    The CSV path format is:
        hcf/paper/production_audit/YYYY-MM-DD/portfolio_backtests_{optimizer_name}_backtest_results.csv

    Args:
        optimizer_name: Name of the optimizer to load (used in the filename).

    Returns:
        pd.DataFrame: The backtest results as a pandas DataFrame.
    """
    latest_date = get_latest_production_audit()
    csv_blob_path = (
        f"{PRODUCTION_AUDIT_PREFIX}{latest_date.strftime('%Y-%m-%d')}/"
        f"portfolio_backtests_{optimizer_name}_backtest_results.csv"
    )

    bucket = get_gcs_bucket()
    blob = bucket.blob(csv_blob_path)

    data_bytes = blob.download_as_bytes()
    df = pd.read_csv(BytesIO(data_bytes))

    return df