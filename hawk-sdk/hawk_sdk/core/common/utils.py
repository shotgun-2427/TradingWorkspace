import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account

from hawk_sdk.core.common.constants import PROJECT_ID


def get_bigquery_client() -> bigquery.Client:
    service_account_json = os.environ.get('SERVICE_ACCOUNT_JSON')
    if service_account_json:
        # Use credentials provided in SERVICE_ACCOUNT_JSON
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(service_account_json)
        )
        return bigquery.Client(project=PROJECT_ID, credentials=credentials)
    else:
        # Rely on Application Default Credentials (ADC),
        # which will automatically use GOOGLE_APPLICATION_CREDENTIALS if set,
        # or use the built-in credentials if running in GCP.
        return bigquery.Client(project=PROJECT_ID)

