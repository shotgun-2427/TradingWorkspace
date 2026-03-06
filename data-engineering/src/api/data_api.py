"""
@description: API wrapper for reading data from BigQuery tables.
"""
import subprocess
import time

from pandas import DataFrame

from .enums import BackfillPipeline
from pipeline.common.bigquery_client import BigQueryClient
from pipeline.common.constants import FIELDS_TABLE_NAME, HAWK_IDENTIFIERS_TABLE_NAME
from pipeline.common.enums import Environment, AssetClass
from pipeline.common.utils import read_gcp_config


class DataAPI:
    """Simple API for reading core reference tables from BigQuery."""

    def __init__(self, environment: Environment):
        self.environment = environment
        self.config = read_gcp_config(environment)
        self.bq_client = BigQueryClient(self.config)

    def _count_rows(self, query: str, params: dict | None = None) -> int:
        """Execute a COUNT query and return the integer count."""
        result = self.bq_client.execute_query(query, params=params)
        return int(result.iloc[0]["cnt"])

    def _latest_execution_name(self, job_name: str, region: str) -> str | None:
        """
        Fetch the latest Cloud Run execution name for a job.
        Returns None if not found.
        """
        command = [
            "gcloud",
            "run",
            "jobs",
            "executions",
            "list",
            f"--job={job_name}",
            f"--region={region}",
            f"--project={self.config.project}",
            "--sort-by=~createTime",
            "--limit=1",
            "--format=value(metadata.name)",
        ]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            return None

        name = result.stdout.strip()
        return name or None

    def _field_id_exists(self, field_id: int) -> bool:
        query = f"""
        SELECT COUNT(1) AS cnt
        FROM `{self.config.project}.{self.config.dataset}.{FIELDS_TABLE_NAME}`
        WHERE field_id = @field_id
        """
        return self._count_rows(query, params={"field_id": (field_id, "INT64")}) > 0

    def _field_name_exists(self, field_name: str, exclude_field_id: int | None = None) -> bool:
        base_query = f"""
        SELECT COUNT(1) AS cnt
        FROM `{self.config.project}.{self.config.dataset}.{FIELDS_TABLE_NAME}`
        WHERE LOWER(TRIM(field_name)) = LOWER(TRIM(@field_name))
        """
        params = {"field_name": (field_name, "STRING")}

        if exclude_field_id is not None:
            query = base_query + " AND field_id != @exclude_field_id"
            params["exclude_field_id"] = (exclude_field_id, "INT64")
            return self._count_rows(query, params=params) > 0

        return self._count_rows(base_query, params=params) > 0

    def _hawk_id_exists(self, hawk_id: int) -> bool:
        query = f"""
        SELECT COUNT(1) AS cnt
        FROM `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        WHERE hawk_id = @hawk_id
        """
        return self._count_rows(query, params={"hawk_id": (hawk_id, "INT64")}) > 0

    def _ticker_exists(self, ticker: str, exclude_hawk_id: int | None = None) -> bool:
        base_query = f"""
        SELECT COUNT(1) AS cnt
        FROM `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        WHERE id_type = 'TICKER'
          AND UPPER(TRIM(value)) = UPPER(TRIM(@ticker))
        """
        params = {"ticker": (ticker, "STRING")}

        if exclude_hawk_id is not None:
            query = base_query + " AND hawk_id != @exclude_hawk_id"
            params["exclude_hawk_id"] = (exclude_hawk_id, "INT64")
            return self._count_rows(query, params=params) > 0

        return self._count_rows(base_query, params=params) > 0

    def _normalize_asset_class(self, asset_class: str) -> str:
        normalized = asset_class.strip().lower()
        valid_asset_classes = {a.value for a in AssetClass}
        if normalized not in valid_asset_classes:
            raise ValueError(
                f"asset_class must be one of: {', '.join(sorted(valid_asset_classes))}"
            )
        return normalized

    def _get_hawk_identifier(self, hawk_id: int) -> tuple[str, str, str] | None:
        """Get (id_type, value, asset_class) for a hawk_id, or None if not found."""
        query = f"""
        SELECT id_type, value, asset_class
        FROM `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        WHERE hawk_id = @hawk_id
        LIMIT 1
        """
        result = self.bq_client.execute_query(query, params={"hawk_id": (hawk_id, "INT64")})
        if result.empty:
            return None
        return (
            str(result.iloc[0]["id_type"]),
            str(result.iloc[0]["value"]),
            str(result.iloc[0]["asset_class"]),
        )

    def read_fields(self) -> DataFrame:
        """
        Read the fields table.

        :return: DataFrame with columns: field_id, field_name, field_type
        """
        query = f"""
        SELECT field_id, field_name, field_type
        FROM `{self.config.project}.{self.config.dataset}.{FIELDS_TABLE_NAME}`
        ORDER BY field_id
        """
        return self.bq_client.execute_query(query)

    def read_hawk_ids(self, id_type: str | None = None, asset_class: str | None = None) -> DataFrame:
        """
        Read the hawk_identifiers table, optionally filtered by identifier type and/or asset class.

        :param id_type: Optional identifier type (e.g., TICKER, FIGI, CUSIP)
        :param asset_class: Optional asset class (e.g., equities, futures)
        :return: DataFrame with columns: hawk_id, asset_class, id_type, value
        """
        query = f"""
        SELECT hawk_id, asset_class, id_type, value
        FROM `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        """
        where_clauses = []
        params: dict = {}

        if id_type:
            where_clauses.append("id_type = @id_type")
            params["id_type"] = (id_type, "STRING")

        if asset_class:
            normalized_asset_class = self._normalize_asset_class(asset_class)
            where_clauses.append("asset_class = @asset_class")
            params["asset_class"] = (normalized_asset_class, "STRING")

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY hawk_id"
        return self.bq_client.execute_query(query, params=params if params else None)

    def add_field(self, field_id: int, field_name: str, field_type: str) -> None:
        """
        Insert a row into the fields table.

        :param field_id: Unique field ID
        :param field_name: Field name
        :param field_type: Field type (int, double, char)
        """
        normalized_field_name = field_name.strip()
        normalized_field_type = field_type.strip().lower()

        if not normalized_field_name:
            raise ValueError("field_name cannot be empty")
        if normalized_field_type not in {"int", "double", "char"}:
            raise ValueError("field_type must be one of: int, double, char")

        if self._field_id_exists(field_id):
            raise ValueError(f"field_id already exists: {field_id}")
        if self._field_name_exists(normalized_field_name):
            raise ValueError(f"field_name already exists: {normalized_field_name}")

        query = f"""
        INSERT INTO `{self.config.project}.{self.config.dataset}.{FIELDS_TABLE_NAME}`
        (field_id, field_name, field_type)
        VALUES (@field_id, @field_name, @field_type)
        """
        params = {
            "field_id": (field_id, "INT64"),
            "field_name": (normalized_field_name, "STRING"),
            "field_type": (normalized_field_type, "STRING"),
        }
        self.bq_client.execute_query(query, params=params)

    def delete_field(self, field_id: int) -> None:
        """
        Delete a row from the fields table by field_id.

        :param field_id: Field ID to delete
        """
        query = f"""
        DELETE FROM `{self.config.project}.{self.config.dataset}.{FIELDS_TABLE_NAME}`
        WHERE field_id = @field_id
        """
        params = {"field_id": (field_id, "INT64")}
        self.bq_client.execute_query(query, params=params)

    def update_field(
        self,
        field_id: int,
        field_name: str | None = None,
        field_type: str | None = None,
        new_field_id: int | None = None,
    ) -> None:
        """
        Update a row in the fields table.

        :param field_id: Existing field_id to update
        :param field_name: Optional new field name
        :param field_type: Optional new field type (int, double, char)
        :param new_field_id: Optional new field_id
        """
        if not self._field_id_exists(field_id):
            raise ValueError(f"field_id does not exist: {field_id}")

        if field_name is None and field_type is None and new_field_id is None:
            raise ValueError("No updates provided for field")

        updates: list[str] = []
        params: dict = {"field_id": (field_id, "INT64")}

        if new_field_id is not None:
            if new_field_id != field_id and self._field_id_exists(new_field_id):
                raise ValueError(f"new field_id already exists: {new_field_id}")
            updates.append("field_id = @new_field_id")
            params["new_field_id"] = (new_field_id, "INT64")

        if field_name is not None:
            normalized_field_name = field_name.strip()
            if not normalized_field_name:
                raise ValueError("field_name cannot be empty")
            if self._field_name_exists(normalized_field_name, exclude_field_id=field_id):
                raise ValueError(f"field_name already exists: {normalized_field_name}")
            updates.append("field_name = @field_name")
            params["field_name"] = (normalized_field_name, "STRING")

        if field_type is not None:
            normalized_field_type = field_type.strip().lower()
            if normalized_field_type not in {"int", "double", "char"}:
                raise ValueError("field_type must be one of: int, double, char")
            updates.append("field_type = @field_type")
            params["field_type"] = (normalized_field_type, "STRING")

        query = f"""
        UPDATE `{self.config.project}.{self.config.dataset}.{FIELDS_TABLE_NAME}`
        SET {", ".join(updates)}
        WHERE field_id = @field_id
        """
        self.bq_client.execute_query(query, params=params)

    def add_hawk_id(self, hawk_id: int, asset_class: str, id_type: str, value: str) -> None:
        """
        Insert a row into the hawk_identifiers table.

        :param hawk_id: Hawk ID
        :param asset_class: Asset class (e.g., equities, futures)
        :param id_type: Identifier type (e.g., TICKER, FIGI)
        :param value: Identifier value
        """
        normalized_asset_class = self._normalize_asset_class(asset_class)
        normalized_id_type = id_type.strip().upper()
        normalized_value = value.strip()

        if not normalized_id_type:
            raise ValueError("id_type cannot be empty")
        if not normalized_value:
            raise ValueError("value cannot be empty")

        if self._hawk_id_exists(hawk_id):
            raise ValueError(f"hawk_id already exists: {hawk_id}")
        if normalized_id_type == "TICKER" and self._ticker_exists(normalized_value):
            raise ValueError(f"ticker already exists: {normalized_value}")

        query = f"""
        INSERT INTO `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        (hawk_id, asset_class, id_type, value)
        VALUES (@hawk_id, @asset_class, @id_type, @value)
        """
        params = {
            "hawk_id": (hawk_id, "INT64"),
            "asset_class": (normalized_asset_class, "STRING"),
            "id_type": (normalized_id_type, "STRING"),
            "value": (normalized_value, "STRING"),
        }
        self.bq_client.execute_query(query, params=params)

    def delete_hawk_id(self, hawk_id: int, id_type: str | None = None) -> None:
        """
        Delete rows from hawk_identifiers by hawk_id, optionally scoped by id_type.

        :param hawk_id: Hawk ID to delete
        :param id_type: Optional identifier type filter
        """
        base_query = f"""
        DELETE FROM `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        WHERE hawk_id = @hawk_id
        """
        params = {"hawk_id": (hawk_id, "INT64")}

        if id_type:
            query = base_query + " AND id_type = @id_type"
            params["id_type"] = (id_type, "STRING")
            self.bq_client.execute_query(query, params=params)
            return

        self.bq_client.execute_query(base_query, params=params)

    def update_hawk_id(
        self,
        hawk_id: int,
        asset_class: str | None = None,
        id_type: str | None = None,
        value: str | None = None,
        new_hawk_id: int | None = None,
    ) -> None:
        """
        Update a row in the hawk_identifiers table.

        :param hawk_id: Existing hawk_id to update
        :param asset_class: Optional new asset class
        :param id_type: Optional new identifier type
        :param value: Optional new identifier value
        :param new_hawk_id: Optional new hawk_id
        """
        if not self._hawk_id_exists(hawk_id):
            raise ValueError(f"hawk_id does not exist: {hawk_id}")

        if asset_class is None and id_type is None and value is None and new_hawk_id is None:
            raise ValueError("No updates provided for hawk identifier")

        updates: list[str] = []
        params: dict = {"hawk_id": (hawk_id, "INT64")}
        current_identifier = self._get_hawk_identifier(hawk_id)
        current_id_type, current_value = ("", "")
        if current_identifier is not None:
            current_id_type, current_value, _ = current_identifier

        if new_hawk_id is not None:
            if new_hawk_id != hawk_id and self._hawk_id_exists(new_hawk_id):
                raise ValueError(f"new hawk_id already exists: {new_hawk_id}")
            updates.append("hawk_id = @new_hawk_id")
            params["new_hawk_id"] = (new_hawk_id, "INT64")

        if asset_class is not None:
            normalized_asset_class = self._normalize_asset_class(asset_class)
            updates.append("asset_class = @asset_class")
            params["asset_class"] = (normalized_asset_class, "STRING")

        normalized_id_type = None
        if id_type is not None:
            normalized_id_type = id_type.strip().upper()
            if not normalized_id_type:
                raise ValueError("id_type cannot be empty")
            updates.append("id_type = @id_type")
            params["id_type"] = (normalized_id_type, "STRING")

            if normalized_id_type == "TICKER" and value is None:
                if self._ticker_exists(current_value, exclude_hawk_id=hawk_id):
                    raise ValueError(f"ticker already exists: {current_value}")

        if value is not None:
            normalized_value = value.strip()
            if not normalized_value:
                raise ValueError("value cannot be empty")

            ticker_type_after_update = normalized_id_type == "TICKER"
            if normalized_id_type is None:
                ticker_type_after_update = current_id_type.upper() == "TICKER"

            if ticker_type_after_update and self._ticker_exists(normalized_value, exclude_hawk_id=hawk_id):
                raise ValueError(f"ticker already exists: {normalized_value}")

            updates.append("value = @value")
            params["value"] = (normalized_value, "STRING")

        query = f"""
        UPDATE `{self.config.project}.{self.config.dataset}.{HAWK_IDENTIFIERS_TABLE_NAME}`
        SET {", ".join(updates)}
        WHERE hawk_id = @hawk_id
        """
        self.bq_client.execute_query(query, params=params)

    def trigger_backfill(
        self,
        hawk_id: int,
        pipeline: BackfillPipeline,
        wait_for_completion: bool = False,
        job_name: str = "hawk-backfill",
        region: str = "us-central1",
    ) -> str:
        """
        Trigger the Cloud Run backfill job.

        Equivalent command:
        gcloud run jobs execute <job_name> --region <region> --args="<environment>,<hawk_id>,<pipeline>"

        :param hawk_id: Hawk ID to backfill
        :param pipeline: Backfill pipeline enum (currently equities)
        :param wait_for_completion: If True, waits for completion; if False, returns immediately
        :param job_name: Cloud Run job name
        :param region: GCP region
        :return: Cloud Console URL to the execution when available, otherwise gcloud stdout
        """
        if hawk_id <= 0:
            raise ValueError("hawk_id must be a positive integer")

        args_value = f"{self.environment.lower},{hawk_id},{pipeline.value}"
        command = [
            "gcloud",
            "run",
            "jobs",
            "execute",
            job_name,
            "--region",
            region,
            f"--project={self.config.project}",
            f"--args={args_value}",
        ]

        if wait_for_completion:
            command.append("--wait")
        else:
            command.append("--async")

        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            stderr = result.stderr.strip()
            stdout = result.stdout.strip()
            error_message = stderr or stdout or "unknown gcloud error"
            raise RuntimeError(f"Failed to trigger backfill job: {error_message}")

        # gcloud stdout can be empty in some environments; resolve execution name directly.
        execution_name = None
        for _ in range(5):
            execution_name = self._latest_execution_name(job_name=job_name, region=region)
            if execution_name:
                break
            time.sleep(1)

        if execution_name:
            return (
                f"https://console.cloud.google.com/run/jobs/executions/details/"
                f"{region}/{execution_name}?project={self.config.project}"
            )

        return result.stdout.strip() or "Execution submitted, but execution URL could not be resolved."
