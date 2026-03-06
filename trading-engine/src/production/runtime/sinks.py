from __future__ import annotations

import asyncio
import dataclasses
import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from common.async_gcs_writer import AsyncGCSWriter
from common.logging import setup_logger
from common.otel import timed
from common.utils import post_to_teams
from production.runtime.context import RuntimeContext

if TYPE_CHECKING:
    from common.model import Config

logger = setup_logger(__name__)

BUCKET_NAME = "wsb-hc-qasap-bucket-1"


class ArtifactSink(ABC):
    @abstractmethod
    async def save_polars(self, frame: pl.DataFrame | pl.LazyFrame, file_name: str) -> None:
        """Persist a polars frame artifact."""
        pass

    @abstractmethod
    async def save_text(
        self,
        text: str,
        file_name: str,
        *,
        content_type: str = "text/plain",
    ) -> None:
        """Persist a text artifact."""
        pass

    @abstractmethod
    async def save_config(self, config: "Config", file_name: str = "config.json") -> None:
        """Persist a serialized config artifact."""
        pass

    @abstractmethod
    def object_reference(self, file_name: str) -> str:
        """Return a retrievable reference to a saved artifact."""
        pass

    @abstractmethod
    async def flush(self) -> None:
        """Flush pending writes."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close sink resources."""
        pass


class GcsSink(ArtifactSink):
    def __init__(self, *, bucket_name: str, prefix: str):
        """Initialize a GCS-backed artifact sink."""
        self.bucket_name = bucket_name
        self.prefix = prefix
        self._writer = AsyncGCSWriter(bucket_name=bucket_name, prefix=prefix)
        logger.info("Using GCS sink at gs://%s/%s", bucket_name, prefix)

    async def save_polars(self, frame: pl.DataFrame | pl.LazyFrame, file_name: str) -> None:
        """Write a dataframe artifact to GCS."""
        await self._writer.save_polars(frame, file_name)

    async def save_text(
        self,
        text: str,
        file_name: str,
        *,
        content_type: str = "text/plain",
    ) -> None:
        """Write a text artifact to GCS."""
        await self._writer.save_text(text, file_name, content_type=content_type)

    async def save_config(self, config: "Config", file_name: str = "config.json") -> None:
        """Serialize and write the runtime config to GCS."""
        json_string = json.dumps(dataclasses.asdict(config), default=str, indent=4)
        await self.save_text(
            json_string,
            file_name,
            content_type="application/json; charset=utf-8",
        )

    def object_reference(self, file_name: str) -> str:
        """Return a browser-accessible URL for an artifact."""
        return f"https://storage.cloud.google.com/{self.bucket_name}/{self.prefix}/{file_name}"

    async def flush(self) -> None:
        """Flush pending GCS uploads."""
        await self._writer.flush()

    async def close(self) -> None:
        """Flush and close the underlying GCS writer."""
        await self._writer.close()


class LocalSink(ArtifactSink):
    def __init__(self, output_dir: Path):
        """Initialize a local filesystem artifact sink."""
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Using local sink at %s", self.output_dir)

    async def save_polars(self, frame: pl.DataFrame | pl.LazyFrame, file_name: str) -> None:
        """Write a dataframe artifact to local disk."""
        output_path = self.output_dir / file_name
        await asyncio.to_thread(self._write_polars, frame, output_path)

    @staticmethod
    def _write_polars(frame: pl.DataFrame | pl.LazyFrame, output_path: Path) -> None:
        """Synchronously write a dataframe artifact to disk."""
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        frame.write_csv(output_path)

    async def save_text(
        self,
        text: str,
        file_name: str,
        *,
        content_type: str = "text/plain",
    ) -> None:
        """Write a text artifact to local disk."""
        _ = content_type
        output_path = self.output_dir / file_name
        await asyncio.to_thread(output_path.write_text, text, "utf-8")

    async def save_config(self, config: "Config", file_name: str = "config.json") -> None:
        """Serialize and write the runtime config to local disk."""
        output_path = self.output_dir / file_name
        json_string = json.dumps(dataclasses.asdict(config), default=str, indent=4)
        await asyncio.to_thread(output_path.write_text, json_string, "utf-8")

    def object_reference(self, file_name: str) -> str:
        """Return the absolute local path for an artifact."""
        return str((self.output_dir / file_name).resolve())

    async def flush(self) -> None:
        """No-op flush for local sink."""
        return None

    async def close(self) -> None:
        """No-op close for local sink."""
        return None


class NotificationSink(ABC):
    @abstractmethod
    def send(self, message: str) -> None:
        """Send a notification message."""
        pass


class TeamsSink(NotificationSink):
    def __init__(self, webhook_url: str):
        """Initialize a Teams notification sink."""
        self.webhook_url = webhook_url

    def send(self, message: str) -> None:
        """Send a message to Microsoft Teams."""
        post_to_teams(webhook_url=self.webhook_url, message=message)


class NoopNotificationSink(NotificationSink):
    def __init__(self, reason: str):
        """Initialize a no-op notification sink."""
        self.reason = reason

    def send(self, message: str) -> None:
        """Skip notification delivery and log the reason."""
        _ = message
        logger.info("Skipping notification sink: %s", self.reason)


class ExecutionSink(ABC):
    @abstractmethod
    async def run(
        self,
        *,
        config: "Config",
        artifact_sink: ArtifactSink,
        prices: pl.DataFrame,
        portfolio_insight: pl.DataFrame,
    ) -> str | None:
        """Execute downstream brokerage actions for a portfolio."""
        pass


class IbkrExecutionSink(ExecutionSink):
    async def run(
        self,
        *,
        config: "Config",
        artifact_sink: ArtifactSink,
        prices: pl.DataFrame,
        portfolio_insight: pl.DataFrame,
    ) -> str | None:
        """Generate execution artifacts and return basket location."""
        from common.interactive_brokers import IBKR
        from production.pipeline.core import (
            construct_goal_positions,
            construct_rebalance_orders,
            to_ibkr_basket_csv,
        )

        ib_client = await IBKR.create(
            hostname=config.ib_gateway.host,
            port=config.ib_gateway.port,
            flex_web_token=config.ib_gateway.flex_web_token,
            nav_flex_query_id=config.ib_gateway.nav_flex_query_id,
            fund_inception_date=config.ib_gateway.fund_inception_date,
            client_id=config.ib_gateway.client_id,
        )
        try:
            with timed("production.goal_positions_duration"):
                goal_positions = construct_goal_positions(
                    ib_client=ib_client,
                    insights=portfolio_insight,
                    prices=prices,
                    universe=config.universe,
                    cash_buffer_pct=float(config.cash_buffer),
                )
            await artifact_sink.save_polars(goal_positions, "goal_positions.csv")

            with timed("production.rebalance_orders_duration"):
                rebalance_df = construct_rebalance_orders(
                    ib_client=ib_client,
                    targets=goal_positions,
                    universe=config.universe,
                    close_out_outside_universe=True,
                )
            await artifact_sink.save_polars(rebalance_df, "rebalance_orders.csv")

            basket_csv = to_ibkr_basket_csv(
                rebalance_df,
                order_type="MOC",
                time_in_force="DAY",
                exchange="SMART",
            )
            await artifact_sink.save_polars(ib_client.get_historical_nav(), "historical_nav.csv")
            await artifact_sink.save_text(
                basket_csv,
                "ibkr_basket_moc.csv",
                content_type="text/csv; charset=utf-8",
            )
            return artifact_sink.object_reference("ibkr_basket_moc.csv")
        finally:
            ib_client.cleanup()


class NoopExecutionSink(ExecutionSink):
    async def run(
        self,
        *,
        config: "Config",
        artifact_sink: ArtifactSink,
        prices: pl.DataFrame,
        portfolio_insight: pl.DataFrame,
    ) -> str | None:
        """Skip execution actions in local mode."""
        _ = config, artifact_sink, prices, portfolio_insight
        logger.info("Skipping IBKR execution sink in local mode.")
        return None


def build_gcs_prefix(*, pipeline_kind: str, profile: str, current_date: str) -> str:
    """Build the canonical production GCS prefix for a pipeline run."""
    if pipeline_kind == "pipeline":
        return f"hcf/{profile}/production_audit/{current_date}"
    if pipeline_kind == "simulations":
        return f"hcf/{profile}/simulations_audit/{current_date}"
    raise ValueError(f"Unsupported pipeline_kind '{pipeline_kind}'")


def build_artifact_sink(context: RuntimeContext) -> ArtifactSink:
    """Select artifact sink implementation based on runtime mode."""
    if context.side_effects_enabled:
        return GcsSink(
            bucket_name=BUCKET_NAME,
            prefix=build_gcs_prefix(
                pipeline_kind=context.pipeline_kind,
                profile=context.profile,
                current_date=context.current_date,
            ),
        )

    local_root = Path(os.environ.get("LOCAL_ARTIFACT_ROOT", "local_artifacts"))
    output_dir = local_root / context.pipeline_kind / context.profile / context.current_date
    return LocalSink(output_dir=output_dir)


def build_notification_sink(
    context: RuntimeContext,
    webhook_url: str | None,
) -> NotificationSink:
    """Select notification sink implementation based on runtime mode."""
    if not context.side_effects_enabled:
        return NoopNotificationSink(reason="RUN_MODE=local")
    if not webhook_url:
        return NoopNotificationSink(reason="No Teams webhook configured")
    return TeamsSink(webhook_url=webhook_url)


def build_execution_sink(context: RuntimeContext) -> ExecutionSink:
    """Select execution sink implementation based on runtime mode."""
    if context.side_effects_enabled:
        return IbkrExecutionSink()
    return NoopExecutionSink()
