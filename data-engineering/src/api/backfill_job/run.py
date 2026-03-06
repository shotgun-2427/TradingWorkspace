"""
Cloud Run backfill entrypoint for Hawk identifiers.
"""
from __future__ import annotations

import argparse
import datetime

from api.enums import BackfillPipeline
from pipeline.common.enums import Environment, WriteMode
from pipeline.pipeline import Pipeline
from pipeline.sources.factset_equities_ohlcv import FactsetEquitiesOhlcvSource


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Job")
    parser.add_argument("environment", type=str, help="Environment (production/development)")
    parser.add_argument("hawk_id", type=int, help="Hawk identifier")
    parser.add_argument("asset_class", type=str, help="Asset class (equities)")
    return parser.parse_args()


def run_equities_backfill(environment: Environment, hawk_id: int) -> None:
    start_date = "1980-01-01"
    end_date = datetime.date.today().strftime("%Y-%m-%d")

    pipeline = Pipeline(
        source=FactsetEquitiesOhlcvSource(environment=environment, interval="1d"),
        write_mode=WriteMode.BIGQUERY,
    )
    pipeline.run(
        start_date=start_date,
        end_date=end_date,
        securities=[hawk_id],
    )


def main() -> None:
    args = parse_args()
    environment = Environment(args.environment.upper())
    asset_class = args.asset_class.strip().lower()

    if asset_class != BackfillPipeline.EQUITIES.value:
        raise ValueError(f"Unsupported asset_class for this job: {asset_class}")

    run_equities_backfill(environment=environment, hawk_id=args.hawk_id)


if __name__ == "__main__":
    main()

