from datetime import date

import pytest

from common.model import Config
from production.pipeline.validation import (
    validate_execution_config,
    validate_simulation_merged_config,
)


def _config(models: list[str], aggregators: list[str], optimizers: list[str] | None = None) -> Config:
    return Config(
        start_date=date(2024, 1, 1),
        end_date=date(2026, 12, 31),
        universe=["GLD-US"],
        model_state_features=["close_momentum_10"],
        models=models,
        aggregators=aggregators,
        optimizers=optimizers,
        cash_buffer=0.0,
        execution_portfolio=None,
        ib_gateway={
            "host": "127.0.0.1",
            "port": 4002,
            "client_id": 1,
            "flex_web_token": 1,
            "nav_flex_query_id": 1,
            "fund_inception_date": date(2024, 1, 1),
        },
        notifications={},
    )


def test_validate_execution_config_rejects_bad_shape() -> None:
    config = _config(models=["m1"], aggregators=["agg1", "agg2"], optimizers=["opt"])
    with pytest.raises(ValueError, match="exactly one aggregator"):
        validate_execution_config(config=config, profile="paper")


def test_validate_simulation_alignment_rejects_mismatch(monkeypatch) -> None:
    execution_config = _config(models=["m1"], aggregators=["agg"], optimizers=["opt"])
    simulation_config = _config(models=["m2"], aggregators=["agg"], optimizers=["opt"])
    monkeypatch.setattr(
        "production.pipeline.validation.load_execution_profile_config",
        lambda profile: execution_config,
    )
    with pytest.raises(ValueError, match="models mismatch"):
        validate_simulation_merged_config(config=simulation_config, profile="paper")


def test_validate_simulation_alignment_passes(monkeypatch) -> None:
    execution_config = _config(models=["m1"], aggregators=["agg"], optimizers=["opt"])
    simulation_config = _config(models=["m1"], aggregators=["agg"], optimizers=["opt"])
    monkeypatch.setattr(
        "production.pipeline.validation.load_execution_profile_config",
        lambda profile: execution_config,
    )
    validate_simulation_merged_config(config=simulation_config, profile="paper")
