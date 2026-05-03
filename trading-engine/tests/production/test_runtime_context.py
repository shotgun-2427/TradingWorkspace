import pytest

from production.runtime.context import (
    RuntimeContextError,
    load_execution_context,
    load_simulation_context,
)


def _clear_runtime_env(monkeypatch) -> None:
    for key in [
        "RUN_MODE",
        "PIPELINE_PROFILE",
        "PIPELINE_MODE",
        "SIMULATION_PROFILE",
        "GITHUB_ACTIONS",
    ]:
        monkeypatch.delenv(key, raising=False)


def test_invalid_run_mode_fails(monkeypatch) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("RUN_MODE", "prodish")
    with pytest.raises(RuntimeContextError, match="Invalid RUN_MODE"):
        load_execution_context()


def test_invalid_pipeline_profile_fails(monkeypatch) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_PROFILE", "staging")
    with pytest.raises(RuntimeContextError, match="Invalid PIPELINE_PROFILE"):
        load_execution_context()


def test_invalid_simulation_profile_fails(monkeypatch) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("SIMULATION_PROFILE", "staging")
    with pytest.raises(RuntimeContextError, match="Invalid SIMULATION_PROFILE"):
        load_simulation_context()


def test_production_requires_github_actions(monkeypatch) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("RUN_MODE", "production")
    monkeypatch.setenv("PIPELINE_PROFILE", "paper")
    with pytest.raises(RuntimeContextError, match="requires GITHUB_ACTIONS=true"):
        load_execution_context()


def test_legacy_pipeline_mode_maps_to_profile(monkeypatch) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_MODE", "live")
    context = load_execution_context()
    assert context.profile == "live"
    assert context.run_mode == "local"


def test_legacy_pipeline_mode_all_defaults_to_paper_in_local(monkeypatch) -> None:
    _clear_runtime_env(monkeypatch)
    monkeypatch.setenv("PIPELINE_MODE", "all")
    context = load_execution_context()
    assert context.profile == "paper"
