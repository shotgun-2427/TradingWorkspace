"""
Pipeline-service tests.

``pipeline_service`` was simplified to thin wrappers around the underlying
production modules — it no longer exposes the ``latest_*_path`` helpers or
the ``_invoke_first`` indirection the older test suite was written against.
Only the still-valid surface (``get_pipeline_status``) is exercised here;
the wrapper functions are integration-tested elsewhere via the production
modules they delegate to.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.dashboard.services import pipeline_service as svc


def test_pipeline_service_exposes_expected_api() -> None:
    """The dashboard depends on these names — fail loudly if they go away."""
    for name in ("get_pipeline_status", "append_ibkr_daily", "refresh_targets", "run_daily_pipeline"):
        assert hasattr(svc, name), f"pipeline_service is missing {name!r}"


def test_get_pipeline_status_returns_canonical_shape(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """``get_pipeline_status`` should always return the canonical dict shape
    with the expected keys, even when nothing on disk exists yet."""
    # Force the project root to a clean tmp dir so no real artifacts appear.
    monkeypatch.setattr(svc, "_project_root", lambda: tmp_path)
    status = svc.get_pipeline_status(profile="paper")
    assert status["profile"] == "paper"
    for key in (
        "prices_path",
        "targets_path",
        "snapshot_path",
        "basket_path",
        "submission_path",
        "fill_log_path",
        "prices_timestamp",
        "targets_timestamp",
        "snapshot_timestamp",
        "basket_timestamp",
        "submission_timestamp",
        "fill_log_timestamp",
    ):
        assert key in status, f"missing canonical key {key!r}"
    # All paths should be None on an empty filesystem.
    assert status["prices_path"] is None
    assert status["targets_path"] is None


def test_append_ibkr_daily_returns_error_dict_when_module_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The wrapper is meant to never raise — it should swallow ImportError /
    runtime errors and return ``{"ok": False, "error": ...}``."""

    # Force the lazy import inside the wrapper to fail.
    def _bad_import(*_args, **_kwargs):  # noqa: ANN001
        raise RuntimeError("simulated import failure")

    # Patch sys.modules to make the import raise.
    import sys

    monkeypatch.setitem(
        sys.modules,
        "src.production.pipeline.append_ibkr_daily",
        type("M", (), {"append_ibkr_daily": _bad_import})(),
    )

    result = svc.append_ibkr_daily(profile="paper")
    assert result["ok"] is False
    assert "error" in result
    assert result["action"] == "append_ibkr_daily"
