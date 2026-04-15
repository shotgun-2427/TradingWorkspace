from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from src.dashboard.services import pipeline_service as svc


def test_append_ibkr_daily_wrapper(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    before = tmp_path / "before.csv"
    after = tmp_path / "after.csv"
    before.write_text("x", encoding="utf-8")
    after.write_text("y", encoding="utf-8")

    state = {"count": 0}

    def fake_latest_snapshot_path() -> Path:
        state["count"] += 1
        return before if state["count"] == 1 else after

    def fake_invoke_first(module_path: str, candidate_names: list[str], **kwargs):
        assert module_path == "src.production.pipeline.append_ibkr_daily"
        assert "append_ibkr_daily" in candidate_names
        assert kwargs["profile"] == "paper"
        return {"rows_added": 12}

    monkeypatch.setattr(svc, "latest_snapshot_path", fake_latest_snapshot_path)
    monkeypatch.setattr(svc, "_invoke_first", fake_invoke_first)

    result = svc.append_ibkr_daily(profile="paper")

    assert result["ok"] is True
    assert result["action"] == "append_ibkr_daily"
    assert result["profile"] == "paper"
    assert result["before_path"] == str(before)
    assert result["after_path"] == str(after)
    assert result["result"] == {"rows_added": 12}
    assert result["after_timestamp"] is not None


def test_refresh_targets_wrapper(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    before = tmp_path / "old_targets.parquet"
    after = tmp_path / "new_targets.parquet"
    before.write_text("x", encoding="utf-8")
    after.write_text("y", encoding="utf-8")

    state = {"count": 0}

    def fake_latest_targets_path() -> Path:
        state["count"] += 1
        return before if state["count"] == 1 else after

    def fake_invoke_first(module_path: str, candidate_names: list[str], **kwargs):
        assert module_path == "src.production.generate_targets"
        assert kwargs["profile"] == "paper"
        return {"targets_written": 22}

    monkeypatch.setattr(svc, "latest_targets_path", fake_latest_targets_path)
    monkeypatch.setattr(svc, "_invoke_first", fake_invoke_first)

    result = svc.refresh_targets(profile="paper")

    assert result["ok"] is True
    assert result["action"] == "generate_targets"
    assert result["before_path"] == str(before)
    assert result["after_path"] == str(after)
    assert result["result"] == {"targets_written": 22}


def test_build_paper_basket_wrapper_reports_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    basket_path = tmp_path / "paper_orders_only.parquet"
    basket_path.write_text("x", encoding="utf-8")

    df = pl.DataFrame(
        {
            "symbol": ["SPY", "QQQ"],
            "order_qty": [10, 5],
        }
    )

    def fake_latest_basket_path() -> Path:
        return basket_path

    def fake_load_latest_basket() -> pl.DataFrame:
        return df

    def fake_invoke_first(module_path: str, candidate_names: list[str], **kwargs):
        assert module_path == "src.production.runtime.build_paper_basket"
        assert kwargs["profile"] == "paper"
        return {"basket_written": True}

    monkeypatch.setattr(svc, "latest_basket_path", fake_latest_basket_path)
    monkeypatch.setattr(svc, "load_latest_basket", fake_load_latest_basket)
    monkeypatch.setattr(svc, "_invoke_first", fake_invoke_first)

    result = svc.build_paper_basket(profile="paper")

    assert result["ok"] is True
    assert result["action"] == "build_paper_basket"
    assert result["basket_rows"] == 2
    assert result["after_path"] == str(basket_path)
    assert result["result"] == {"basket_written": True}


def test_run_daily_prep_uses_primary_daily_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    basket_path = tmp_path / "paper_orders_only.csv"
    basket_path.write_text("x", encoding="utf-8")

    def fake_latest_basket_path() -> Path:
        return basket_path

    def fake_invoke_first(module_path: str, candidate_names: list[str], **kwargs):
        assert module_path == "src.runtime.daily_run"
        return {"daily_run": "ok"}

    monkeypatch.setattr(svc, "latest_basket_path", fake_latest_basket_path)
    monkeypatch.setattr(svc, "_invoke_first", fake_invoke_first)

    result = svc.run_daily_prep(profile="paper")

    assert result["ok"] is True
    assert result["action"] == "daily_prep"
    assert result["result"] == {"daily_run": "ok"}
    assert result["basket_path"] == str(basket_path)


def test_run_daily_prep_falls_back_to_steps(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    basket_path = tmp_path / "paper_orders_only.csv"
    basket_path.write_text("x", encoding="utf-8")

    def fake_latest_basket_path() -> Path:
        return basket_path

    def fake_invoke_first(module_path: str, candidate_names: list[str], **kwargs):
        if module_path == "src.runtime.daily_run":
            raise RuntimeError("daily run not ready")
        if module_path == "src.production.pipeline.append_ibkr_daily":
            return {"append": "ok"}
        if module_path == "src.production.generate_targets":
            return {"targets": "ok"}
        if module_path == "src.production.runtime.build_paper_basket":
            return {"basket": "ok"}
        raise AssertionError(f"unexpected module path: {module_path}")

    monkeypatch.setattr(svc, "latest_basket_path", fake_latest_basket_path)
    monkeypatch.setattr(svc, "_invoke_first", fake_invoke_first)

    result = svc.run_daily_prep(profile="paper")

    assert result["ok"] is True
    assert result["action"] == "daily_prep_fallback"
    assert len(result["steps"]) == 3
    assert result["errors"] == []
    assert result["basket_path"] == str(basket_path)


def test_get_pipeline_status(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    prices = tmp_path / "etf_prices_master.parquet"
    targets = tmp_path / "etf_targets_monthly.parquet"
    snapshot = tmp_path / "ibkr_daily_append_20260313_150000.csv"
    basket = tmp_path / "paper_orders_only.csv"
    submission = tmp_path / "paper_orders_submitted_20260313_150500.csv"
    fills = tmp_path / "paper_trade_log_20260313_150500.csv"

    for p in [prices, targets, snapshot, basket, submission, fills]:
        p.write_text("x", encoding="utf-8")

    monkeypatch.setattr(svc, "latest_prices_path", lambda: prices)
    monkeypatch.setattr(svc, "latest_targets_path", lambda: targets)
    monkeypatch.setattr(svc, "latest_snapshot_path", lambda: snapshot)
    monkeypatch.setattr(svc, "latest_basket_path", lambda: basket)
    monkeypatch.setattr(svc, "latest_submission_path", lambda: submission)
    monkeypatch.setattr(svc, "latest_fill_log_path", lambda: fills)

    status = svc.get_pipeline_status(profile="paper")

    assert status["profile"] == "paper"
    assert status["prices_path"] == str(prices)
    assert status["targets_path"] == str(targets)
    assert status["snapshot_path"] == str(snapshot)
    assert status["basket_path"] == str(basket)
    assert status["submission_path"] == str(submission)
    assert status["fill_log_path"] == str(fills)
    assert status["prices_timestamp"] is not None
    assert status["targets_timestamp"] is not None
    assert status["basket_timestamp"] is not None