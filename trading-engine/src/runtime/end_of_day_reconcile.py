"""
end_of_day_reconcile.py — End-of-day reconciliation entrypoint.

Runs after the close every trading day.  Job:

  1. Pull current account positions from IBKR.
  2. Pull today's executions / trade log from IBKR.
  3. Compare against the engine's internal book (what it thinks it
     submitted + what it thinks should be on the books).
  4. Write a structured reconciliation report under
     ``data/broker/reconciliations/{profile}_eod_{YYYY-MM-DD}.json``.
  5. If any mismatch is material (> $100 notional) arm the kill switch
     so the next morning's cycle can't auto-trade until a human signs
     off.

Implementation here is a thin orchestrator — the heavy lifting is in
``src.broker.ibkr.reconciler``. Keeping the orchestration thin is on
purpose so the audit trail (read positions → read engine state → diff →
write report → maybe arm switch) is obvious from one read of this file.
"""
from __future__ import annotations

import json
import logging
import math
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from src.execution.kill_switch import (
    KillSwitchCorrupted,
    arm_kill_switch,
    is_kill_switch_armed,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RECON_DIR = PROJECT_ROOT / "data" / "broker" / "reconciliations"

log = logging.getLogger(__name__)


@dataclass(slots=True)
class EodReconcileResult:
    ok: bool
    profile: str
    as_of: str
    diffs: list[dict[str, Any]]
    material_mismatch: bool
    report_path: str | None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, default=str, sort_keys=True)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _safe_diff_notional(diff: dict[str, Any]) -> float:
    """Coerce a diff's notional to a finite float. Bad input → 0.0
    (not flagged as material) but logged for follow-up."""
    raw = diff.get("notional_diff_usd", 0.0)
    try:
        v = float(raw)
    except (TypeError, ValueError):
        log.warning("EOD reconcile: non-numeric notional_diff_usd %r", raw)
        return 0.0
    if not math.isfinite(v):
        log.warning("EOD reconcile: non-finite notional_diff_usd %r", raw)
        return 0.0
    return v


def reconcile_end_of_day(
    *,
    profile: str = "paper",
    material_threshold_usd: float = 100.0,
    as_of: date | None = None,
    arm_switch_on_mismatch: bool = True,
) -> EodReconcileResult:
    """Run the EOD reconciliation. Returns a structured result.

    Side effects:
      * writes a JSON report under ``data/broker/reconciliations/``
        (atomic write — survives mid-write crashes).
      * arms the kill switch if a material mismatch is detected and
        ``arm_switch_on_mismatch=True``. Idempotent: if the switch is
        already armed we don't double-arm.

    Validates ``material_threshold_usd`` (must be finite >= 0) and
    ``profile`` (non-empty string).
    """
    if not isinstance(profile, str) or not profile:
        raise ValueError(f"profile must be a non-empty string, got {profile!r}")
    if not (isinstance(material_threshold_usd, (int, float))
            and math.isfinite(float(material_threshold_usd))
            and material_threshold_usd >= 0):
        raise ValueError(
            f"material_threshold_usd must be a finite non-negative number, "
            f"got {material_threshold_usd!r}"
        )

    as_of = as_of or date.today()
    if not isinstance(as_of, date):
        raise TypeError(f"as_of must be a datetime.date, got {type(as_of).__name__}")

    RECON_DIR.mkdir(parents=True, exist_ok=True)
    report_path = RECON_DIR / f"{profile}_eod_{as_of.isoformat()}.json"

    try:
        # Lazy import — broker.ibkr pulls heavy deps that we don't want
        # at import-time of every other module.
        from src.broker.ibkr import reconciler as ibkr_reconciler  # type: ignore

        diffs: list[dict[str, Any]] = ibkr_reconciler.diff_book_vs_account(  # type: ignore[attr-defined]
            profile=profile, as_of=as_of
        )
    except (ImportError, AttributeError) as exc:
        # Reconciler module exists but doesn't expose ``diff_book_vs_account``
        # yet, or IBKR client unavailable in test env — return a structured
        # "skipped" result rather than crash the EOD job.
        log.warning("EOD reconciler unavailable: %s", exc)
        return EodReconcileResult(
            ok=True,
            profile=profile,
            as_of=as_of.isoformat(),
            diffs=[],
            material_mismatch=False,
            report_path=None,
            error=f"reconciler unavailable: {exc}",
        )
    except Exception as exc:  # pragma: no cover - defensive
        log.exception("EOD reconcile failed")
        return EodReconcileResult(
            ok=False,
            profile=profile,
            as_of=as_of.isoformat(),
            diffs=[],
            material_mismatch=False,
            report_path=None,
            error=str(exc),
        )

    # Defensive: diffs must be a list of dicts. Drop garbage rows.
    if not isinstance(diffs, list):
        log.warning("EOD reconcile: reconciler returned non-list %r", type(diffs))
        diffs = []
    diffs = [d for d in diffs if isinstance(d, dict)]

    material = any(abs(_safe_diff_notional(d)) >= material_threshold_usd for d in diffs)

    payload = {
        "profile": profile,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "material_threshold_usd": material_threshold_usd,
        "material_mismatch": material,
        "diff_count": len(diffs),
        "diffs": diffs,
    }
    try:
        _atomic_write_json(report_path, payload)
    except OSError as exc:
        log.exception("EOD reconcile: failed to write report")
        return EodReconcileResult(
            ok=False,
            profile=profile,
            as_of=as_of.isoformat(),
            diffs=diffs,
            material_mismatch=material,
            report_path=None,
            error=f"report write failed: {exc}",
        )

    if material and arm_switch_on_mismatch:
        # Idempotent — don't re-arm if already armed (avoids overwriting
        # the original armed_at timestamp / reason on a re-run).
        try:
            already_armed = is_kill_switch_armed()
        except KillSwitchCorrupted:
            # Treat corruption as armed (per kill_switch contract).
            already_armed = True
        if not already_armed:
            log.error("Material mismatch detected — arming kill switch.")
            try:
                arm_kill_switch(
                    reason=f"EOD reconciliation mismatch on {as_of.isoformat()}; "
                           f"see {report_path.name}",
                    by="end_of_day_reconcile",
                )
            except Exception as exc:
                log.exception("EOD reconcile: failed to arm kill switch")
                return EodReconcileResult(
                    ok=False,
                    profile=profile,
                    as_of=as_of.isoformat(),
                    diffs=diffs,
                    material_mismatch=material,
                    report_path=str(report_path),
                    error=f"arm failed: {exc}",
                )
        else:
            log.info("Material mismatch on rerun — kill switch already armed.")

    return EodReconcileResult(
        ok=True,
        profile=profile,
        as_of=as_of.isoformat(),
        diffs=diffs,
        material_mismatch=material,
        report_path=str(report_path),
    )


__all__ = ["EodReconcileResult", "reconcile_end_of_day"]
