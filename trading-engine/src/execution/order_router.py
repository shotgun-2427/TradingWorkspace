"""
order_router.py — Single point of egress for all broker orders.

Does the smallest amount of work possible to make order submission
auditable and idempotent:

  1. Read kill switch (abort if armed). Fails CLOSED on corrupt state.
  2. Apply order policy (allow/reject each ticket).
  3. Compute an idempotency key per ticket.
  4. Detect within-batch idempotency-key collisions and refuse to
     double-submit (the policy is "first wins, second is recorded as
     rejected with reason 'duplicate'").
  5. Persist a "pre_submit" record BEFORE sending to the broker so a
     crash mid-call doesn't lose the trace. Persistence is atomic
     (tmp + rename); a write failure does NOT block submission — the
     broker must still see the order — but is logged loudly.
  6. Hand off to ``broker.ibkr.orders.submit`` (or the provided sink).
  7. Persist the broker's response.

Idempotency key formula: ``sha1(symbol|qty|price|YYYY-MM-DD)`` — stable
across retries within the same trading day, different across days.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import date as _date, datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from src.execution.kill_switch import KillSwitchTripped, require_kill_switch_clear
from src.execution.order_policy import (
    DEFAULT_ORDER_POLICY,
    OrderPolicy,
    OrderTicket,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SUBMISSION_LOG_DIR = PROJECT_ROOT / "artifacts" / "runs" / "submissions"

log = logging.getLogger(__name__)


@dataclass(slots=True)
class RouterDecision:
    ticket: OrderTicket
    idempotency_key: str
    submitted: bool
    rejected_reason: str | None = None
    broker_response: dict[str, Any] | None = None


def idempotency_key(ticket: OrderTicket, *, on_date: _date | None = None) -> str:
    """Stable per-trading-day key for a ticket."""
    if not isinstance(ticket.symbol, str) or not ticket.symbol:
        raise ValueError("ticket.symbol must be a non-empty string for idempotency")
    if not isinstance(ticket.qty, int):
        raise TypeError(f"ticket.qty must be int, got {type(ticket.qty).__name__}")
    on_date = on_date or _date.today()
    raw = f"{ticket.symbol}|{ticket.qty}|{ticket.price:.6f}|{on_date.isoformat()}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _atomic_write(path: Path, payload: str) -> None:
    """Atomic write: tmp file + os.replace. Survives mid-write crashes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(payload)
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass  # Some test filesystems don't support fsync.
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _persist(record: dict[str, Any], *, log_dir: Path | None = None) -> bool:
    """Write a submission record atomically. Returns True on success.

    Audit-log write failures are logged but do NOT raise — losing an
    audit row is bad, but blocking a broker call because the disk is
    full would be worse. The caller logs the failure for follow-up.
    """
    target_dir = log_dir or SUBMISSION_LOG_DIR
    try:
        p = target_dir / f"{record['idempotency_key']}.json"
        payload = json.dumps(record, indent=2, default=str, sort_keys=True)
        _atomic_write(p, payload)
        return True
    except Exception as exc:
        log.error(
            "order_router: audit-log write failed for key=%s err=%s",
            record.get("idempotency_key"), exc,
        )
        return False


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def submit_orders(
    tickets: Iterable[OrderTicket],
    *,
    policy: OrderPolicy | None = None,
    sink: Callable[[OrderTicket], dict[str, Any]] | None = None,
    on_date: _date | None = None,
    dry_run: bool = False,
    log_dir: Path | None = None,
) -> list[RouterDecision]:
    """Route a batch of order tickets.

    Parameters
    ----------
    sink : optional callable; if not provided we route to
           ``broker.ibkr.orders.submit_one`` (lazily imported).
           Tests use a fake sink.
    log_dir : optional override for audit-log location (tests use this).
    """
    policy = policy or DEFAULT_ORDER_POLICY
    decisions: list[RouterDecision] = []
    # Materialise once — caller might pass a one-shot iterator and we
    # iterate it twice (once for the kill-switch reject loop).
    tickets = list(tickets)

    # Kill-switch check — fails closed on corrupt state.
    try:
        require_kill_switch_clear()
    except KillSwitchTripped as exc:
        for t in tickets:
            try:
                key = idempotency_key(t, on_date=on_date)
            except (ValueError, TypeError):
                # Invalid ticket; still produce a decision so the caller
                # gets the full audit trail.
                key = "invalid_ticket"
            decisions.append(RouterDecision(
                ticket=t,
                idempotency_key=key,
                submitted=False,
                rejected_reason=f"kill switch: {exc}",
            ))
        return decisions

    seen_keys: set[str] = set()
    for t in tickets:
        # Pre-validate the ticket shape so a malformed batch member
        # doesn't poison the rest of the run.
        try:
            key = idempotency_key(t, on_date=on_date)
        except (ValueError, TypeError) as exc:
            decisions.append(RouterDecision(
                ticket=t, idempotency_key="invalid_ticket",
                submitted=False, rejected_reason=f"invalid ticket: {exc}",
            ))
            continue

        if key in seen_keys:
            # Duplicate within batch — refuse to double-submit.
            decisions.append(RouterDecision(
                ticket=t, idempotency_key=key, submitted=False,
                rejected_reason="duplicate idempotency key in batch",
            ))
            continue
        seen_keys.add(key)

        decision = policy.evaluate(t)
        if not decision.accept:
            decisions.append(RouterDecision(t, key, False, decision.reason))
            _persist({"idempotency_key": key, "ticket": asdict(t),
                      "submitted": False, "reason": decision.reason,
                      "ts": _utc_now_iso()}, log_dir=log_dir)
            continue

        if dry_run:
            # Even dry-run gets an audit row, so operators can see what
            # would have happened.
            _persist({"idempotency_key": key, "ticket": asdict(t),
                      "submitted": False, "reason": "dry_run",
                      "ts": _utc_now_iso()}, log_dir=log_dir)
            decisions.append(RouterDecision(t, key, False, "dry_run"))
            continue

        # Persist BEFORE the broker call (so a crash leaves a trace).
        # If the audit write fails we still proceed — _persist logs the
        # failure and returns False.
        _persist({"idempotency_key": key, "ticket": asdict(t),
                  "submitted": True, "stage": "pre_submit",
                  "ts": _utc_now_iso()}, log_dir=log_dir)

        try:
            if sink is None:
                from src.broker.ibkr import orders as ibkr_orders  # type: ignore
                response = ibkr_orders.submit_one(t)  # type: ignore[attr-defined]
            else:
                response = sink(t)
        except Exception as exc:
            response = {"ok": False, "error": str(exc)}
            log.exception("Order router: broker raised on %s", t.symbol)

        # Defensive: response must be a dict so callers don't crash.
        if not isinstance(response, dict):
            response = {"ok": False, "error": f"non-dict broker response: {response!r}"}

        decisions.append(RouterDecision(t, key, True, broker_response=response))
        _persist({"idempotency_key": key, "ticket": asdict(t),
                  "submitted": True, "stage": "post_submit",
                  "broker_response": response,
                  "ts": _utc_now_iso()}, log_dir=log_dir)

    return decisions


__all__ = [
    "RouterDecision",
    "idempotency_key",
    "submit_orders",
]
