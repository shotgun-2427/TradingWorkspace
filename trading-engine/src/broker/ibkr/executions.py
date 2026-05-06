"""
executions.py — Read recent fills/executions from IBKR.

Thin reader. ``execution.fill_monitor.list_recent_fills`` calls
``list_executions(profile, since)`` and expects an iterable of dicts
with at minimum: order_id, symbol, qty, price, ts.

Connection is opened on demand (paper profile by default) and closed
on exit, so the function is safe to call from short-lived jobs. Tests
inject a fake client to avoid the network.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .client import IBKRClient, IBKRConnectionConfig

log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_BROKER_CONFIG_PATH = _PROJECT_ROOT / "config" / "broker.yaml"

# Offset added to the configured paper client_id when opening a
# dedicated read-only connection so we don't collide with a live
# order-submitting client_id on the same gateway.
_READ_CLIENT_ID_OFFSET = 50

_DEFAULT_BROKER_CONFIG: dict[str, Any] = {
    "host": "127.0.0.1",
    "port": 4004,
    "client_id": 21,
    "readonly": True,
    "account": None,
}


def _broker_profile_config(profile: str) -> dict[str, Any]:
    """Read the profile section from ``config/broker.yaml``.

    Falls back to defaults if the file or the ``ibkr.<profile>`` block
    is missing — keeps the module importable in unit-test envs without
    the repo's config dir.
    """
    try:
        import yaml  # local import — keeps top-level import light
    except ImportError:
        return dict(_DEFAULT_BROKER_CONFIG)

    try:
        with _BROKER_CONFIG_PATH.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except FileNotFoundError:
        return dict(_DEFAULT_BROKER_CONFIG)
    except Exception as exc:  # noqa: BLE001 — config corruption shouldn't crash imports
        log.warning("executions: broker.yaml read failed: %s", exc)
        return dict(_DEFAULT_BROKER_CONFIG)

    ibkr = (data.get("ibkr") or {})
    profile_cfg = (ibkr.get(profile) or {})
    return {
        "host": ibkr.get("host", _DEFAULT_BROKER_CONFIG["host"]),
        "port": int(profile_cfg.get("port", _DEFAULT_BROKER_CONFIG["port"])),
        "client_id": int(profile_cfg.get("client_id", _DEFAULT_BROKER_CONFIG["client_id"])),
        "readonly": bool(profile_cfg.get("readonly", _DEFAULT_BROKER_CONFIG["readonly"])),
        "account": profile_cfg.get("account") or None,
    }


def _make_readonly_client(profile: str, *, client_id_offset: int = _READ_CLIENT_ID_OFFSET) -> IBKRClient:
    cfg = _broker_profile_config(profile)
    return IBKRClient(IBKRConnectionConfig(
        host=cfg["host"],
        port=cfg["port"],
        client_id=int(cfg["client_id"]) + int(client_id_offset),
        readonly=True,  # forced — list_executions never writes
        account=cfg["account"],
    ))


def _normalize_fill(fill: Any) -> dict[str, Any] | None:
    """Coerce one ``ib_async.Fill`` into our flat dict shape, or None on bad input."""
    try:
        execution = getattr(fill, "execution", None)
        contract = getattr(fill, "contract", None)
        if execution is None or contract is None:
            return None

        symbol = getattr(contract, "symbol", None)
        shares = getattr(execution, "shares", None)
        price = getattr(execution, "price", None)
        if symbol is None or shares is None or price is None:
            return None

        # Sign the qty so downstream code can sum cumulatively.
        side = getattr(execution, "side", "") or ""
        try:
            qty = int(round(float(shares)))
        except (TypeError, ValueError):
            return None
        if isinstance(side, str) and side.strip().upper().startswith("S"):
            qty = -qty

        ts_raw = getattr(execution, "time", None)
        if isinstance(ts_raw, datetime):
            ts_iso = ts_raw.isoformat()
        elif ts_raw is not None:
            ts_iso = str(ts_raw)
        else:
            ts_iso = datetime.now(timezone.utc).isoformat()

        order_id = getattr(execution, "orderId", None)
        return {
            "order_id": str(order_id) if order_id is not None else "",
            "exec_id": str(getattr(execution, "execId", "") or ""),
            "symbol": str(symbol).strip().upper(),
            "qty": qty,
            "price": float(price),
            "side": str(side),
            "ts": ts_iso,
        }
    except Exception as exc:  # noqa: BLE001 — never blow up on a single bad fill
        log.warning("executions: failed to normalize fill: %s", exc)
        return None


def _parse_iso_ts(value: str) -> datetime | None:
    """Parse an ISO-8601 timestamp; return None on failure."""
    if not isinstance(value, str) or not value:
        return None
    raw = value.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def list_executions(
    *,
    profile: str = "paper",
    since: datetime | None = None,
    client: IBKRClient | None = None,
) -> list[dict[str, Any]]:
    """Return executions for ``profile`` since ``since``.

    Pulls via ``IB.fills()``. The caller may inject a connected
    ``client`` for tests or to share an existing connection. If
    ``client`` is None we open a short-lived read-only connection,
    drain fills, and disconnect.

    On any connection failure we log and return ``[]`` rather than
    raising — fill_monitor treats this module as best-effort and the
    EOD job has its own retry path.
    """
    owns_client = client is None
    if client is None:
        try:
            client = _make_readonly_client(profile)
            client.connect()
        except Exception as exc:  # noqa: BLE001 — broker down ≠ caller's problem
            log.warning("executions: IBKR connect failed (%s); returning []", exc)
            return []

    try:
        try:
            raw_fills = list(client.ib.fills())
        except Exception as exc:  # noqa: BLE001
            log.warning("executions: ib.fills() failed: %s", exc)
            return []
    finally:
        if owns_client:
            try:
                client.disconnect()
            except Exception:  # noqa: BLE001
                pass

    out: list[dict[str, Any]] = []
    for fill in raw_fills:
        norm = _normalize_fill(fill)
        if norm is None:
            continue
        if since is not None:
            fill_ts = _parse_iso_ts(norm["ts"])
            if fill_ts is not None:
                # Reconcile naive/aware mismatches before comparing.
                if fill_ts.tzinfo is not None and since.tzinfo is None:
                    fill_ts = fill_ts.replace(tzinfo=None)
                elif fill_ts.tzinfo is None and since.tzinfo is not None:
                    fill_ts = fill_ts.replace(tzinfo=since.tzinfo)
                if fill_ts < since:
                    continue
        out.append(norm)
    return out


__all__ = ["list_executions"]
