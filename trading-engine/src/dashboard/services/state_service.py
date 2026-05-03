"""
state_service.py — Persistent dashboard state helpers.

Streamlit's ``st.session_state`` only survives a single browser session. This
service persists a small set of flags and timestamps to a JSON file on disk so
important state (submit lock, last pipeline run, last submit time, etc.)
survives Streamlit restarts and tab reloads.

The file lives at ``artifacts/runs/<profile>_dashboard_state.json``.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

_TRADING_ENGINE_ROOT = Path(__file__).resolve().parents[3]
_STATE_DIR = _TRADING_ENGINE_ROOT / "artifacts" / "runs"

_DEFAULT_STATE: dict[str, Any] = {
    "submit_locked": False,
    "last_pipeline_run": None,
    "last_submit_time": None,
    "last_broker_refresh": None,
    "last_pipeline_action": None,
    "last_submit_action": None,
    "last_error": None,
    "active_run_id": None,
}


def _state_path(profile: str = "paper") -> Path:
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    return _STATE_DIR / f"{profile}_dashboard_state.json"


def load_state(profile: str = "paper") -> dict[str, Any]:
    """Load persisted state merged with defaults for missing keys."""
    path = _state_path(profile)
    state = dict(_DEFAULT_STATE)
    if not path.exists():
        return state
    try:
        stored = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return state
    if isinstance(stored, dict):
        state.update(stored)
    return state


def save_state(state: dict[str, Any], profile: str = "paper") -> Path:
    """Persist the provided state dict to disk."""
    path = _state_path(profile)
    sanitized = {key: value for key, value in state.items() if _is_json_safe(value)}
    path.write_text(json.dumps(sanitized, indent=2), encoding="utf-8")
    return path


def update_state(profile: str = "paper", **kwargs: Any) -> dict[str, Any]:
    """Patch the persisted state with the supplied key/value pairs."""
    state = load_state(profile)
    state.update(kwargs)
    save_state(state, profile=profile)
    return state


def reset_state(profile: str = "paper") -> dict[str, Any]:
    """Reset persisted state to defaults."""
    state = dict(_DEFAULT_STATE)
    save_state(state, profile=profile)
    return state


def hydrate_session_state(st_session_state: Any, profile: str = "paper") -> None:
    """Pull persisted state into a Streamlit session.

    Only missing keys are populated so the caller can still override values
    interactively. This is typically called at the top of the entrypoint.
    """
    state = load_state(profile)
    for key, value in state.items():
        if key not in st_session_state:
            st_session_state[key] = value


def snapshot_event(
    event: str,
    profile: str = "paper",
    **extra: Any,
) -> dict[str, Any]:
    """Record a timestamped event and update the corresponding last_* field."""
    now_iso = datetime.now().isoformat(timespec="seconds")
    field = _event_field(event)
    payload: dict[str, Any] = {"event": event, "timestamp": now_iso}
    payload.update({k: v for k, v in extra.items() if _is_json_safe(v)})
    if field is not None:
        return update_state(profile=profile, **{field: now_iso, **payload})
    return update_state(profile=profile, **payload)


def _event_field(event: str) -> str | None:
    mapping = {
        "pipeline": "last_pipeline_run",
        "submit": "last_submit_time",
        "broker_refresh": "last_broker_refresh",
    }
    return mapping.get(event.lower())


def _is_json_safe(value: Any) -> bool:
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False
