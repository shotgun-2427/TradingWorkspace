from __future__ import annotations

import importlib
import inspect
from typing import Any, Callable


def _resolve_legacy_callable() -> Callable[..., Any]:
    mod = importlib.import_module("src.production.pipeline.append_indices_daily")

    candidate_names = [
        "append_indices_daily",
        "run",
        "main",
    ]

    for name in candidate_names:
        fn = getattr(mod, name, None)
        if callable(fn):
            return fn

    raise AttributeError(
        "Could not find a callable in src.production.pipeline.append_indices_daily. "
        f"Tried: {candidate_names}"
    )


def _call_with_supported_kwargs(fn: Callable[..., Any], **kwargs: Any) -> Any:
    sig = inspect.signature(fn)
    accepted = {k: v for k, v in kwargs.items() if k in sig.parameters}
    return fn(**accepted)


def append_indices_daily(profile: str = "paper", **kwargs: Any) -> dict[str, Any]:
    fn = _resolve_legacy_callable()
    result = _call_with_supported_kwargs(fn, profile=profile, **kwargs)

    if isinstance(result, dict):
        result.setdefault("ok", True)
        result.setdefault("profile", profile)
        result.setdefault("action", "append_indices_daily")
        return result

    return {
        "ok": True,
        "profile": profile,
        "action": "append_indices_daily",
        "result": result,
    }


def run(profile: str = "paper", **kwargs: Any) -> dict[str, Any]:
    return append_indices_daily(profile=profile, **kwargs)


def main(profile: str = "paper", **kwargs: Any) -> dict[str, Any]:
    return append_indices_daily(profile=profile, **kwargs)
