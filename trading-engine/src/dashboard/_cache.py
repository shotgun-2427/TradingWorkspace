"""
_cache.py - Streamlit-free cache_data replacement.

The Streamlit dashboard has been replaced by a FastAPI + JS frontend, but the
data-loading helpers in ``utils/data_loaders.py`` and ``utils/slippage.py`` were
written against ``@st.cache_data``. Rather than rewrite every loader, this
module exposes a drop-in shim that mimics the bits of the Streamlit cache API
those files actually use:

    @cache_data
    def my_loader(...): ...

    @cache_data(ttl=60, show_spinner=False)
    def my_other_loader(...): ...

    cache_data.clear()           # invalidates every wrapped function
    my_loader.clear()            # invalidates one wrapped function

The cache key is built from ``(args, sorted(kwargs.items()))`` and stored in a
process-local dict. Values that are not hashable (lists, dicts, DataFrames in
defaults) are not used as keys.

Replace ``import streamlit as st`` with::

    from src.dashboard._cache import cache_data as _cache_data

and ``@st.cache_data(...)`` with ``@_cache_data(...)``.
"""
from __future__ import annotations

import functools
import threading
import time
from typing import Any, Callable


_REGISTRY: list["_CachedFunction"] = []
_REGISTRY_LOCK = threading.Lock()


def _make_key(args: tuple, kwargs: dict) -> tuple:
    try:
        return (args, tuple(sorted(kwargs.items())))
    except TypeError:
        # Unhashable kwargs; fall back to repr (still deterministic enough
        # for the dashboard's purposes).
        return (args, repr(sorted(kwargs.items(), key=lambda kv: kv[0])))


class _CachedFunction:
    """A callable wrapper that memoizes results with an optional TTL."""

    def __init__(self, func: Callable[..., Any], ttl: float | None) -> None:
        self._func = func
        self._ttl = ttl
        self._lock = threading.Lock()
        self._store: dict[Any, tuple[float, Any]] = {}
        functools.update_wrapper(self, func)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        key = _make_key(args, kwargs)
        now = time.monotonic()
        with self._lock:
            cached = self._store.get(key)
            if cached is not None:
                stored_at, value = cached
                if self._ttl is None or (now - stored_at) <= self._ttl:
                    return value

        # Compute outside the lock - the loader may be slow.
        result = self._func(*args, **kwargs)
        with self._lock:
            self._store[key] = (time.monotonic(), result)
        return result

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


def _wrap(func: Callable[..., Any], ttl: float | None) -> _CachedFunction:
    wrapped = _CachedFunction(func, ttl)
    with _REGISTRY_LOCK:
        _REGISTRY.append(wrapped)
    return wrapped


def cache_data(
    func: Callable[..., Any] | None = None,
    *,
    ttl: float | None = None,
    show_spinner: bool = True,  # accepted for API parity, ignored
):
    """Decorator. Usable either bare (``@cache_data``) or with kwargs."""
    _ = show_spinner  # API-compat with Streamlit; ignored.
    if func is not None and callable(func):
        return _wrap(func, ttl)

    def _decorator(f: Callable[..., Any]) -> _CachedFunction:
        return _wrap(f, ttl)

    return _decorator


def _clear_all() -> None:
    with _REGISTRY_LOCK:
        functions = list(_REGISTRY)
    for fn in functions:
        fn.clear()


# Streamlit exposes ``st.cache_data.clear()``; mirror that here.
cache_data.clear = _clear_all  # type: ignore[attr-defined]
