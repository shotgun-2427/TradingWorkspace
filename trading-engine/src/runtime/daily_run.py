"""
daily_run.py — Public entrypoint for the daily auto-trade pipeline.

This is a thin wrapper that delegates to the canonical implementation in
``src.production.daily_runner``. Two reasons it exists:

  1. Discoverability — ``src/runtime/`` is the conceptual home for the
     things that run on a schedule. Putting the entrypoint here means
     ``ls src/runtime/`` is enough to understand what happens day-to-day.
  2. Backwards-compat — earlier docs referenced ``src.runtime.daily_run``;
     keep it working.

For new code, prefer importing from ``src.production.daily_runner``
directly.

Usage:
    python -m src.runtime.daily_run                # full cycle
    python -m src.runtime.daily_run --dry-run      # no order submission
    python -m src.runtime.daily_run --force-rebalance
"""
from __future__ import annotations

import sys
from typing import Sequence

from src.production import daily_runner


def main(argv: Sequence[str] | None = None) -> int:
    """Run one daily cycle. Returns process exit code."""
    if argv is not None:
        # daily_runner reads sys.argv directly; let callers override.
        sys.argv = ["daily_run", *list(argv)]
    rc = daily_runner.main()
    return int(rc or 0)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
