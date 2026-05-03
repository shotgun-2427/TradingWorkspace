"""
run_pipeline.py — CLI shim for ``src.runtime.daily_run``.

Same job; different entrypoint name. People reach for ``run_pipeline``
when they want to "run the whole thing", and for ``daily_run`` when they
want to think about it as the cron job.

    python -m src.cli.run_pipeline                # full cycle
    python -m src.cli.run_pipeline --dry-run
    python -m src.cli.run_pipeline --force-rebalance
"""
from __future__ import annotations

import sys

from src.runtime import daily_run


def main() -> int:
    return daily_run.main()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
