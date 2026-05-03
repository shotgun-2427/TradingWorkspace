"""
backfill.py — CLI for back-filling missing daily bars.

Wraps ``src.production.backfill_gaps`` so you can backfill a date range
without writing a one-off script.

    python -m src.cli.backfill --profile paper --start 2026-01-01 --end 2026-04-30
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Backfill missing IBKR daily bars.")
    p.add_argument("--profile", default="paper", choices=["paper", "live"])
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, default=date.today())
    p.add_argument("--symbols", nargs="*", default=None,
                   help="Optional symbol list; defaults to the profile universe.")
    args = p.parse_args(argv)

    try:
        from src.production import backfill_gaps  # type: ignore
    except ImportError as exc:
        print(json.dumps({"ok": False, "error": f"backfill module unavailable: {exc}"}))
        return 1

    fn = (
        getattr(backfill_gaps, "backfill_range", None)
        or getattr(backfill_gaps, "main", None)
    )
    if fn is None:
        print(json.dumps({"ok": False, "error": "no backfill function found"}))
        return 1

    result = fn(profile=args.profile, start=args.start, end=args.end, symbols=args.symbols)
    print(json.dumps(result if isinstance(result, dict) else {"result": str(result)},
                     indent=2, default=str))
    return 0 if (isinstance(result, dict) and result.get("ok", True)) else 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
