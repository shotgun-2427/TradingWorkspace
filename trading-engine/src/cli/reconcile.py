"""
reconcile.py — CLI for the end-of-day reconciliation step.

    python -m src.cli.reconcile --profile paper
    python -m src.cli.reconcile --profile paper --no-arm   # don't arm kill switch on mismatch
"""
from __future__ import annotations

import argparse
import json
import sys

from src.runtime.end_of_day_reconcile import reconcile_end_of_day


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="End-of-day reconcile.")
    p.add_argument("--profile", default="paper", choices=["paper", "live"])
    p.add_argument("--material-threshold-usd", type=float, default=100.0)
    p.add_argument("--no-arm", action="store_true",
                   help="Do not arm kill switch even on material mismatch.")
    args = p.parse_args(argv)

    result = reconcile_end_of_day(
        profile=args.profile,
        material_threshold_usd=args.material_threshold_usd,
        arm_switch_on_mismatch=not args.no_arm,
    )
    print(json.dumps(result.to_dict(), indent=2, default=str))
    return 0 if result.ok and not result.material_mismatch else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
