"""
build_basket.py — CLI for the basket-build step in isolation.

Useful when you want to regenerate the diff vs current positions without
re-running the bar-append + target-generate steps.

    python -m src.cli.build_basket --profile paper
    python -m src.cli.build_basket --profile paper --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys

from src.production.daily_runner import (
    DEFAULT_CLIENT_ID,
    DEFAULT_HOST,
    DEFAULT_PORT_PAPER,
    step_build_basket,
)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build the rebalance basket.")
    p.add_argument("--profile", default="paper", choices=["paper", "live"])
    p.add_argument("--host", default=DEFAULT_HOST)
    p.add_argument("--port", type=int, default=DEFAULT_PORT_PAPER)
    p.add_argument("--client-id", type=int, default=DEFAULT_CLIENT_ID)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    result = step_build_basket(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        profile=args.profile,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
