"""
Initialize all BigQuery tables used by the pipelines.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


SCRIPTS_IN_ORDER = [
    "setup_fields_table.py",
    "setup_hawk_identifiers_table.py",
    "setup_records_table.py",
    "setup_supplemental_series_table.py",
    "setup_supplemental_records_table.py",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize all pipeline tables")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["production", "development"],
        help="Target dataset mode",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(__file__).resolve().parent

    for script_name in SCRIPTS_IN_ORDER:
        script_path = base_dir / script_name
        print(f"Running {script_name} --mode {args.mode}")
        subprocess.run(
            [sys.executable, str(script_path), "--mode", args.mode],
            check=True,
        )

    print("All table initialization scripts completed.")


if __name__ == "__main__":
    main()
