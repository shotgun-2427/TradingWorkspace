#!/usr/bin/env python3
"""
daily_runner.py — Daily auto-trade pipeline.

Run this at 4:30 PM ET (after market close) every trading day.
It handles the full cycle automatically:

  1. Append today's IBKR daily bars to the master price file
  2. Regenerate targets (momentum model — picks top-5 ETFs monthly)
  3. Detect if today is a rebalance date (end-of-month or new signal)
  4. If rebalance needed: build basket (diff vs current positions)
  5. Submit paper orders to IBKR
  6. Write structured log to data/logs/runtime/

Usage:
    cd /Users/tradingworkspace/TradingWorkspace/trading-engine
    python -m src.production.daily_runner
    python -m src.production.daily_runner --dry-run         # skip order submission
    python -m src.production.daily_runner --force-rebalance # always rebalance today
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# ── Logging ──────────────────────────────────────────────────────────────────

LOG_DIR = PROJECT_ROOT / "data" / "logs" / "runtime"
LOG_DIR.mkdir(parents=True, exist_ok=True)

_ts_file = datetime.now().strftime("%Y%m%d_%H%M%S")
_log_path = LOG_DIR / f"daily_runner_{_ts_file}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_log_path),
    ],
)
log = logging.getLogger("daily_runner")

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_HOST = os.environ.get("IBKR_HOST", "127.0.0.1")
# Allow overriding the IBKR API port via env so the same plist/script works
# whether the user runs IB Gateway (4002 paper / 4001 live) or TWS
# (7497 paper / 7496 live). CLI --port still wins over env.
DEFAULT_PORT_PAPER = int(os.environ.get("IBKR_PORT_PAPER", "4002"))
DEFAULT_PORT_LIVE = int(os.environ.get("IBKR_PORT_LIVE", "4001"))
DEFAULT_CLIENT_ID = int(os.environ.get("IBKR_CLIENT_ID", "101"))

# Momentum strategy params
MOMENTUM_LOOKBACK = 126   # trading days (~6 months)
MIN_HISTORY = 126
TOP_K = 5                 # number of ETFs to hold at a time

# Rebalance threshold: submit orders if any weight diff exceeds this
REBALANCE_WEIGHT_THRESHOLD = 0.02   # 2% drift triggers rebalance


# ── Helpers ───────────────────────────────────────────────────────────────────

def _today() -> date:
    return datetime.now().date()


def _is_month_end_week() -> bool:
    """True if today is in the last 5 calendar days of the month."""
    d = _today()
    import calendar
    last_day = calendar.monthrange(d.year, d.month)[1]
    return d.day >= last_day - 4


def _load_latest_targets() -> "pd.DataFrame | None":
    """Load the most recent rebalance date from the targets file."""
    try:
        import pandas as pd
        path = PROJECT_ROOT / "data" / "market" / "cleaned" / "targets" / "etf_targets_monthly.parquet"
        if not path.exists():
            path = path.with_suffix(".csv")
        if not path.exists():
            return None
        df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
        df["rebalance_date"] = pd.to_datetime(df["rebalance_date"]).dt.date
        return df
    except Exception as exc:
        log.warning("Could not load targets: %s", exc)
        return None


def _latest_rebalance_date(targets_df: "pd.DataFrame") -> date | None:
    try:
        return targets_df["rebalance_date"].max()
    except Exception:
        return None


def _needs_rebalance(targets_df: "pd.DataFrame | None", force: bool = False) -> bool:
    """
    Decide if we should submit orders today.

    Rules (any one triggers rebalance):
      - force=True flag
      - No targets file at all → definitely need to set up
      - Latest rebalance date was in the current month → positions may be stale
      - Latest rebalance date was in a prior month → missed a rebalance
    """
    if force:
        log.info("Force-rebalance flag set → will submit orders.")
        return True

    if targets_df is None or targets_df.empty:
        log.info("No targets found → will rebalance.")
        return True

    latest = _latest_rebalance_date(targets_df)
    today = _today()

    if latest is None:
        return True

    # If the latest signal date is from a past month, we need fresh targets
    if latest.year < today.year or latest.month < today.month:
        log.info(
            "Latest rebalance date %s is before current month (%s-%02d) → will rebalance.",
            latest, today.year, today.month,
        )
        return True

    log.info("Latest rebalance date %s is current month → skipping order submission today.", latest)
    return False


def _write_run_log(run_id: str, result: dict[str, Any]) -> Path:
    run_dir = PROJECT_ROOT / "artifacts" / "runs"
    run_dir.mkdir(parents=True, exist_ok=True)
    out = run_dir / f"daily_run_{run_id}.json"
    with open(out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    log.info("Run log saved: %s", out)
    return out


# ── Step functions ────────────────────────────────────────────────────────────

def step_append_daily(
    host: str,
    port: int,
    client_id: int,
    profile: str,
    lookback: str = "5 D",
) -> dict[str, Any]:
    log.info("── Step 1: Append IBKR daily bars ──────────────────────────")
    from src.production.pipeline.append_ibkr_daily import append_ibkr_daily

    result = append_ibkr_daily(
        profile=profile,
        host=host,
        port=port,
        client_id=client_id,
        lookback=lookback,
    )

    if result.get("ok"):
        log.info(
            "✓ Appended %s new rows for %s symbols. Latest date: %s",
            result.get("new_rows_added_to_master", 0),
            result.get("symbols_with_data", 0),
            result.get("latest_date"),
        )
    else:
        log.error("✗ Append failed: %s", result.get("error"))

    return result


def step_generate_targets(profile: str) -> dict[str, Any]:
    log.info("── Step 2: Generate targets ─────────────────────────────────")
    from src.production.generate_targets import generate_targets

    optimizer = os.environ.get("CAPITALFUND_OPTIMIZER")  # None → use module default
    log.info(
        "  Optimizer: %s (override with CAPITALFUND_OPTIMIZER env)",
        optimizer or "default (inverse_vol)",
    )

    result = generate_targets(
        profile=profile,
        momentum_lookback=MOMENTUM_LOOKBACK,
        min_history=MIN_HISTORY,
        top_k=TOP_K,
        optimizer=optimizer,
    )

    if result.get("ok"):
        log.info(
            "✓ Targets written. Rebalance dates: %s. Latest: %s.",
            result.get("rebalance_dates", 0),
            result.get("latest_rebalance_date"),
        )
    else:
        log.error("✗ Target generation failed: %s", result.get("error"))

    return result


def step_build_basket(
    host: str,
    port: int,
    client_id: int,
    profile: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    log.info("── Step 3: Build basket ─────────────────────────────────────")
    try:
        from src.production.runtime.build_paper_basket import (
            default_config,
            BasketBuildConfig,
        )
        import dataclasses

        cfg = default_config()
        cfg = dataclasses.replace(cfg, host=host, port=port, client_id=client_id)

        # Import the actual build function dynamically
        import importlib
        mod = importlib.import_module("src.production.runtime.build_paper_basket")

        # Try to find the main build callable
        for name in ["build_paper_basket", "run", "main", "build"]:
            fn = getattr(mod, name, None)
            if callable(fn) and name != "default_config":
                result = fn(cfg)
                if isinstance(result, dict):
                    result.setdefault("ok", True)
                    result.setdefault("action", "build_basket")
                    return result
                return {"ok": True, "action": "build_basket", "result": str(result)}

        return {"ok": False, "action": "build_basket", "error": "No build function found"}

    except Exception as exc:
        log.exception("Build basket error")
        return {"ok": False, "action": "build_basket", "error": str(exc)}


def step_submit_orders(
    host: str,
    port: int,
    client_id: int,
    profile: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    log.info("── Step 4: Submit paper orders ──────────────────────────────")

    # ── Pre-trade risk gate ──────────────────────────────────────────────
    # Runs the kill switch + price validators + exposure limits before any
    # network call to IBKR. Aborts with a structured reason if anything
    # fails so the run log makes the failure cause obvious.
    try:
        import pandas as pd
        from src.runtime.risk_checks import run_pre_trade_risk_checks

        recon_path = PROJECT_ROOT / "data" / "broker" / "reconciliations" / f"{profile}_reconciliation.parquet"
        if not recon_path.exists():
            recon_path = recon_path.with_suffix(".csv")
        basket_df = (
            pd.read_parquet(recon_path) if recon_path.suffix == ".parquet"
            else pd.read_csv(recon_path)
        )

        # NAV from latest account summary.
        acct_dir = PROJECT_ROOT / "data" / "broker" / "account"
        files = sorted(acct_dir.glob(f"{profile}_account_summary_*.csv"))
        nav = 0.0
        if files:
            try:
                summary = pd.read_csv(files[-1])
                tag_col = "tag" if "tag" in summary.columns else summary.columns[0]
                val_col = "value" if "value" in summary.columns else summary.columns[1]
                hit = summary.loc[
                    summary[tag_col].astype(str).str.lower() == "netliquidation"
                ]
                if not hit.empty:
                    nav = float(str(hit.iloc[0][val_col]).replace(",", ""))
            except Exception as exc:
                log.warning("Could not parse NAV from account summary: %s", exc)

        prices_path = PROJECT_ROOT / "data" / "market" / "cleaned" / "prices" / "etf_prices_master.parquet"
        risk = run_pre_trade_risk_checks(
            basket_df=basket_df,
            nav=nav,
            prices_path=prices_path,
        )
        log.info(risk.report())
        if not risk.ok:
            log.error("✗ Pre-trade risk checks BLOCKED submission.")
            return {
                "ok": False,
                "action": "submit_orders",
                "blocked": True,
                "reasons": risk.blocking_reasons,
                "warnings": risk.warnings,
                "risk_details": risk.details,
            }
    except Exception as exc:
        # If the risk gate itself fails to load, that's a *fail-open* risk —
        # block submission rather than send orders blind.
        log.exception("Risk gate raised — blocking submission as a precaution.")
        return {
            "ok": False,
            "action": "submit_orders",
            "blocked": True,
            "reasons": [f"Risk gate error: {type(exc).__name__}: {exc}"],
        }

    if dry_run:
        log.info("DRY RUN — skipping order submission.")
        return {"ok": True, "action": "submit_orders", "skipped": True, "reason": "dry_run"}

    try:
        from src.dashboard.services.order_service import submit_paper_orders
        result = submit_paper_orders(
            profile=profile,
            host=host,
            port=port,
            client_id=client_id,
            force=False,
            dry_run=False,
        )
        if result.get("ok"):
            log.info(
                "✓ Orders submitted. submitted=%s · path=%s",
                result.get("submitted"),
                result.get("submission_path"),
            )
        else:
            log.error("✗ Submit failed: %s", result.get("reason") or result.get("error"))
        return result
    except Exception as exc:
        log.exception("Submit orders error")
        return {"ok": False, "action": "submit_orders", "error": str(exc)}


# ── Main runner ───────────────────────────────────────────────────────────────

def run(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT_PAPER,
    client_id: int = DEFAULT_CLIENT_ID,
    profile: str = "paper",
    lookback: str = "5 D",
    dry_run: bool = False,
    force_rebalance: bool = False,
    skip_append: bool = False,
) -> dict[str, Any]:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    today = str(_today())

    log.info("=" * 60)
    log.info("DAILY RUNNER — %s", today)
    log.info("  Profile   : %s", profile)
    log.info("  Endpoint  : %s:%s (client %s)", host, port, client_id)
    log.info("  Dry run   : %s", dry_run)
    log.info("  Force rebalance : %s", force_rebalance)
    log.info("=" * 60)

    run_result: dict[str, Any] = {
        "run_id": run_id,
        "date": today,
        "profile": profile,
        "dry_run": dry_run,
        "steps": {},
    }

    # ── Step 1: Append daily bars ─────────────────────────────────────────────
    if not skip_append:
        append_result = step_append_daily(
            host=host, port=port, client_id=client_id,
            profile=profile, lookback=lookback,
        )
        run_result["steps"]["append_daily"] = append_result
        if not append_result.get("ok"):
            log.error("Append failed — aborting pipeline.")
            run_result["ok"] = False
            run_result["error"] = "append_daily failed"
            _write_run_log(run_id, run_result)
            return run_result
    else:
        log.info("Skipping append (--skip-append flag).")
        run_result["steps"]["append_daily"] = {"skipped": True}

    # ── Step 2: Generate targets ──────────────────────────────────────────────
    targets_result = step_generate_targets(profile=profile)
    run_result["steps"]["generate_targets"] = targets_result

    if not targets_result.get("ok"):
        log.error("Target generation failed — aborting.")
        run_result["ok"] = False
        run_result["error"] = "generate_targets failed"
        _write_run_log(run_id, run_result)
        return run_result

    # ── Rebalance decision ────────────────────────────────────────────────────
    targets_df = _load_latest_targets()
    should_rebalance = _needs_rebalance(targets_df, force=force_rebalance)
    run_result["should_rebalance"] = should_rebalance

    if not should_rebalance:
        log.info("No rebalance needed today. Done.")
        run_result["ok"] = True
        _write_run_log(run_id, run_result)
        return run_result

    # ── Step 3: Build basket ──────────────────────────────────────────────────
    basket_result = step_build_basket(
        host=host, port=port, client_id=client_id + 10,
        profile=profile, dry_run=dry_run,
    )
    run_result["steps"]["build_basket"] = basket_result

    if not basket_result.get("ok"):
        log.error("Basket build failed — aborting order submission.")
        run_result["ok"] = False
        run_result["error"] = "build_basket failed"
        _write_run_log(run_id, run_result)
        return run_result

    # ── Step 4: Submit orders ─────────────────────────────────────────────────
    submit_result = step_submit_orders(
        host=host, port=port, client_id=client_id + 20,
        profile=profile, dry_run=dry_run,
    )
    run_result["steps"]["submit_orders"] = submit_result

    overall_ok = submit_result.get("ok", False)
    run_result["ok"] = overall_ok

    log.info("=" * 60)
    log.info("DAILY RUNNER COMPLETE — %s", "✓ OK" if overall_ok else "✗ FAILED")
    log.info("=" * 60)

    _write_run_log(run_id, run_result)
    return run_result


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Daily auto-trade runner")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=(
            "IBKR API port. Defaults to IBKR_PORT_PAPER env var (or 4002) "
            "for paper, IBKR_PORT_LIVE (or 4001) for live."
        ),
    )
    parser.add_argument("--client-id", type=int, default=DEFAULT_CLIENT_ID, dest="client_id")
    parser.add_argument("--profile", default="paper", choices=["paper", "live"])
    parser.add_argument("--lookback", default="5 D", help="Lookback for daily bar append")
    parser.add_argument("--dry-run", action="store_true", dest="dry_run",
                        help="Run everything except order submission")
    parser.add_argument("--force-rebalance", action="store_true", dest="force_rebalance",
                        help="Submit orders even if no new rebalance date detected")
    parser.add_argument("--skip-append", action="store_true", dest="skip_append",
                        help="Skip IBKR data append (use existing data)")
    parser.add_argument("--json-out", action="store_true", help="Print result JSON to stdout")
    args = parser.parse_args()

    # Resolve port: CLI > env > profile default.
    resolved_port = args.port
    if resolved_port is None:
        resolved_port = (
            DEFAULT_PORT_PAPER if args.profile == "paper" else DEFAULT_PORT_LIVE
        )

    result = run(
        host=args.host,
        port=resolved_port,
        client_id=args.client_id,
        profile=args.profile,
        lookback=args.lookback,
        dry_run=args.dry_run,
        force_rebalance=args.force_rebalance,
        skip_append=args.skip_append,
    )

    if args.json_out:
        print(json.dumps(result, indent=2, default=str))

    sys.exit(0 if result.get("ok") else 1)


if __name__ == "__main__":
    main()
