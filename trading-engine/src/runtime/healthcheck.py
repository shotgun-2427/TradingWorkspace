"""
healthcheck.py — System-wide health probe.

Combines every check the dashboard's System Audit page runs into a single
function so any caller (dashboard, CLI, supervisor) gets the same report.

Returns a ``HealthReport`` with three classes of issues:
  * `failures` — at least one critical condition is broken; trading is unsafe
  * `warnings` — non-critical (e.g. log file slightly stale)
  * `info`     — everything green

Designed to be importable without pulling in Streamlit (so it works from
crontab / launchd / `python -m`).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from src.execution.kill_switch import read_kill_switch_state

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class HealthReport:
    ok: bool
    failures: list[dict[str, str]] = field(default_factory=list)
    warnings: list[dict[str, str]] = field(default_factory=list)
    info: list[dict[str, str]] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def all_checks(self) -> list[dict[str, str]]:
        return self.failures + self.warnings + self.info

    def report(self) -> str:
        out = [f"Health: {'OK' if self.ok else 'NOT OK'}"]
        for cls, items, sym in (
            ("FAIL", self.failures, "✗"),
            ("WARN", self.warnings, "⚠"),
            ("INFO", self.info, "✓"),
        ):
            for c in items:
                out.append(f"  {sym} {c['check']}: {c['detail']}")
        return "\n".join(out)


def _file_age_hours(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        return (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds() / 3600.0
    except OSError:
        return None


def _check_master_prices() -> dict[str, str]:
    base = PROJECT_ROOT / "data" / "market" / "cleaned" / "prices"
    parquet = base / "etf_prices_master.parquet"
    csv = base / "etf_prices_master.csv"
    path = parquet if parquet.exists() else (csv if csv.exists() else None)
    if path is None:
        return {"check": "master_prices", "level": "fail", "detail": "no master price file"}
    try:
        import pandas as pd

        df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        latest = df["date"].max()
        age = (datetime.now() - latest).days
        rows = len(df)
        n_sym = df["symbol"].nunique()
        if age <= 1:
            level = "info"
        elif age <= 7:
            level = "warn"
        else:
            level = "fail"
        return {
            "check": "master_prices",
            "level": level,
            "detail": f"{rows:,} rows · {n_sym} symbols · latest {latest.date()} ({age}d ago)",
        }
    except Exception as exc:
        return {"check": "master_prices", "level": "fail", "detail": f"read failed: {exc}"}


def _check_targets() -> dict[str, str]:
    base = PROJECT_ROOT / "data" / "market" / "cleaned" / "targets"
    parquet = base / "etf_targets_monthly.parquet"
    csv = base / "etf_targets_monthly.csv"
    path = parquet if parquet.exists() else (csv if csv.exists() else None)
    if path is None:
        return {"check": "targets", "level": "fail", "detail": "no targets file"}
    try:
        import pandas as pd

        df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
        date_col = "rebalance_date" if "rebalance_date" in df.columns else "date"
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        latest = df[date_col].max()
        age = (datetime.now() - latest).days
        n_picks = (df[df[date_col] == latest].shape[0])
        level = "info" if age <= 31 else "warn"
        return {
            "check": "targets",
            "level": level,
            "detail": f"latest rebalance {latest.date()} ({age}d ago) · {n_picks} picks",
        }
    except Exception as exc:
        return {"check": "targets", "level": "fail", "detail": f"read failed: {exc}"}


def _check_positions(profile: str) -> dict[str, str]:
    path = PROJECT_ROOT / "data" / "broker" / "positions" / f"{profile}_positions_snapshot.csv"
    age = _file_age_hours(path)
    if age is None:
        return {"check": "positions_snapshot", "level": "fail", "detail": "missing"}
    if age <= 24:
        level = "info"
    elif age <= 72:
        level = "warn"
    else:
        level = "fail"
    return {
        "check": "positions_snapshot",
        "level": level,
        "detail": f"last write {age:.1f}h ago",
    }


def _check_account_summary(profile: str) -> dict[str, str]:
    folder = PROJECT_ROOT / "data" / "broker" / "account"
    files = sorted(folder.glob(f"{profile}_account_summary_*.csv"))
    if not files:
        return {"check": "account_summary", "level": "fail", "detail": "no snapshots"}
    age = _file_age_hours(files[-1])
    if age is None:
        return {"check": "account_summary", "level": "fail", "detail": "unreadable"}
    level = "info" if age <= 24 else ("warn" if age <= 72 else "fail")
    return {
        "check": "account_summary",
        "level": level,
        "detail": f"{len(files)} snapshots · last {age:.1f}h ago",
    }


def _check_kill_switch() -> dict[str, str]:
    state = read_kill_switch_state()
    if not state.armed:
        return {"check": "kill_switch", "level": "info", "detail": "disarmed"}
    return {
        "check": "kill_switch",
        "level": "fail",
        "detail": f"ARMED — {state.reason} (since {state.armed_at})",
    }


def _check_runner_log() -> dict[str, str]:
    folder = PROJECT_ROOT / "data" / "logs" / "runtime"
    files = sorted(folder.glob("daily_runner_*.log"))
    if not files:
        return {"check": "runner_log", "level": "warn", "detail": "no runs yet"}
    age = _file_age_hours(files[-1])
    try:
        tail = files[-1].read_text(encoding="utf-8", errors="ignore").splitlines()[-200:]
    except OSError:
        tail = []
    failed = any("Append failed" in line or "✗" in line or "FAILED" in line for line in tail)
    completed = any(
        "DAILY RUNNER COMPLETE" in line or "No rebalance needed today" in line
        for line in tail
    )
    if failed:
        return {"check": "runner_log", "level": "fail", "detail": f"last run failed ({age:.1f}h ago)"}
    if not completed:
        return {"check": "runner_log", "level": "warn", "detail": f"last run did not complete ({age:.1f}h ago)"}
    if age and age > 36:
        return {"check": "runner_log", "level": "warn", "detail": f"older than 36h ({age:.1f}h)"}
    return {"check": "runner_log", "level": "info", "detail": f"last run OK ({age:.1f}h ago)"}


def run_healthcheck(profile: str = "paper") -> HealthReport:
    """Run every check and roll into a single report."""
    checks = [
        _check_kill_switch(),
        _check_master_prices(),
        _check_targets(),
        _check_positions(profile),
        _check_account_summary(profile),
        _check_runner_log(),
    ]

    report = HealthReport(ok=True)
    for c in checks:
        level = c.get("level", "info")
        if level == "fail":
            report.failures.append(c)
            report.ok = False
        elif level == "warn":
            report.warnings.append(c)
        else:
            report.info.append(c)

    report.details["profile"] = profile
    report.details["checks_run"] = len(checks)
    return report
