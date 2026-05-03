"""
validators.py — Sanity-check the master price file before signals consume it.

Bad price data → bad signals → bad trades. Catch problems early.

Checks performed:
  * required columns present (date, symbol, close)
  * no negative or zero closes
  * no NaN closes for an active row
  * no duplicate (date, symbol) rows
  * no impossible single-day moves (>50% absolute move on a major ETF is
    almost always a corporate action / split that wasn't adjusted)
  * staleness — every symbol has at least one row in the last N business days

The validator returns a structured result with `errors`, `warnings`, and a
boolean `ok` flag. Daily runner calls this before signals so a corrupted
master file aborts the run rather than producing a bad basket.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as _date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from src.common.clock import is_us_business_day, now_et


# Tolerance: anything bigger than ±50% in a single day for a regulated US
# ETF is almost certainly a split / data-feed bug. ETHA/IBIT etc. can swing
# 20-30% but +50% in one bar is implausible for these instruments.
MAX_DAILY_RETURN_PCT = 0.50

# Symbols are considered stale if their newest bar is older than N business
# days. The runner pulls daily bars at 16:32 ET, so a 1-business-day lag is
# normal (we're seeing yesterday's close). 3 business days is the threshold.
STALE_BUSINESS_DAYS = 3


@dataclass
class PriceValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)

    def report(self) -> str:
        lines: list[str] = []
        lines.append(f"Validation: {'PASS' if self.ok else 'FAIL'}")
        for k, v in self.summary.items():
            lines.append(f"  {k}: {v}")
        if self.errors:
            lines.append(f"Errors ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"  ✗ {e}")
        if self.warnings:
            lines.append(f"Warnings ({len(self.warnings)}):")
            for w in self.warnings:
                lines.append(f"  ⚠ {w}")
        return "\n".join(lines)


def _business_days_between(a: _date, b: _date) -> int:
    """Number of NYSE business days strictly between (a, b]."""
    if b <= a:
        return 0
    n = 0
    cursor = a + timedelta(days=1)
    while cursor <= b:
        if is_us_business_day(cursor):
            n += 1
        cursor += timedelta(days=1)
    return n


def validate_master_prices(
    df: pd.DataFrame,
    *,
    expected_symbols: tuple[str, ...] | None = None,
    max_daily_return_pct: float = MAX_DAILY_RETURN_PCT,
    stale_business_days: int = STALE_BUSINESS_DAYS,
) -> PriceValidationResult:
    """Validate a master prices DataFrame in long format.

    Expected columns: ``date``, ``symbol``, ``close`` (case-insensitive).
    Optional ``expected_symbols`` tuple — if provided, every symbol in this
    list must have at least one row.
    """
    res = PriceValidationResult(ok=True)
    if df is None or df.empty:
        res.ok = False
        res.errors.append("DataFrame is empty")
        return res

    # ── Schema ─────────────────────────────────────────────────────────────
    df = df.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]
    required = ["date", "symbol", "close"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        res.ok = False
        res.errors.append(f"Missing required columns: {missing_cols}")
        return res

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    nulls_in_required = df[required].isna().any(axis=1).sum()
    if nulls_in_required:
        res.errors.append(f"{nulls_in_required} rows have nulls in required columns")
        res.ok = False

    df = df.dropna(subset=required)
    n = len(df)
    n_symbols = df["symbol"].nunique()
    res.summary["rows"] = n
    res.summary["symbols"] = n_symbols
    res.summary["date_range"] = (
        f"{df['date'].min().date()} → {df['date'].max().date()}" if n else "—"
    )

    # ── Negative or zero closes ────────────────────────────────────────────
    bad_close = df[df["close"] <= 0]
    if not bad_close.empty:
        sample = bad_close.head(3)[["date", "symbol", "close"]].to_dict(orient="records")
        res.errors.append(f"{len(bad_close)} rows with non-positive close. e.g. {sample}")
        res.ok = False

    # ── Duplicate (date, symbol) ───────────────────────────────────────────
    dups = df.duplicated(subset=["date", "symbol"]).sum()
    if dups:
        res.errors.append(f"{dups} duplicate (date, symbol) rows")
        res.ok = False

    # ── Impossible daily moves ─────────────────────────────────────────────
    df_sorted = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    df_sorted["pct_change"] = df_sorted.groupby("symbol")["close"].pct_change()
    big_moves = df_sorted[df_sorted["pct_change"].abs() > max_daily_return_pct]
    if not big_moves.empty:
        # Only the most recent few (older spikes are usually known splits,
        # already-acceptable history).
        recent = big_moves[big_moves["date"] >= (df["date"].max() - pd.Timedelta(days=365))]
        if not recent.empty:
            sample = recent.tail(3)[["date", "symbol", "close", "pct_change"]].to_dict(
                orient="records"
            )
            res.warnings.append(
                f"{len(recent)} suspicious >|{max_daily_return_pct*100:.0f}%| "
                f"moves in the last year. e.g. {sample}"
            )

    # ── Staleness ──────────────────────────────────────────────────────────
    today_et = now_et().date()
    latest_per_sym = df.groupby("symbol")["date"].max().dt.date
    stale = []
    for sym, latest in latest_per_sym.items():
        bdays = _business_days_between(latest, today_et)
        if bdays > stale_business_days:
            stale.append((sym, str(latest), bdays))
    res.summary["stale_symbols"] = len(stale)
    if stale:
        sample = stale[:5]
        res.warnings.append(
            f"{len(stale)} symbols are stale (>{stale_business_days} business "
            f"days behind). e.g. {sample}"
        )

    # ── Coverage of expected universe ──────────────────────────────────────
    if expected_symbols:
        present = set(df["symbol"].unique())
        missing_universe = sorted(set(expected_symbols) - present)
        if missing_universe:
            res.errors.append(f"Missing expected symbols: {missing_universe}")
            res.ok = False

    return res


def validate_master_prices_file(
    path: str | Path,
    **kwargs: Any,
) -> PriceValidationResult:
    """Convenience wrapper that loads the file and validates it."""
    p = Path(path)
    if not p.exists():
        return PriceValidationResult(
            ok=False, errors=[f"File not found: {p}"]
        )
    if p.suffix == ".parquet":
        df = pd.read_parquet(p)
    else:
        df = pd.read_csv(p)
    return validate_master_prices(df, **kwargs)
