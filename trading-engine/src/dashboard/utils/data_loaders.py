from __future__ import annotations

import re
from datetime import date as _date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.dashboard._cache import cache_data as _cache_data


# Compatibility shim: this module was written against Streamlit's cache API.
# The new FastAPI dashboard uses a Streamlit-free in-process cache. We expose a
# tiny ``st`` namespace so the existing ``@st.cache_data(...)`` decorators below
# need no further edits. Use a class (not an instance) so that
# ``st.cache_data`` is a plain function reference, not a bound method.
class _StShim:
    cache_data = staticmethod(_cache_data)


st = _StShim

# NOTE: broker_service imports ib_async / ib_insync at module load. We DON'T
# want every dashboard screen to fail with ModuleNotFoundError just because
# IBKR libraries aren't installed — most screens only read CSVs from disk and
# don't need a live broker connection. Lazy-import inside the helper so the
# rest of data_loaders works without ib_async / ib_insync.


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _latest_file(folder: Path, pattern: str) -> Path | None:
    files = sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _extract_timestamp(path: Path) -> pd.Timestamp | None:
    m = re.search(r"(\d{8})_(\d{6})", path.name)
    if m:
        return pd.to_datetime(f"{m.group(1)} {m.group(2)}", format="%Y%m%d %H%M%S", errors="coerce")
    m = re.search(r"(\d{8})", path.name)
    if m:
        return pd.to_datetime(m.group(1), format="%Y%m%d", errors="coerce")
    try:
        return pd.Timestamp.fromtimestamp(path.stat().st_mtime)
    except Exception:
        return None


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def _first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {_normalize_name(c): c for c in df.columns}
    for candidate in candidates:
        actual = normalized.get(_normalize_name(candidate))
        if actual:
            return actual
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", "", regex=False).str.replace("$", "", regex=False),
        errors="coerce",
    )


def _extract_tag_value(df: pd.DataFrame, tags: list[str]) -> float | None:
    tag_col = _first_existing_col(df, ["tag", "key", "field", "name"])
    value_col = _first_existing_col(df, ["value", "amount", "val"])
    if tag_col is None or value_col is None:
        return None

    temp = df.copy()
    temp[tag_col] = temp[tag_col].astype(str)

    for tag in tags:
        hit = temp.loc[temp[tag_col].str.lower() == tag.lower()]
        if not hit.empty:
            val = _to_float(hit.iloc[0][value_col])
            if val is not None:
                return val
    return None


# TTL = 60s on the loaders that read live-ish files (positions / account /
# prices). Everything else stays cached indefinitely until the user clicks
# Clear cache or Refresh.
_LIVE_TTL = 60


@st.cache_data(show_spinner=False, ttl=_LIVE_TTL)
def build_equity_curve() -> pd.DataFrame:
    account_dir = _project_root() / "data" / "broker" / "account"
    rows: list[dict[str, Any]] = []

    for path in sorted(account_dir.glob("paper_account_summary_*.csv")):
        try:
            df = pd.read_csv(path)
            nav = _extract_tag_value(
                df,
                ["NetLiquidation", "EquityWithLoanValue", "Net Liquidation", "Equity With Loan Value"],
            )
            ts = _extract_timestamp(path)
            if nav is not None and ts is not None:
                rows.append({"timestamp": ts, "nav": float(nav)})
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["timestamp", "nav"])

    out = pd.DataFrame(rows).sort_values("timestamp").drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    return out


@st.cache_data(show_spinner=False, ttl=_LIVE_TTL)
def load_positions_snapshot() -> pd.DataFrame:
    path = _project_root() / "data" / "broker" / "positions" / "paper_positions_snapshot.csv"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame()

    symbol_col = _first_existing_col(df, ["symbol", "ticker"])
    shares_col = _first_existing_col(df, ["current_shares", "shares", "position", "qty", "quantity"])
    avg_col = _first_existing_col(df, ["avg_cost", "average_cost", "avg_px", "avgprice"])

    if symbol_col is None or shares_col is None or avg_col is None:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["symbol"] = df[symbol_col].astype(str).str.upper()
    out["shares"] = _to_num(df[shares_col]).fillna(0.0)
    out["avg_cost"] = _to_num(df[avg_col]).fillna(0.0)
    out = out.loc[out["shares"] != 0].copy()
    out = out.sort_values("symbol").reset_index(drop=True)
    return out


@st.cache_data(show_spinner=False, ttl=_LIVE_TTL)
def load_latest_prices() -> pd.DataFrame:
    prices_dir = _project_root() / "data" / "market" / "cleaned" / "prices"
    parquet_path = prices_dir / "etf_prices_master.parquet"
    csv_path = prices_dir / "etf_prices_master.csv"

    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
    elif csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        return pd.DataFrame()

    df.columns = [str(c).lower().strip() for c in df.columns]
    symbol_col = _first_existing_col(df, ["symbol", "ticker"])
    date_col = _first_existing_col(df, ["date", "datetime", "timestamp"])
    close_col = _first_existing_col(df, ["close", "adj_close", "adjclose", "adjusted_close"])

    if symbol_col is None or date_col is None or close_col is None:
        return pd.DataFrame()

    out = df[[symbol_col, date_col, close_col]].copy()
    out.columns = ["symbol", "date", "close"]
    out["symbol"] = out["symbol"].astype(str).str.upper()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["close"] = _to_num(out["close"])
    out = out.dropna(subset=["date", "close"])
    out = out.sort_values(["symbol", "date"]).reset_index(drop=True)
    return out


@st.cache_data(show_spinner=False)
def build_holdings_table() -> pd.DataFrame:
    positions = load_positions_snapshot()
    prices = load_latest_prices()

    if positions.empty or prices.empty:
        return pd.DataFrame()

    latest = (
        prices.groupby("symbol", as_index=False)
        .tail(1)[["symbol", "close"]]
        .rename(columns={"close": "last"})
        .sort_values("symbol")
        .reset_index(drop=True)
    )

    merged = positions.merge(latest, on="symbol", how="left")
    merged["last"] = merged["last"].fillna(0.0)
    merged["market_value"] = merged["shares"] * merged["last"]
    merged["pnl"] = merged["shares"] * (merged["last"] - merged["avg_cost"])

    total_mv = float(merged["market_value"].sum())
    merged["weight"] = np.where(total_mv > 0, merged["market_value"] / total_mv, 0.0)

    merged = merged.sort_values("market_value", ascending=False).reset_index(drop=True)
    return merged


def compute_curve_metrics(curve: pd.DataFrame) -> dict[str, float | None]:
    if curve.empty or len(curve) < 2:
        latest_nav = float(curve["nav"].iloc[-1]) if not curve.empty else None
        return {
            "latest_nav": latest_nav,
            "daily_pnl": None,
            "total_return": None,
            "annualized_return": None,
            "sharpe": None,
            "max_drawdown": None,
        }

    work = curve.copy().sort_values("timestamp").reset_index(drop=True)
    work["ret"] = work["nav"].pct_change()
    work["cummax"] = work["nav"].cummax()
    work["drawdown"] = work["nav"] / work["cummax"] - 1.0

    latest_nav = float(work["nav"].iloc[-1])
    prior_nav = float(work["nav"].iloc[-2])
    daily_pnl = latest_nav - prior_nav

    total_return = latest_nav / float(work["nav"].iloc[0]) - 1.0

    days = max((work["timestamp"].iloc[-1] - work["timestamp"].iloc[0]).days, 1)
    annualized_return = (1.0 + total_return) ** (365.0 / days) - 1.0 if total_return > -1 else None

    ret = work["ret"].dropna()
    sharpe = float((ret.mean() / ret.std()) * np.sqrt(252)) if len(ret) >= 2 and float(ret.std()) > 0 else None
    max_drawdown = float(work["drawdown"].min()) if not work["drawdown"].empty else None

    return {
        "latest_nav": latest_nav,
        "daily_pnl": daily_pnl,
        "total_return": total_return,
        "annualized_return": annualized_return,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown,
    }


def _load_live_net_liq(profile: str = "paper") -> float | None:
    try:
        # Lazy import — broker_service pulls in ib_async / ib_insync.
        from src.dashboard.services.broker_service import get_account_summary

        rows = get_account_summary(profile=profile)
        if not rows:
            return None
        df = pd.DataFrame(rows)
        return _extract_tag_value(df, ["NetLiquidation", "EquityWithLoanValue"])
    except Exception:
        return None


def load_home_dashboard_data(profile: str = "paper") -> dict[str, Any]:
    curve = build_equity_curve()
    holdings = build_holdings_table()

    live_nav = _load_live_net_liq(profile=profile)
    now = pd.Timestamp.now().floor("s")

    if live_nav is not None:
        live_row = pd.DataFrame([{"timestamp": now, "nav": float(live_nav)}])
        if curve.empty:
            curve = live_row
        else:
            curve = pd.concat([curve, live_row], ignore_index=True)
            curve = curve.sort_values("timestamp").drop_duplicates("timestamp", keep="last").reset_index(drop=True)

    metrics = compute_curve_metrics(curve)

    warnings: list[str] = []
    if curve.empty:
        warnings.append("No paper account snapshots found.")
    if holdings.empty:
        warnings.append("No positions snapshot or latest prices found.")
    if live_nav is None:
        warnings.append("Live IBKR NetLiquidation could not be loaded.")

    return {
        "curve": curve,
        "holdings": holdings,
        "metrics": metrics,
        "warnings": warnings,
    }


# ─── Engine-parity helpers (local-first analogs of the cloud data_loaders) ─────
#
# These are lightweight re-implementations of the cloud-backed helpers found in
# the original Trading-engine dashboard, adapted to read from the local
# workspace filesystem (data/broker/**, data/market/cleaned/**, artifacts/**).
# Every helper returns an empty / None value when the underlying artifact is
# missing so screens can render a graceful "no data yet" message.

_DATA_ROOT = _project_root() / "data"
_ARTIFACTS_ROOT = _project_root() / "artifacts"
_BROKER_DIR = _DATA_ROOT / "broker"
_ACCOUNT_DIR = _BROKER_DIR / "account"
_POSITIONS_DIR = _BROKER_DIR / "positions"
_RECON_DIR = _BROKER_DIR / "reconciliations"
_MARKET_DIR = _DATA_ROOT / "market" / "cleaned"
_PRICES_DIR = _MARKET_DIR / "prices"
_TARGETS_DIR = _MARKET_DIR / "targets"
_REPORTS_DIR = _ARTIFACTS_ROOT / "reports"
_RUNS_DIR = _ARTIFACTS_ROOT / "runs"
_BASKETS_DIR = _ARTIFACTS_ROOT / "baskets"
_FILLS_DIR = _ARTIFACTS_ROOT / "fills"

_DEFAULT_OPTIMIZER_NAME = "momentum_top5"


def _read_csv_or_parquet(path_no_suffix: Path) -> pd.DataFrame | None:
    """Read a parquet (preferred) or csv version of a given base path."""
    parquet = path_no_suffix.with_suffix(".parquet")
    csv = path_no_suffix.with_suffix(".csv")
    try:
        if parquet.exists():
            return pd.read_parquet(parquet)
        if csv.exists():
            return pd.read_csv(csv)
    except Exception:
        return None
    return None


@st.cache_data(show_spinner=False, ttl=_LIVE_TTL)
def get_latest_production_audit(profile: str = "paper") -> _date | None:
    """Return the most recent audit date for the selected profile.

    Priority order:
      1. Latest rebalance date in ``etf_targets_monthly`` (most relevant — this
         is what the algorithm last decided).
      2. Most recent reconciliation row date (older basket build).
      3. Most recent account_summary mtime (last time we touched IBKR).
    """
    targets = _read_csv_or_parquet(_TARGETS_DIR / "etf_targets_monthly")
    if targets is not None and not targets.empty:
        date_col = _first_existing_col(targets, ["rebalance_date", "date"])
        if date_col is not None:
            dates = pd.to_datetime(targets[date_col], errors="coerce").dropna()
            if not dates.empty:
                return dates.max().date()

    recon_df = _read_csv_or_parquet(_RECON_DIR / f"{profile}_reconciliation")
    if recon_df is not None and not recon_df.empty and "date" in recon_df.columns:
        dates = pd.to_datetime(recon_df["date"], errors="coerce").dropna()
        if not dates.empty:
            return dates.max().date()

    files = sorted(_ACCOUNT_DIR.glob(f"{profile}_account_summary_*.csv"))
    if not files:
        return None
    ts = _extract_timestamp(files[-1])
    return ts.date() if ts is not None else None


def get_active_optimizer(profile: str = "paper") -> str:  # noqa: ARG001
    """Return the active optimizer name for the selected profile.

    The workspace only ships a single momentum-based optimizer, so this returns
    a fixed default. Screens can still key on the return value so they stay
    source-compatible with the cloud dashboard.
    """
    return _DEFAULT_OPTIMIZER_NAME


def get_local_active_optimizer_for_attribution(profile: str = "paper") -> str | None:
    """Optional local override for attribution. Always defers to the default."""
    return get_active_optimizer(profile)


@st.cache_data(show_spinner=False)
def get_historical_nav(profile: str = "paper") -> pd.DataFrame:
    """Return a DataFrame with columns ``date`` and ``nav`` from local snapshots."""
    curve = build_equity_curve()
    if curve.empty:
        return pd.DataFrame(columns=["date", "nav"])

    out = curve.copy()
    out["date"] = pd.to_datetime(out["timestamp"]).dt.normalize()
    out = out[["date", "nav"]].sort_values("date").drop_duplicates("date", keep="last")
    return out.reset_index(drop=True)


def _load_latest_nav(profile: str = "paper") -> float | None:
    nav_df = get_historical_nav(profile)
    if nav_df.empty:
        return None
    return float(nav_df["nav"].iloc[-1])


@st.cache_data(show_spinner=False, ttl=_LIVE_TTL)
def get_position_data_for_date(
    audit_date: _date | pd.Timestamp | str | None = None,
    *,
    profile: str = "paper",
) -> pd.DataFrame:
    """Recompute current target / current / delta against the freshest data.

    This is intentionally NOT keyed off the reconciliation CSV anymore — that
    file only gets rewritten on rebalance days, which means it goes stale
    between rebalances. Instead we read:

      * latest target weights from ``etf_targets_monthly.parquet``
      * current positions from ``<profile>_positions_snapshot.csv``
      * latest close from ``etf_prices_master.parquet``
      * latest NAV from ``<profile>_account_summary_*.csv``

    and compute target_shares = round(target_weight * NAV / price), delta =
    target − current. This makes the Portfolio Composition tab reflect what
    the algorithm wants *right now* rather than what it wanted at the last
    rebalance.

    ``audit_date`` is accepted for API compatibility with the engine
    signature but is otherwise unused — the function always returns the
    most-recent picture.
    """
    _ = audit_date  # API-compat with engine; we always render "now".

    targets = _read_csv_or_parquet(_TARGETS_DIR / "etf_targets_monthly")
    positions = load_positions_snapshot()
    prices = load_latest_prices()
    if targets is None or targets.empty or prices is None or prices.empty:
        return pd.DataFrame()

    targets = targets.copy()
    targets.columns = [str(c).lower().strip() for c in targets.columns]
    date_col = _first_existing_col(targets, ["rebalance_date", "date"])
    sym_col = _first_existing_col(targets, ["symbol", "ticker"])
    weight_col = _first_existing_col(targets, ["target_weight", "weight"])
    if date_col is None or sym_col is None or weight_col is None:
        return pd.DataFrame()

    targets["_date"] = pd.to_datetime(targets[date_col], errors="coerce")
    targets["_symbol"] = targets[sym_col].astype(str).str.upper().str.strip()
    targets["_weight"] = _to_num(targets[weight_col]).fillna(0.0)
    targets = targets.dropna(subset=["_date"])
    if targets.empty:
        return pd.DataFrame()

    latest_rebalance = targets["_date"].max()
    latest_targets = targets[targets["_date"] == latest_rebalance].copy()

    # Latest close per symbol from the master price file.
    px = prices.copy()
    px["symbol"] = px["symbol"].astype(str).str.upper().str.strip()
    px["date"] = pd.to_datetime(px["date"], errors="coerce")
    latest_close = (
        px.dropna(subset=["date"])
        .sort_values(["symbol", "date"])
        .groupby("symbol", as_index=False)
        .tail(1)[["symbol", "close"]]
        .rename(columns={"close": "price"})
    )

    # Current shares from the positions snapshot (default 0).
    if positions is None or positions.empty:
        current_map: dict[str, float] = {}
    else:
        pos = positions.copy()
        pos["symbol"] = pos["symbol"].astype(str).str.upper().str.strip()
        current_map = dict(zip(pos["symbol"], _to_num(pos["shares"]).fillna(0.0)))

    # Latest NAV (drives target $).
    nav: float | None = None
    files = sorted(_ACCOUNT_DIR.glob(f"{profile}_account_summary_*.csv"))
    if files:
        try:
            df = pd.read_csv(files[-1])
            nav = _extract_tag_value(
                df, ["NetLiquidation", "EquityWithLoanValue"]
            )
        except Exception:
            nav = None
    if nav is None or nav <= 0:
        # Fall back: gross MV from positions × latest close.
        if current_map:
            tmp = (
                latest_close.assign(
                    shares=latest_close["symbol"].map(current_map).fillna(0.0)
                )
            )
            nav = float((tmp["price"] * tmp["shares"]).abs().sum())
        else:
            nav = 0.0

    # Build the UNION of current holdings + target symbols. Without this,
    # symbols that the user holds but the algorithm wants to fully exit
    # (e.g. last month's picks) wouldn't appear in the table — and the
    # downstream "sum of deltas equals zero" sanity-check (cash balance
    # row) would be off by the value of those exited positions.
    target_map = dict(zip(latest_targets["_symbol"], latest_targets["_weight"]))
    all_symbols = sorted(set(target_map.keys()) | set(current_map.keys()))

    rows = []
    for sym in all_symbols:
        weight = float(target_map.get(sym, 0.0))
        current = float(current_map.get(sym, 0.0))
        # Latest close from master prices.
        match = latest_close.loc[latest_close["symbol"] == sym, "price"]
        price = float(match.iloc[0]) if not match.empty else float("nan")
        target_value = weight * float(nav or 0.0)
        if price and price > 0 and weight > 0:
            target_shares = round(target_value / price)
        elif weight == 0:
            target_shares = 0.0  # algorithm wants to fully exit this symbol
        else:
            target_shares = float("nan")
        delta_shares = target_shares - current
        delta_value = (delta_shares * price) if price and not pd.isna(price) else 0.0
        ticker = sym if sym.endswith("-US") else f"{sym}-US"
        rows.append({
            "ticker": ticker,
            "weight": weight,
            "delta_shares": delta_shares,
            "target_shares": target_shares,
            "current_shares": current,
            "price": price,
            "delta_value": delta_value,
            "target_value": target_value,
        })

    out = pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)
    return out


@st.cache_data(show_spinner=False)
def get_historical_weights(
    optimizer_name: str | None = None,  # noqa: ARG001
    *,
    profile: str = "paper",  # noqa: ARG001
) -> pd.DataFrame:
    """Return wide-format historical target weights keyed by rebalance date."""
    targets = _read_csv_or_parquet(_TARGETS_DIR / "etf_targets_monthly")
    if targets is None or targets.empty:
        return pd.DataFrame()

    df = targets.copy()
    df.columns = [str(c).lower().strip() for c in df.columns]
    date_col = _first_existing_col(df, ["rebalance_date", "date"])
    symbol_col = _first_existing_col(df, ["symbol", "ticker"])
    weight_col = _first_existing_col(df, ["target_weight", "weight"])
    if date_col is None or symbol_col is None or weight_col is None:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["symbol"] = df[symbol_col].astype(str).str.upper()
    df["target_weight"] = _to_num(df[weight_col]).fillna(0.0)
    df = df.dropna(subset=["date"])

    pivot = df.pivot_table(
        index="date",
        columns="symbol",
        values="target_weight",
        fill_value=0.0,
        aggfunc="sum",
    )
    pivot = pivot.sort_index().reset_index()
    pivot.columns = [
        "date" if str(col).lower() == "date" else f"{col}-US" for col in pivot.columns
    ]
    return pivot


@st.cache_data(show_spinner=False)
def get_spx_prices_from_date(start_date: _date | pd.Timestamp | str) -> pd.DataFrame:
    """Return SPY close prices (used as an SPX proxy) from ``start_date``."""
    prices = load_latest_prices()
    if prices.empty:
        return pd.DataFrame(columns=["date", "price"])

    try:
        start_ts = pd.to_datetime(start_date)
    except Exception:
        return pd.DataFrame(columns=["date", "price"])

    spy = prices[prices["symbol"] == "SPY"].copy()
    if spy.empty:
        return pd.DataFrame(columns=["date", "price"])

    spy = spy[pd.to_datetime(spy["date"]) >= start_ts]
    out = spy[["date", "close"]].rename(columns={"close": "price"}).reset_index(drop=True)
    return out


# ── Backtest + attribution helpers (return empty frames when no data) ────────

_ATTRIBUTION_COLUMNS = [
    "date",
    "ticker",
    "gross_pnl",
    "slippage_cost",
    "commission_cost",
    "net_pnl",
    "contribution_bps",
]

_TRADES_COLUMNS = [
    "event_date",
    "execution_date",
    "ticker",
    "pre_trade_value",
    "pre_trade_weight",
    "target_value",
    "target_weight",
    "post_trade_value",
    "post_trade_weight",
    "trade_notional",
    "trade_direction",
    "slippage_cost",
    "commission_cost",
    "cost_bps",
]


def _attribution_paths(profile: str, kind: str) -> list[Path]:
    """Candidate paths for portfolio attribution / trades artifacts."""
    return [
        _REPORTS_DIR / f"{profile}_{kind}.parquet",
        _REPORTS_DIR / f"{profile}_{kind}.csv",
        _RUNS_DIR / f"{profile}_{kind}.parquet",
        _RUNS_DIR / f"{profile}_{kind}.csv",
    ]


def _read_first_existing(paths: list[Path]) -> pd.DataFrame:
    for path in paths:
        if not path.exists():
            continue
        try:
            if path.suffix == ".parquet":
                return pd.read_parquet(path)
            if path.suffix == ".csv":
                return pd.read_csv(path)
        except Exception:
            continue
    return pd.DataFrame()


@st.cache_data(show_spinner=False)
def get_portfolio_backtest_attribution(
    optimizer_name: str | None = None,  # noqa: ARG001
    *,
    profile: str = "paper",
) -> pd.DataFrame:
    df = _read_first_existing(_attribution_paths(profile, "backtest_attribution"))
    if df.empty:
        return pd.DataFrame(columns=_ATTRIBUTION_COLUMNS)
    return df


@st.cache_data(show_spinner=False)
def get_portfolio_backtest_trades(
    optimizer_name: str | None = None,  # noqa: ARG001
    *,
    profile: str = "paper",
) -> pd.DataFrame:
    df = _read_first_existing(_attribution_paths(profile, "backtest_trades"))
    if df.empty:
        return pd.DataFrame(columns=_TRADES_COLUMNS)
    return df


@st.cache_data(show_spinner=False)
def get_actual_portfolio_attribution(profile: str = "paper") -> pd.DataFrame:
    df = _read_first_existing(_attribution_paths(profile, "actual_attribution"))
    if df.empty:
        return pd.DataFrame(columns=_ATTRIBUTION_COLUMNS)
    return df


@st.cache_data(show_spinner=False)
def get_actual_portfolio_trades(profile: str = "paper") -> pd.DataFrame:
    df = _read_first_existing(_attribution_paths(profile, "actual_trades"))
    if df.empty:
        return pd.DataFrame(columns=_TRADES_COLUMNS)
    return df


def _latest_audit_from_attribution(df: pd.DataFrame) -> _date | None:
    if df is None or df.empty:
        return None
    if "date" not in df.columns:
        return None
    dates = pd.to_datetime(df["date"], errors="coerce").dropna()
    return dates.max().date() if not dates.empty else None


def get_latest_actual_portfolio_audit(profile: str = "paper") -> _date | None:
    return _latest_audit_from_attribution(get_actual_portfolio_attribution(profile))


def get_latest_portfolio_attribution_audit(
    *,
    profile: str = "paper",
    optimizer_name: str | None = None,
) -> _date | None:
    return _latest_audit_from_attribution(
        get_portfolio_backtest_attribution(optimizer_name, profile=profile)
    )


@st.cache_data(show_spinner=False)
def get_portfolio_backtest(
    optimizer_name: str | None = None,  # noqa: ARG001
    *,
    profile: str = "paper",
) -> pd.DataFrame:
    """Return portfolio backtest equity curve (date, equity)."""
    paths = [
        _REPORTS_DIR / f"{profile}_portfolio_backtest.parquet",
        _REPORTS_DIR / f"{profile}_portfolio_backtest.csv",
    ]
    df = _read_first_existing(paths)
    if df.empty:
        return pd.DataFrame(columns=["date", "equity"])
    df.columns = [str(c).lower() for c in df.columns]
    if "date" in df.columns and "equity" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["equity"] = _to_num(df["equity"])
        return df.dropna(subset=["date", "equity"])
    return pd.DataFrame(columns=["date", "equity"])


@st.cache_data(show_spinner=False)
def get_model_backtest(
    model: str | None = None,
    *,
    profile: str = "paper",  # noqa: ARG001
) -> pd.DataFrame:
    """Return per-model backtest equity/returns if available.

    Looks for model-specific artifacts under ``artifacts/reports/`` with filename
    conventions ``model_backtest_<model>.{parquet,csv}`` or a single
    ``model_backtest.{parquet,csv}`` with a ``model`` column that we filter by.
    """
    if model:
        paths = [
            _REPORTS_DIR / f"model_backtest_{model}.parquet",
            _REPORTS_DIR / f"model_backtest_{model}.csv",
        ]
        direct = _read_first_existing(paths)
        if not direct.empty:
            return direct

    paths = [
        _REPORTS_DIR / "model_backtest.parquet",
        _REPORTS_DIR / "model_backtest.csv",
    ]
    df = _read_first_existing(paths)
    if df.empty:
        return pd.DataFrame()
    if model and "model" in df.columns:
        return df[df["model"].astype(str) == model].reset_index(drop=True)
    return df


# ── Stubs for engine-parity Model Analysis screen ────────────────────────────
#
# The workspace currently ships a single optimizer (``momentum_top5``) and has
# no per-model marginal backtest artifacts. These helpers mirror the engine's
# API surface so the Model Analysis screen can render gracefully with the
# data that *is* available locally, falling back to empty frames / None when
# simulations artifacts are missing.


def get_latest_simulations_audit(profile: str = "paper") -> _date | None:
    """Return the most recent simulations audit date, if available.

    The workspace treats reconciliation-derived dates as the source of truth
    for the latest audit. A dedicated ``simulations_audit.csv`` under
    ``artifacts/reports/<profile>/`` takes precedence if present.
    """
    sim_paths = [
        _REPORTS_DIR / profile / "simulations_audit.parquet",
        _REPORTS_DIR / profile / "simulations_audit.csv",
        _REPORTS_DIR / f"{profile}_simulations_audit.parquet",
        _REPORTS_DIR / f"{profile}_simulations_audit.csv",
    ]
    df = _read_first_existing(sim_paths)
    if not df.empty and "date" in df.columns:
        dates = pd.to_datetime(df["date"], errors="coerce").dropna()
        if not dates.empty:
            return dates.max().date()
    return get_latest_production_audit(profile)


def get_production_audit_models(profile: str = "paper") -> list[str]:  # noqa: ARG001
    """Return the list of known models for the Model Analysis selector."""
    models_dir = _REPORTS_DIR / "models"
    discovered: list[str] = []
    if models_dir.exists():
        for path in sorted(models_dir.glob("model_backtest_*.parquet")) + sorted(
            models_dir.glob("model_backtest_*.csv")
        ):
            stem = path.stem.replace("model_backtest_", "")
            if stem and stem not in discovered:
                discovered.append(stem)
    if not discovered:
        discovered = [_DEFAULT_OPTIMIZER_NAME]
    return discovered


def get_production_audit_optimizers(profile: str = "paper") -> list[str]:  # noqa: ARG001
    """Return the list of available optimizer names."""
    return [_DEFAULT_OPTIMIZER_NAME]


def get_model_backtest_metrics(
    model: str | None = None,
    *,
    profile: str = "paper",
) -> pd.DataFrame:
    """Return cached metrics for a model backtest, if present."""
    df = get_model_backtest(model, profile=profile)
    if df is None or df.empty:
        return pd.DataFrame()
    return df


def get_portfolio_backtest_metrics(
    optimizer_name: str | None = None,
    *,
    profile: str = "paper",
) -> pd.DataFrame:
    df = get_portfolio_backtest(optimizer_name, profile=profile)
    if df is None or df.empty:
        return pd.DataFrame()
    return df


@st.cache_data(show_spinner=False)
def get_reduced_portfolio_backtest(
    removed_model: str,  # noqa: ARG001
    *,
    optimizer_name: str | None = None,  # noqa: ARG001
    profile: str = "paper",
) -> pd.DataFrame:
    """Return a reduced portfolio backtest (baseline with one model removed).

    Not produced locally yet, so this returns the full portfolio backtest as a
    graceful fallback. Downstream marginal calculations will then show zero
    marginal contribution, which is preferable to crashing the UI.
    """
    return get_portfolio_backtest(optimizer_name, profile=profile)


def get_latest_audit_date(profile: str = "paper") -> _date | None:
    """Alias for ``get_latest_production_audit`` kept for engine parity."""
    return get_latest_production_audit(profile)


# ── Synthetic NAV reconstruction ─────────────────────────────────────────────
#
# The dashboard often only has 1-2 ``paper_account_summary_*.csv`` snapshots,
# which means the equity curve is just a straight line between two points.
# These helpers reconstruct a daily NAV by walking the *current* position
# vector through historical close prices. The result is approximate (it
# assumes today's position vector held throughout the window), but it gives a
# real daily curve and real metrics for short windows.


@st.cache_data(show_spinner=False, ttl=_LIVE_TTL)
def synthesize_nav_history(
    profile: str = "paper",
    *,
    cash: float | None = None,
    start: _date | pd.Timestamp | str | None = None,
) -> pd.DataFrame:
    """Reconstruct a daily NAV time series from the latest position vector
    and the master price file.

    Returns a DataFrame with columns ``date``, ``nav``, ``daily_return``.
    Returns an empty frame when positions or prices are unavailable.
    """
    positions = load_positions_snapshot()
    prices = load_latest_prices()
    if positions is None or positions.empty or prices is None or prices.empty:
        return pd.DataFrame(columns=["date", "nav", "daily_return"])

    if cash is None:
        # Pull cash from the latest account summary if available, else 0.
        try:
            files = sorted(_ACCOUNT_DIR.glob(f"{profile}_account_summary_*.csv"))
            if files:
                df = pd.read_csv(files[-1])
                cash = _extract_tag_value(
                    df, ["TotalCashValue", "AvailableFunds", "CashBalance"]
                ) or 0.0
            else:
                cash = 0.0
        except Exception:
            cash = 0.0

    # Wide-format prices: date × symbol → close.
    held = positions[positions["shares"] != 0][["symbol", "shares"]].copy()
    held["symbol"] = held["symbol"].astype(str).str.upper()
    if held.empty:
        return pd.DataFrame(columns=["date", "nav", "daily_return"])

    px = prices.copy()
    px["symbol"] = px["symbol"].astype(str).str.upper()
    px["date"] = pd.to_datetime(px["date"]).dt.normalize()
    px = px[px["symbol"].isin(held["symbol"])].sort_values(["symbol", "date"])

    if px.empty:
        return pd.DataFrame(columns=["date", "nav", "daily_return"])

    if start is not None:
        try:
            start_ts = pd.to_datetime(start).normalize()
            px = px[px["date"] >= start_ts]
        except Exception:
            pass

    # Forward-fill prices so a symbol with a missing day still contributes.
    wide = (
        px.pivot_table(index="date", columns="symbol", values="close", aggfunc="last")
        .sort_index()
        .ffill()
    )
    held_map = dict(zip(held["symbol"], held["shares"]))
    cols = [c for c in wide.columns if c in held_map]
    if not cols:
        return pd.DataFrame(columns=["date", "nav", "daily_return"])

    shares_vec = pd.Series(held_map).reindex(cols).astype(float)
    market_value = wide[cols].mul(shares_vec, axis=1).sum(axis=1)
    nav = market_value + float(cash or 0.0)

    out = pd.DataFrame({"date": nav.index, "nav": nav.values})
    out["date"] = pd.to_datetime(out["date"])
    out = out.dropna(subset=["nav"]).sort_values("date").reset_index(drop=True)
    out["daily_return"] = out["nav"].pct_change().fillna(0.0)
    return out


def get_paper_start_date(profile: str = "paper") -> _date | None:
    """Return the earliest date this paper account has any record.

    Prefer the earliest ``<profile>_account_summary_*.csv`` mtime (since the
    file's filename embeds the timestamp), then fall back to the earliest
    reconciliation row date. Returns ``None`` when neither exists.
    """
    files = sorted(_ACCOUNT_DIR.glob(f"{profile}_account_summary_*.csv"))
    earliest_file = files[0] if files else None
    if earliest_file is not None:
        ts = _extract_timestamp(earliest_file)
        if ts is not None:
            return ts.date()

    recon = _read_csv_or_parquet(_RECON_DIR / f"{profile}_reconciliation")
    if recon is not None and not recon.empty and "date" in recon.columns:
        dates = pd.to_datetime(recon["date"], errors="coerce").dropna()
        if not dates.empty:
            return dates.min().date()
    return None


def best_available_nav_history(
    profile: str = "paper",
    *,
    extend_history: bool = False,
) -> pd.DataFrame:
    """Pick the richest NAV series available.

    By default the series is anchored to the actual paper-account start date
    (earliest ``<profile>_account_summary_*.csv``). When ``extend_history`` is
    True the synthesized series is allowed to extend back as far as the
    master price file goes — useful for a "backtest of current allocation"
    view, but **not** the user's real paper history.

    Returns columns ``date``, ``nav``, plus ``source`` indicating whether each
    row came from a real account snapshot or was synthesized.
    """
    start = None if extend_history else get_paper_start_date(profile)

    snapshots = get_historical_nav(profile)
    synthesized = synthesize_nav_history(profile, start=start)

    snap = snapshots.copy() if snapshots is not None and not snapshots.empty else pd.DataFrame()
    synth = (
        synthesized[["date", "nav"]].copy()
        if synthesized is not None and not synthesized.empty
        else pd.DataFrame()
    )

    if not snap.empty:
        snap["date"] = pd.to_datetime(snap["date"]).dt.normalize()
        snap["source"] = "snapshot"
    if not synth.empty:
        synth["date"] = pd.to_datetime(synth["date"]).dt.normalize()
        synth["source"] = "synthesized"

    if snap.empty and synth.empty:
        return pd.DataFrame(columns=["date", "nav", "source"])
    if snap.empty:
        return synth.sort_values("date").reset_index(drop=True)
    if synth.empty:
        return snap.sort_values("date").reset_index(drop=True)

    # Snapshots win on dates where both exist (real > synthesized).
    snap_dates = set(snap["date"].tolist())
    synth_only = synth.loc[~synth["date"].isin(snap_dates)]
    combined = pd.concat([snap, synth_only], ignore_index=True)
    combined = combined.sort_values("date").reset_index(drop=True)
    return combined