"""
Per-ETF backtest pipeline.

Produces equity curves for **each (ticker, model)** pair in the registered
per-ETF universe and writes them to:

    data/research/per_etf/<TICKER>/<MODEL>.parquet

The dashboard's Backtester → Engine backtests tab reads these files through
``/api/model/per-etf-backtest`` so the cascading "pick an ETF, then a model"
flow shows real backtest curves instead of the indexed-price fallback.

Models
------
Today the pipeline backs each of the following:

  * ``amma``                          — AMMATrend (Kaufman adaptive moving average)
  * ``amma_mirror``                   — AMMATrend with inverted score (counter-trend)
  * ``momentum``                      — MomentumSignal (12-1 style)
  * ``natr_mean_reversion``           — NATRMeanReversion
  * ``inverse_momentum_mean_reversion``— InverseMomentumMeanReversion
  * ``ensemble``                      — default ensemble of the four signals

All four signal classes share the same shape: ``compute(prices, as_of)``
returns a ``DataFrame[symbol, raw, score]`` where ``score`` is a
cross-sectional z-score across the eligible universe.

Backtest convention (single-ticker, long-only)
----------------------------------------------
For each model we walk the master prices on a **monthly rebalance** cadence,
call ``compute(prices, as_of)`` once per rebalance, extract this ticker's
score, and map it to a single-ticker weight using:

    weight = clip(score / 2.0, 0, 1)              # default
    weight = clip(-score / 2.0, 0, 1)             # amma_mirror

z=2 → fully invested, z=1 → half invested, below-average → flat.

Equity is then the standard ``cumprod(1 + w.shift(1) * daily_return)`` so
there is no look-ahead — the weight set on day t earns the day-(t+1) return.

Why long-only, why monthly
--------------------------
This is a *research view* of how a single model performs on a single ETF,
not a live PnL backtest. Long-only matches the current production system's
no-short policy. Monthly rebalance keeps the compute cost down (we call
each model O(N_dates / 21) times) and matches the production cadence so
the curves the dashboard shows are roughly comparable to what a real run
would have produced.

Slippage and commissions are intentionally omitted — these are research
benchmarks, not live PnL projections.

Run
---
    python -m src.research.per_etf_backtest --all
    python -m src.research.per_etf_backtest --tickers SPY QQQ --models tsmom

Add ``--quiet`` to suppress per-(ticker, model) progress lines.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

# Force the project root onto sys.path so this file works as both a `python
# -m src.research.per_etf_backtest` invocation and an ad-hoc script.
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.strategies.etf.adaptive_trend import AdaptiveTrend
from src.strategies.etf.amma import AMMATrend
from src.strategies.etf.buy_and_hold import BuyAndHold
from src.strategies.etf.ensemble import default_ensemble
from src.strategies.etf.inverse_momentum_mean_reversion import (
    InverseMomentumMeanReversion,
)
from src.strategies.etf.momentum import MomentumSignal
from src.strategies.etf.natr_mean_reversion import NATRMeanReversion
from src.strategies.etf.trend_filter import TrendFilter
from src.strategies.etf.tsmom_ts import TSMomentum
from src.strategies.etf.vol_target_trend import VolTargetTrend


# ── Configuration ───────────────────────────────────────────────────────────


PROJECT_ROOT = _ROOT
PRICES_PATH = PROJECT_ROOT / "data" / "market" / "cleaned" / "prices" / "etf_prices_master.parquet"
OUTPUT_ROOT = PROJECT_ROOT / "data" / "research" / "per_etf"

# Default rebalance interval in trading days (≈ 1 month).
REBALANCE_INTERVAL = 21

# Burn-in: don't trade for the first BURN_IN bars to let lookbacks fill.
BURN_IN = 252


def _model_factory(name: str):
    """Map a model id → fresh instance. Centralized so the dropdown labels
    in the dashboard always have a canonical builder."""
    if name == "amma":
        return AMMATrend()
    if name == "amma_mirror":
        # Same signal; sign of the score is flipped at backtest time.
        return AMMATrend()
    if name == "momentum":
        return MomentumSignal()
    if name == "natr_mean_reversion":
        return NATRMeanReversion()
    if name == "inverse_momentum_mean_reversion":
        return InverseMomentumMeanReversion()
    if name == "ensemble":
        return default_ensemble()
    # Time-series / benchmark models — added to beat the cross-sectional
    # signals on a single-ticker view. See strategies/etf/{buy_and_hold,
    # trend_filter, tsmom_ts, vol_target_trend}.py for the math.
    if name == "buy_and_hold":
        return BuyAndHold()
    if name == "trend_filter":
        return TrendFilter()
    if name == "tsmom_ts":
        return TSMomentum()
    if name == "vol_target_trend":
        return VolTargetTrend()
    if name == "adaptive_trend":
        return AdaptiveTrend()
    raise ValueError(f"Unknown model id {name!r}")


MODEL_IDS: list[str] = [
    "amma",
    "amma_mirror",
    "momentum",
    "natr_mean_reversion",
    "inverse_momentum_mean_reversion",
    "ensemble",
    # Time-series models. ``buy_and_hold`` is the reference benchmark — every
    # other model in the dashboard is expected to beat it on Sharpe.
    "buy_and_hold",
    "trend_filter",
    "tsmom_ts",
    "vol_target_trend",
    "adaptive_trend",
]


# ── Score → weight (one knob, easy to swap) ─────────────────────────────────


def _score_to_weight(score: float, *, mirror: bool = False) -> float:
    """Cross-sectional z-score → long-only single-ticker weight.

    z=2 (well above the universe mean) → fully invested.
    z<=0 → flat.

    Mirror mode flips the sign — used for ``amma_mirror`` where positive AMMA
    is bullish trend-continuation and we want the counter-trend view.
    """
    if score is None or not np.isfinite(score):
        return 0.0
    s = -float(score) if mirror else float(score)
    return float(max(0.0, min(1.0, s / 2.0)))


# ── Data loaders ────────────────────────────────────────────────────────────


def _load_prices() -> pd.DataFrame:
    """Return the master parquet as a long-format frame with date / symbol / close."""
    if not PRICES_PATH.exists():
        raise FileNotFoundError(f"Price master not found at {PRICES_PATH}")
    df = pd.read_parquet(PRICES_PATH, columns=["date", "symbol", "close"])
    df["date"] = pd.to_datetime(df["date"])
    df["symbol"] = df["symbol"].astype(str).str.upper()
    return df.dropna(subset=["close"]).sort_values(["symbol", "date"])


def _universe(prices: pd.DataFrame) -> list[str]:
    return sorted(prices["symbol"].unique().tolist())


# ── Single backtest run (one model, all tickers) ────────────────────────────


def backtest_one_model(
    prices_long: pd.DataFrame,
    model,
    *,
    model_id: str,
    universe: list[str],
    rebalance_interval: int = REBALANCE_INTERVAL,
    burn_in: int = BURN_IN,
    quiet: bool = False,
) -> dict[str, pd.DataFrame]:
    """Run ``model`` against each ticker in ``universe`` and return a dict
    mapping ticker -> DataFrame with columns [date, weight, daily_return,
    equity, drawdown]."""
    wide = prices_long.pivot_table(
        index="date", columns="symbol", values="close", aggfunc="last"
    ).sort_index()
    # Daily simple returns aligned to the wide panel. fill_method=None so
    # we don't pad NAs forward — that would silently turn "no data yet" into
    # zero returns (and skew the early-year stats for newly-listed ETFs).
    returns = wide.pct_change(fill_method=None).fillna(0.0)

    all_dates = list(wide.index)
    if len(all_dates) <= burn_in:
        return {}

    # Rebalance dates: every N trading days starting at burn_in.
    rebalance_dates = all_dates[burn_in::rebalance_interval]
    mirror = model_id == "amma_mirror"

    # Pre-allocate per-ticker weight series (forward-fill between rebalances).
    weights = pd.DataFrame(0.0, index=all_dates, columns=universe)

    t0 = time.time()
    last_pct = -1
    for i, as_of in enumerate(rebalance_dates):
        try:
            scores = model.compute(prices_long, pd.Timestamp(as_of))
        except Exception as exc:
            if not quiet:
                print(f"  [{model_id}] compute() failed at {as_of}: {exc}", file=sys.stderr)
            continue
        if scores is None or scores.empty:
            continue
        # Map per-ticker score → weight.
        scored = dict(zip(scores["symbol"].astype(str).str.upper(), scores["score"]))
        # Apply the weight at this rebalance date, forward-fill until the
        # next rebalance with the assignment below.
        for sym in universe:
            w = _score_to_weight(scored.get(sym), mirror=mirror)
            weights.at[as_of, sym] = w

        if not quiet:
            pct = int(100.0 * (i + 1) / len(rebalance_dates))
            if pct != last_pct and pct % 10 == 0:
                print(f"  [{model_id}] {pct}% ({i + 1}/{len(rebalance_dates)} rebalances)")
                last_pct = pct

    # Forward-fill: outside rebalance dates inherit the prior assignment.
    # We track this by marking rebalance rows and filling 0s forward only
    # AFTER the first rebalance.
    rebalance_index = pd.DatetimeIndex(rebalance_dates)
    mask = pd.Series(False, index=all_dates)
    mask.loc[rebalance_index] = True
    weights_filled = weights.copy()
    # Replace pre-rebalance zeros with NaN so .ffill respects the gap...
    weights_filled = weights_filled.where(mask.values[:, None] | (weights != 0))
    weights_filled = weights_filled.ffill().fillna(0.0)

    # Strategy daily return = weight_yesterday * asset_return_today.
    # shift(1) avoids look-ahead.
    strat_ret = weights_filled.shift(1).fillna(0.0) * returns
    equity = (1.0 + strat_ret).cumprod()

    out: dict[str, pd.DataFrame] = {}
    for sym in universe:
        rets = strat_ret[sym]
        eq = equity[sym]
        run_max = eq.cummax()
        dd = (eq / run_max) - 1.0
        df = pd.DataFrame({
            "date": eq.index,
            "weight": weights_filled[sym].values,
            "daily_return": rets.values,
            "equity": eq.values,
            "drawdown": dd.values,
        })
        out[sym] = df.reset_index(drop=True)

    if not quiet:
        dur = time.time() - t0
        print(f"  [{model_id}] done in {dur:.1f}s ({len(out)} tickers)")
    return out


# ── Summary metrics + persistence ───────────────────────────────────────────


_TRADING_DAYS = 252


def _summary_metrics(df: pd.DataFrame) -> dict:
    """Sharpe / total return / annualized return / max DD / # signals."""
    if df.empty:
        return {}
    rets = df["daily_return"].astype(float)
    eq = df["equity"].astype(float)
    n = max(int((rets != 0).any() and len(rets)) or 0, 0)
    daily_mean = float(rets.mean()) if len(rets) > 1 else 0.0
    daily_std = float(rets.std()) if len(rets) > 1 else 0.0
    sharpe = (
        daily_mean * _TRADING_DAYS / (daily_std * (_TRADING_DAYS ** 0.5))
        if daily_std > 0
        else None
    )
    total_return = float(eq.iloc[-1] - 1.0) if len(eq) else None
    years = (df["date"].iloc[-1] - df["date"].iloc[0]).days / 365.25 if len(df) > 1 else 0
    annualized_return = (float(eq.iloc[-1]) ** (1.0 / years) - 1.0) if years > 0.25 and eq.iloc[-1] > 0 else None
    max_dd = float(df["drawdown"].min()) if "drawdown" in df else None
    signals = int((df["weight"].diff().fillna(df["weight"]).abs() > 1e-9).sum())
    return {
        "sharpe_ratio": sharpe,
        "total_return": total_return,
        "annualized_return": annualized_return,
        "max_drawdown": max_dd,
        "num_signals": signals,
        "years_observed": years,
    }


def _save(out_dir: Path, ticker: str, model_id: str, df: pd.DataFrame) -> Path:
    target_dir = out_dir / ticker
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{model_id}.parquet"
    df.to_parquet(path, index=False)
    return path


def _save_summary(out_dir: Path, ticker: str, summaries: dict[str, dict]) -> Path:
    """Persist per-model summaries, **merging** with any existing file.

    Earlier runs may have populated other model entries — overwriting
    blindly would clobber them and the dashboard would show "no data" for
    models we still have parquet artifacts for. Merge instead.
    """
    target_dir = out_dir / ticker
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / "summary.json"
    existing: dict[str, dict] = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text())
        except Exception:  # noqa: BLE001
            existing = {}
    existing.update(summaries)
    path.write_text(json.dumps(existing, indent=2, default=float))
    return path


# ── Top-level driver ────────────────────────────────────────────────────────


def run_pipeline(
    *,
    tickers: Iterable[str] | None = None,
    models: Iterable[str] | None = None,
    rebalance_interval: int = REBALANCE_INTERVAL,
    burn_in: int = BURN_IN,
    out_dir: Path = OUTPUT_ROOT,
    quiet: bool = False,
) -> dict:
    prices = _load_prices()
    universe = _universe(prices)
    selected_tickers = list(tickers) if tickers else universe
    selected_models = list(models) if models else MODEL_IDS

    if not quiet:
        print(f"Loaded {len(prices):,} bars across {len(universe)} symbols. "
              f"Backtesting {len(selected_models)} models × {len(selected_tickers)} tickers "
              f"(rebalance every {rebalance_interval}d, burn-in {burn_in}d).")

    out_dir.mkdir(parents=True, exist_ok=True)
    summaries_by_ticker: dict[str, dict[str, dict]] = {}

    for model_id in selected_models:
        if not quiet:
            print(f"\n=== {model_id} ===")
        model = _model_factory(model_id)
        per_ticker_dfs = backtest_one_model(
            prices_long=prices,
            model=model,
            model_id=model_id,
            universe=universe,
            rebalance_interval=rebalance_interval,
            burn_in=burn_in,
            quiet=quiet,
        )
        for ticker in selected_tickers:
            df = per_ticker_dfs.get(ticker)
            if df is None or df.empty:
                continue
            _save(out_dir, ticker, model_id, df)
            summary = _summary_metrics(df)
            summaries_by_ticker.setdefault(ticker, {})[model_id] = summary

    for ticker, summaries in summaries_by_ticker.items():
        _save_summary(out_dir, ticker, summaries)

    if not quiet:
        wrote = sum(len(v) for v in summaries_by_ticker.values())
        print(f"\nDone. Wrote {wrote} (ticker, model) artifact pairs to {out_dir}.")
    return {
        "tickers": list(summaries_by_ticker.keys()),
        "models": selected_models,
        "out_dir": str(out_dir),
    }


def _main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else "")
    p.add_argument("--all", action="store_true", help="Backtest every ticker × every model (default).")
    p.add_argument("--tickers", nargs="*", help="Subset of tickers (uppercase).")
    p.add_argument("--models", nargs="*", choices=MODEL_IDS, help="Subset of models.")
    p.add_argument("--quiet", action="store_true", help="Suppress per-model progress lines.")
    p.add_argument("--rebalance-interval", type=int, default=REBALANCE_INTERVAL)
    p.add_argument("--burn-in", type=int, default=BURN_IN)
    args = p.parse_args(argv)

    run_pipeline(
        tickers=args.tickers,
        models=args.models,
        rebalance_interval=args.rebalance_interval,
        burn_in=args.burn_in,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    _main()
