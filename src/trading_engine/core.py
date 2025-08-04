import datetime
from typing import List, Optional, Union, Dict, Sequence

import polars as pl
from hawk_backtester import HawkBacktester
from polars import LazyFrame, DataFrame

from common.constants import ProcessingMode
from trading_engine.model_state import FEATURES
from trading_engine.models import MODELS
from trading_engine.optimizers import OPTIMIZERS
from trading_engine.utils import calculate_calendar_lookback

pl.enable_string_cache()

# ---------------------------
# Global shared model state & prices
# ---------------------------
MODEL_STATE: Optional[pl.DataFrame] = None
PRICES: Optional[pl.DataFrame] = None


def set_model_state(df: pl.DataFrame) -> None:
    """Replace the global model state atomically."""
    global MODEL_STATE
    MODEL_STATE = df.rechunk()  # compact buffers for faster slicing


def get_model_state() -> pl.DataFrame:
    assert MODEL_STATE is not None, "MODEL_STATE not built yet. Call create_model_state(...) first."
    return MODEL_STATE


def set_prices(df: pl.DataFrame) -> None:
    """Replace the global prices atomically."""
    global PRICES
    PRICES = df.rechunk()  # compact buffers for faster slicing


def get_prices() -> pl.DataFrame:
    assert PRICES is not None, "PRICES not built yet. Call create_model_state_and_prices(...) first."
    return PRICES


def read_data() -> LazyFrame:
    """Read the raw data from the Parquet file in lazy mode."""
    BUCKET = "wsb-hc-qasap-bucket-1"
    PREFIX = "hcf/raw_data/factset_equities_ohlcv_snapshot"
    parquet_uri = f"gs://{BUCKET}/{PREFIX}/parquet/part-*.parquet"

    return pl.scan_parquet(parquet_uri)


def create_model_state(
        lf: LazyFrame, features: list[str], start_date: datetime.date, end_date: datetime.date, universe: List[str],
        catalogue=None
) -> tuple[DataFrame, DataFrame]:
    """
    Build model state with warmup lookback and eager feature support.
    Returns an eager DataFrame (to store in memory).
    """
    if not catalogue:
        catalogue = FEATURES

    lazy_fns, eager_fns = [], []
    for f in features:
        if f not in catalogue:
            continue
        entry = catalogue[f]
        (lazy_fns if entry["mode"] == ProcessingMode.LAZY else eager_fns).append(entry["func"])

    # Filter to the date range + lookback buffer
    lookback_days = _max_feature_lookback(features)
    lookback_calendar_days = calculate_calendar_lookback(lookback_days)
    buffer_start_date = start_date - datetime.timedelta(days=lookback_calendar_days)

    lf = (
        lf
        .with_columns(pl.col("date").dt.date().alias("date"))
        .filter(pl.col("date").is_between(buffer_start_date, end_date))
        .sort(["ticker", "date"])
    )

    # Apply lazy feature functions
    for feature_fn in lazy_fns:
        lf = feature_fn(lf)  # Callable[[LazyFrame], LazyFrame]

    # Materialize once; streaming helps on large backfill
    out = lf.collect(engine="streaming")

    # Apply eager feature functions (DataFrame -> DataFrame)
    for feature_fn in eager_fns:
        out = feature_fn(out)

    # Trim warmup and finalize
    out = (
        out.filter((pl.col("date") >= pl.lit(start_date)) & (pl.col("date") <= pl.lit(end_date)))
        .sort(["ticker", "date"])
        .rechunk()
    )

    set_model_state(out)
    prices = construct_prices(universe)
    set_prices(prices)

    return out, prices


def _build_model_lazy_input(tickers: List[str], columns: List[str]) -> pl.LazyFrame:
    base = get_model_state().clone()
    cols = ["date", "ticker", *columns]
    return (
        base.lazy()
        .filter(pl.col("ticker").is_in(tickers))
        .select(cols)
    )


def _ensure_lazy(obj: Union[pl.DataFrame, pl.LazyFrame]) -> pl.LazyFrame:
    if isinstance(obj, pl.LazyFrame):
        return obj
    if isinstance(obj, pl.DataFrame):
        return obj.lazy()
    raise TypeError(f"Model runner returned unsupported type: {type(obj)}")


def _coerce_weights_to_float(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Cast all non-'date' columns to Float64 to avoid int/float mixing
    cols = [c for c in lf.collect_schema().names() if c != "date"]
    if not cols:
        return lf
    return lf.with_columns([pl.col(cols).cast(pl.Float64)])


def _clamp_weights(lf: pl.LazyFrame, lo: float = -1.0, hi: float = 1.0) -> pl.LazyFrame:
    cols = [c for c in lf.collect_schema().names() if c != "date"]
    if not cols:
        return lf
    return lf.with_columns(pl.col(cols).clip(lo, hi))


def _pad_to_universe(lf: pl.LazyFrame, universe: List[str]) -> pl.LazyFrame:
    """
    Ensure lf has exactly ['date'] + universe columns:
      - add missing universe tickers as 0.0
      - drop any extra weight columns not in universe
      - order columns as ['date'] + universe
    """
    current = lf.collect_schema().names()
    weight_cols = [c for c in current if c != "date"]
    missing = [t for t in universe if t not in weight_cols]
    extra = [c for c in weight_cols if c not in universe]

    add_zero = [pl.lit(0.0).alias(t) for t in missing]
    lf2 = lf.with_columns(add_zero) if add_zero else lf

    # Select in desired order and drop extras
    return lf2.select(["date", *universe])


def _max_feature_lookback(features: Sequence[str]) -> int:
    return max((FEATURES[f]["lookback"] for f in features), default=0)


def orchestrate_model_backtests(
        models: List[str],
        universe: List[str],
        clamp_bounds: tuple[float, float] = (-1.0, 1.0),
        catalogue=None,
) -> Dict[str, pl.LazyFrame]:
    """
    Run selected models and return per-model LazyFrames padded to the full universe:
      { model_name: LazyFrame(["date"] + universe) }, weights in [-1, 1].
    No aggregation or L1 normalization here.
    """
    if not catalogue:
        catalogue = MODELS

    lo, hi = clamp_bounds
    results: Dict[str, pl.LazyFrame] = {}

    for name in models:
        if name not in catalogue:
            raise KeyError(f"Unknown model: {name}")

        spec = catalogue[name]
        tickers: List[str] = spec["tickers"]  # input tickers needed by this model
        columns: List[str] = spec["columns"]
        runner = spec["function"]  # Callable[[LazyFrame], DataFrame|LazyFrame]

        lf_in = _build_model_lazy_input(tickers=tickers, columns=columns)
        out = _ensure_lazy(runner(lf_in))  # ["date", <traded tickers...>]
        out = _coerce_weights_to_float(out)  # float weights
        out = _clamp_weights(out, lo, hi)  # clamp to bounds
        out = _pad_to_universe(out, universe)  # add missing tickers as 0.0, reorder

        results[name] = out

    return results


def construct_prices(universe: List[str]) -> pl.DataFrame:
    base = get_model_state().clone()

    prices = (
        base.pivot(index="date", on="ticker", values="adjusted_close_1d")
        .sort("date")
        .with_columns(pl.col("date").cast(pl.String))
        .fill_null(strategy="forward")
        .fill_null(strategy="backward")
    )

    # Keep only ['date'] + universe (in that order). If some tickers aren’t present, add them as null then fill.
    present = [c for c in prices.columns if c in universe]
    missing = [t for t in universe if t not in prices.columns]

    if missing:
        prices = prices.with_columns([pl.lit(None).alias(t) for t in missing])

    prices = prices.select(["date", *universe]).fill_null(strategy="forward").fill_null(strategy="backward")
    return prices


def orchestrate_model_simulations(
        model_insights: Dict[str, pl.LazyFrame],
        initial_value: float = 1_000_000.0,
) -> Dict[str, dict]:
    """
    Runs all backtests and returns { model_name: backtest_result }.
    No cross-model aggregation/weighting here.
    """
    prices = get_prices()

    backtester = HawkBacktester(initial_value)
    results: Dict[str, dict] = {}

    for name, lf in model_insights.items():
        weights = lf.collect()

        if weights.schema.get("date") != pl.String:
            weights = weights.with_columns(pl.col("date").cast(pl.String))

        price_cols = [c for c in prices.columns if c != "date"]
        if set(price_cols) != set(weights.columns) - {"date"}:
            # Add missing as 0.0, drop extras, enforce order
            missing = [c for c in price_cols if c not in weights.columns]
            if missing:
                weights = weights.with_columns([pl.lit(0.0).alias(c) for c in missing])
            weights = weights.select(["date", *price_cols]).fill_null(0.0)

        results[name] = backtester.run(prices, weights)

    return results


def _enforce_l1_budget(lf: pl.LazyFrame, budget: float = 1.0) -> pl.LazyFrame:
    cols = [c for c in lf.collect_schema().names() if c != "date"]
    if not cols:
        return lf
    l1 = pl.sum_horizontal([pl.col(c).abs() for c in cols]).alias("_l1")
    lf2 = lf.with_columns(l1).with_columns(
        pl.when(pl.col("_l1") > budget)
        .then(budget / pl.col("_l1"))
        .otherwise(1.0)
        .alias("_scale")
    )
    return lf2.with_columns([(pl.col(c) * pl.col("_scale")).alias(c) for c in cols]).drop(["_l1", "_scale"])


def orchestrate_portfolio_backtests(
        optimizers: List[str],
        model_insights: Dict[str, pl.LazyFrame],
        backtest_results: Dict[str, dict],
        universe: List[str],
        clamp_bounds: tuple[float, float] = (-1.0, 1.0),
        l1_budget: float = 1.0,
        catalogue=None,
) -> Dict[str, DataFrame]:
    """
    Run portfolio optimizers on the model insights and per-model backtests.
    Returns { optimizer_name: backtest_result }.

    All post-processing (padding, float coercion, clamping, L1 budget, price alignment)
    happens here so optimizers stay minimal.
    """
    if not catalogue:
        catalogue = OPTIMIZERS

    results: Dict[str, DataFrame] = {}
    for name in optimizers:
        if name not in catalogue:
            raise KeyError(f"Unknown optimizer: {name}")

        optimizer_fn = catalogue[name]["function"]  # Callable[[Dict[str, LF], Dict], LF]
        raw = optimizer_fn(model_insights, backtest_results)  # LazyFrame or DataFrame

        lf = _ensure_lazy(raw)
        lf = _coerce_weights_to_float(lf)
        lf = _pad_to_universe(lf, universe)  # in theory should do nothing
        lf = _clamp_weights(lf, *clamp_bounds)
        lf = _enforce_l1_budget(lf, budget=l1_budget)

        results[name] = lf.collect()

    return results


def orchestrate_portfolio_simulations(
        portfolio_insights: Dict[str, DataFrame],
        initial_value: float = 1_000_000.0,
):
    prices = get_prices()

    backtester = HawkBacktester(initial_value)
    results: Dict[str, dict] = {}

    for name, lf in portfolio_insights.items():
        weights = lf

        if weights.schema.get("date") != pl.String:
            weights = weights.with_columns(pl.col("date").cast(pl.String))

        price_cols = [c for c in prices.columns if c != "date"]
        if set(price_cols) != set(weights.columns) - {"date"}:
            # Add missing as 0.0, drop extras, enforce order
            missing = [c for c in price_cols if c not in weights.columns]
            if missing:
                weights = weights.with_columns([pl.lit(0.0).alias(c) for c in missing])
            weights = weights.select(["date", *price_cols]).fill_null(0.0)

        results[name] = backtester.run(prices, weights)

    return results
