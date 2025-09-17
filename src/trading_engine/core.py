import datetime
from typing import List, Union, Dict, Sequence

import polars as pl
from hawk_backtester import HawkBacktester
from polars import LazyFrame, DataFrame

from common.constants import ProcessingMode
from common.logging import setup_logger
from trading_engine.model_state import FEATURES
from trading_engine.models import MODELS
from trading_engine.aggregators import AGGREGATORS
from trading_engine.optimizers import PORTFOLIO_OPTIMIZERS
from trading_engine.utils import calculate_calendar_lookback

logger = setup_logger(__name__)

pl.enable_string_cache()


def read_data() -> LazyFrame:
    """Read the raw data from the Parquet file in lazy mode."""
    BUCKET = "wsb-hc-qasap-bucket-1"
    PREFIX = "hcf/raw_data/factset_equities_ohlcv_snapshot"
    parquet_uri = f"gs://{BUCKET}/{PREFIX}/parquet/part-*.parquet"
    return pl.scan_parquet(parquet_uri)


def create_model_state(
    lf: LazyFrame,
    features: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
    universe: List[str],
    registry=None,
) -> tuple[DataFrame, DataFrame]:
    """
    Build model state with warmup lookback and eager feature support.
    Returns (model_state, prices) tuple.
    """
    if not registry:
        registry = FEATURES

    lazy_fns, eager_fns = [], []
    for f in features:
        if f not in registry:
            continue
        entry = registry[f]
        (lazy_fns if entry["mode"] == ProcessingMode.LAZY else eager_fns).append(
            entry["func"]
        )

    # Filter to the date range + lookback buffer
    lookback_days = _max_feature_lookback(features)
    lookback_calendar_days = calculate_calendar_lookback(lookback_days)
    buffer_start_date = start_date - datetime.timedelta(days=lookback_calendar_days)

    lf = (
        lf.with_columns(pl.col("date").dt.date().alias("date"))
        .filter(pl.col("date").is_between(buffer_start_date, end_date))
        .sort(["ticker", "date"])
    )

    # Apply lazy feature functions
    for feature_fn in lazy_fns:
        lf = feature_fn(lf)

    # Materialize once; streaming helps on large backfill
    out = lf.collect(engine="streaming")

    # Apply eager feature functions
    for feature_fn in eager_fns:
        out = feature_fn(out)

    # Trim warmup and finalize
    model_state = (
        out.filter(
            (pl.col("date") >= pl.lit(start_date))
            & (pl.col("date") <= pl.lit(end_date))
        )
        .sort(["ticker", "date"])
        .rechunk()
    )

    prices = construct_prices(model_state, universe)

    return model_state, prices


def _build_model_lazy_input(
    model_state: DataFrame, tickers: List[str], columns: List[str]
) -> pl.LazyFrame:
    cols = ["date", "ticker", *columns]
    return model_state.lazy().filter(pl.col("ticker").is_in(tickers)).select(cols)


def _ensure_lazy(obj: Union[pl.DataFrame, pl.LazyFrame]) -> pl.LazyFrame:
    if isinstance(obj, pl.LazyFrame):
        return obj
    if isinstance(obj, pl.DataFrame):
        return obj.lazy()
    raise TypeError(f"Model runner returned unsupported type: {type(obj)}")


def _coerce_weights_to_float(lf: pl.LazyFrame) -> pl.LazyFrame:
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

    add_zero = [pl.lit(0.0).alias(t) for t in missing]
    lf2 = lf.with_columns(add_zero) if add_zero else lf

    return lf2.select(["date", *universe])


def _max_feature_lookback(features: Sequence[str]) -> int:
    return max((FEATURES[f]["lookback"] for f in features), default=0)


def orchestrate_model_backtests(
    model_state: DataFrame,
    models: List[str],
    universe: List[str],
    clamp_bounds: tuple[float, float] = (-1.0, 1.0),
    registry=MODELS,
) -> Dict[str, pl.LazyFrame]:
    """
    Run selected models and return per-model LazyFrames padded to the full universe:
      { model_name: LazyFrame(["date"] + universe) }, weights in [-1, 1].
    No aggregation or L1 normalization here.
    """
    lo, hi = clamp_bounds
    results: Dict[str, pl.LazyFrame] = {}

    for name in models:
        if name not in registry:
            raise KeyError(f"Unknown model: {name}")

        spec = registry[name]
        tickers: List[str] = spec["tickers"]  # input tickers needed by this model
        columns: List[str] = spec["columns"]
        runner = spec["function"]  # Callable[[LazyFrame], DataFrame|LazyFrame]

        lf_in = _build_model_lazy_input(model_state, tickers=tickers, columns=columns)
        out = _ensure_lazy(runner(lf_in))  # ["date", <traded tickers...>]
        out = _coerce_weights_to_float(out)  # float weights
        out = _clamp_weights(out, lo, hi)  # clamp to bounds
        out = _pad_to_universe(out, universe)  # add missing tickers as 0.0, reorder

        results[name] = out

    return results


def construct_prices(model_state: DataFrame, universe: List[str]) -> pl.DataFrame:
    prices = (
        model_state.pivot(index="date", on="ticker", values="adjusted_close_1d")
        .sort("date")
        .with_columns(pl.col("date").cast(pl.String))
        .fill_null(strategy="forward")
        .fill_null(strategy="backward")
    )

    # Keep only ['date'] + universe (in that order). If some tickers aren't present, add them as null then fill.
    present = [c for c in prices.columns if c in universe]
    missing = [t for t in universe if t not in prices.columns]

    if missing:
        prices = prices.with_columns([pl.lit(None).alias(t) for t in missing])

    prices = (
        prices.select(["date", *universe])
        .fill_null(strategy="forward")
        .fill_null(strategy="backward")
    )
    return prices


def orchestrate_model_simulations(
    prices: DataFrame,
    model_insights: Dict[str, pl.LazyFrame],
    initial_value: float = 1_000_000.0,
) -> Dict[str, dict]:
    """
    Runs all backtests and returns { model_name: backtest_result }.
    No cross-model aggregation/weighting here.
    """
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
    return lf2.with_columns(
        [(pl.col(c) * pl.col("_scale")).alias(c) for c in cols]
    ).drop(["_l1", "_scale"])


def orchestrate_portfolio_aggregation(
    model_insights: Dict[str, pl.LazyFrame],
    backtest_results: Dict[str, dict],
    universe: List[str],
    aggregators: List[str],
    clamp_bounds: tuple[float, float] = (-1.0, 1.0),
    l1_budget: float = 1.0,
    registry=AGGREGATORS,
) -> Dict[str, DataFrame]:
    """
    Aggregate model insights into a single portfolio per aggregator.
    Returns { aggregator_name: wide DataFrame of weights }.

    Post-processing (padding, float coercion, clamping, L1 budget) happens here.
    """
    results: Dict[str, DataFrame] = {}
    for name in aggregators:
        if name not in registry:
            raise KeyError(f"Unknown aggregator: {name}")

        aggregator_fn = registry[name][
            "function"
        ]  # Callable[[Dict[str, LF], Dict], LF]
        raw = aggregator_fn(model_insights, backtest_results)  # LazyFrame or DataFrame

        lf = _ensure_lazy(raw)
        lf = _coerce_weights_to_float(lf)
        lf = _pad_to_universe(lf, universe)
        lf = _clamp_weights(lf, *clamp_bounds)
        lf = _enforce_l1_budget(lf, budget=l1_budget)

        results[name] = lf.collect()

    return results


def orchestrate_portfolio_optimizations(
    prices: DataFrame,
    aggregated_insights: Dict[str, DataFrame],
    universe: List[str],
    optimizers: List[str],
    clamp_bounds: tuple[float, float] = (-1.0, 1.0),
    l1_budget: float = 1.0,
    registry=PORTFOLIO_OPTIMIZERS,
) -> Dict[str, DataFrame]:
    """
    Run asset-level portfolio optimizers on aggregated desired weights.
    Assumes a single aggregated portfolio input for MVP; uses the first one if multiple.
    Returns { optimizer_name: wide DataFrame of weights }.
    """
    if not aggregated_insights:
        return {}

    # MVP: use the first aggregated set of weights
    agg_name, agg_df = next(iter(aggregated_insights.items()))

    results: Dict[str, DataFrame] = {}
    for name in optimizers:
        if name not in registry:
            raise KeyError(f"Unknown portfolio optimizer: {name}")

        optimizer_fn = registry[name][
            "function"
        ]  # Callable[[DataFrame, DataFrame, dict|None], LF]
        raw = optimizer_fn(prices, agg_df, None)  # LazyFrame or DataFrame

        lf = _ensure_lazy(raw)
        lf = _coerce_weights_to_float(lf)
        lf = _pad_to_universe(lf, universe)
        lf = _clamp_weights(lf, *clamp_bounds)
        lf = _enforce_l1_budget(lf, budget=l1_budget)

        results[name] = lf.collect()

    return results


def orchestrate_portfolio_simulations(
    prices: DataFrame,
    portfolio_insights: Dict[str, DataFrame],
    initial_value: float = 1_000_000.0,
):
    backtester = HawkBacktester(initial_value)
    results: Dict[str, dict] = {}

    for name, weights_df in portfolio_insights.items():
        weights = weights_df

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


def run_full_backtest(
    universe: list[str],
    features: list[str],
    models: list[str],
    aggregators: list[str],
    portfolio_optimizers: list[str] | None,
    start_date: datetime.date,
    end_date: datetime.date,
    initial_value: int = 1_000_000,
    model_registry: dict = MODELS,
    aggregator_registry: dict = AGGREGATORS,
    portfolio_optimizer_registry: dict = PORTFOLIO_OPTIMIZERS,
):
    """Complete backtest orchestration (models → aggregation → optimization)."""

    # Load data and create model state
    lf = read_data()
    model_state, prices = create_model_state(
        lf=lf,
        features=features,
        start_date=start_date,
        end_date=end_date,
        universe=universe,
    )

    # Run models
    model_results = orchestrate_model_backtests(
        model_state=model_state,
        models=models,
        universe=universe,
        registry=model_registry,
    )

    # Run model simulations
    model_simulations = orchestrate_model_simulations(
        prices=prices, model_insights=model_results, initial_value=initial_value
    )

    # Run aggregation
    aggregated_results = orchestrate_portfolio_aggregation(
        model_insights=model_results,
        backtest_results=model_simulations,
        universe=universe,
        aggregators=aggregators,
        registry=aggregator_registry,
    )

    # Simulate aggregated portfolios
    aggregation_simulations = orchestrate_portfolio_simulations(
        prices=prices,
        portfolio_insights=aggregated_results,
        initial_value=initial_value,
    )

    # Optional: run asset-level optimization on top of aggregated weights
    optimizer_results: Dict[str, DataFrame] = {}
    optimizer_simulations: Dict[str, dict] = {}
    if portfolio_optimizers:
        optimizer_results = orchestrate_portfolio_optimizations(
            prices=prices,
            aggregated_insights=aggregated_results,
            universe=universe,
            optimizers=portfolio_optimizers,
            registry=portfolio_optimizer_registry,
        )
        optimizer_simulations = orchestrate_portfolio_simulations(
            prices=prices,
            portfolio_insights=optimizer_results,
            initial_value=initial_value,
        )

    return {
        "model_simulations": model_simulations,
        "aggregation_results": aggregated_results,
        "aggregation_simulations": aggregation_simulations,
        "optimizer_results": optimizer_results,
        "optimizer_simulations": optimizer_simulations,
    }
