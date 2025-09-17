### Goals
- **Rename and reframe current “optimizer” to “aggregator”**: combines per-model desired weights into a single portfolio using cross-model structure.
- **Introduce a new true portfolio optimizer**: runs an asset-level mean-variance optimization (MVO) using asset correlations, taking the aggregator’s output as target/desired weights.
- **Preserve current behavior by default** while enabling correlation-aware extensions.

### Current architecture (high-level)
- **Models**: produce per-date, per-ticker weights as LazyFrames, normalized and padded in the orchestrator.
- **Optimizers (to-be Aggregators)**: combine model outputs (e.g., equal-weight, min-avg-drawdown).
- **Orchestrator**: centralizes formatting — float coercion, padding to universe, clamping, L1 budget, materialization.

Code touchpoints:
```234:246:src/trading_engine/core.py
def orchestrate_portfolio_backtests(
        model_insights: Dict[str, pl.LazyFrame],
        backtest_results: Dict[str, dict],
        universe: List[str],
        optimizers: List[str],
        clamp_bounds: tuple[float, float] = (-1.0, 1.0),
        l1_budget: float = 1.0,
        registry=OPTIMIZERS,
) -> Dict[str, DataFrame]:
    """
    Run portfolio optimizers on the model insights and per-model backtests.
    """
# ... rest of function ...
```
```1:11:src/trading_engine/optimizers/registry.py
from trading_engine.optimizers.catalogue.equal_weight import EqualWeightOptimizer
from trading_engine.optimizers.catalogue.min_avg_drawdown import MinAvgDrawdownOptimizer

OPTIMIZERS = {
    "equal_weight": {
        "function": EqualWeightOptimizer(),
    },
    "min_avg_drawdown": {
        "function": MinAvgDrawdownOptimizer(window_days=252)
    }
}
```

### Proposed target architecture
- **`trading_engine/aggregators/`**
  - `registry.py` exports `AGGREGATORS` (moved/renamed from current `OPTIMIZERS`).
  - `catalogue/` contains aggregation strategies (migrated from current ones; add correlation-aware strategies later).
  - Interface unchanged: `Callable[[Dict[str, LazyFrame], Dict], LazyFrame]` producing per-date wide weights.
- **`trading_engine/optimizers/` (true, asset-level)**
  - `registry.py` exports `PORTFOLIO_OPTIMIZERS`.
  - `catalogue/mean_variance.py`: asset-level MVO optimizer.
  - Interface: `Callable[[pl.DataFrame, pl.DataFrame, dict | None], LazyFrame]`
    - inputs: `prices` (wide DataFrame), `desired_weights` (wide DF from aggregator), optional `config`.
    - output: final per-date wide portfolio weights as LazyFrame.
- **`trading_engine/core.py`**
  - Rename `orchestrate_portfolio_backtests` → `orchestrate_portfolio_aggregation` (uses `AGGREGATORS`).
  - New `orchestrate_portfolio_optimizations` (uses `PORTFOLIO_OPTIMIZERS`).
  - `run_full_backtest`: models → model sims → aggregation → aggregation sims → optimization → optimization sims.
  - Maintain clamps/L1 budget centrally after each stage for consistency.

### Detailed steps
1) **Aggregator rename (non-breaking shim)**
- Create `src/trading_engine/aggregators/` and move current files:
  - `catalogue/equal_weight.py`, `catalogue/min_avg_drawdown.py`, `registry.py`.
  - Update imports to new package path.
- In `src/trading_engine/aggregators/registry.py`: export `AGGREGATORS` mirroring current `OPTIMIZERS` mapping.
- Update `src/trading_engine/core.py` to import `AGGREGATORS` and rename parameter names (`optimizers` → `aggregators`).
- Add temporary compatibility shim:
  - Keep `src/trading_engine/optimizers/__init__.py` and `registry.py` that re-export `AGGREGATORS` as `OPTIMIZERS` with a `DeprecationWarning`.

2) **New asset-level optimizer (MVO)**
- Create `src/trading_engine/optimizers/registry.py` for true portfolio optimizers with `PORTFOLIO_OPTIMIZERS = {"mean_variance": {"function": MeanVarianceOptimizer(...)}}`.
- Implement `src/trading_engine/optimizers/catalogue/mean_variance.py`:
  - Inputs:
    - `prices: pl.DataFrame` wide with `date` + universe (from `construct_prices`).
    - `desired_weights: pl.DataFrame` wide with `date` + universe (aggregator output after orchestrator post-processing).
    - `config: dict | None` (`cov_window_days`, `risk_aversion`, `clamp_bounds`, `l1_budget`, optional `target_return` or `min_var` mode).
  - Processing:
    - Compute rolling window returns from `prices`.
    - For each `date` after warmup:
      - Estimate covariance Σ (rolling sample cov; optional shrinkage later).
      - Solve regularized tracking MV: minimize 0.5 wᵀΣw + λ ||w − w_target||².
        - Closed form: w = (Σ + 2λ I)⁻¹ (2λ w_target).
      - Apply clamping, then L1-budget normalization to match orchestrator semantics.
    - Return a lazy frame with `date` + weights in universe order.
  - Keep dependency-light (NumPy for linear algebra), full type annotations, Sphinx docstrings.

3) **Orchestrator updates in `core.py`**
- `orchestrate_model_backtests`: unchanged.
- `orchestrate_model_simulations`: unchanged.
- Replace:
  - `orchestrate_portfolio_backtests` → `orchestrate_portfolio_aggregation(model_insights, backtest_results, universe, aggregators, ...)` using `AGGREGATORS`.
- Add:
  - `orchestrate_portfolio_optimizations(prices, aggregated_weights: Dict[str, pl.DataFrame], optimizers: List[str], ...)` returning `{ optimizer_name: pl.DataFrame }` with post-processing identical to aggregation stage.
- Extend `run_full_backtest`:
  - Accept `aggregators: list[str]` and `portfolio_optimizers: list[str]`.
  - Return groups: `model_simulations`, `aggregation_simulations`, `optimizer_simulations` (plus intermediate weights if useful).

4) **Types, docs, consistency**
- Add precise type annotations, Sphinx-style docstrings throughout.
- Keep central post-processing utilities: `_ensure_lazy`, `_coerce_weights_to_float`, `_pad_to_universe`, `_clamp_weights`, `_enforce_l1_budget`.
- Maintain L1 budget and clamping semantics across aggregation and optimization outputs.

5) **Backwards compatibility and migration**
- Provide `OPTIMIZERS` alias to `AGGREGATORS` with deprecation warning for one release.
- Keep `orchestrate_portfolio_backtests` as a thin wrapper calling the new aggregation function with a warning.
- Update notebooks and `production/paper/main.py` after validation.

### Low-complexity design choices
- Closed-form MVO with ridge regularization (no external solvers).
- Per-date loop (NumPy) for covariance and solve; clear and adequate for modest universes.
- Reuse existing orchestration for alignment/padding/clamping/L1-budget.

### Open questions
1) **Aggregator correlation basis**: Use correlation of each model’s simulated PnL/returns (preferred) vs correlation of raw weight series?
2) **Optimizer objective**: Confirm regularized tracking vs pure min-variance with equality constraints (e.g., sum weights == 1)?
3) **Constraints**: Keep global L1 budget and per-asset clamps only, or add net exposure, long-only, sector caps?
4) **Covariance window/returns**: Preferred window (e.g., 60 trading days) and return type (log vs simple)? Any shrinkage now (e.g., Ledoit–Wolf) or later?
5) **Frequency/alignment**: Optimize daily on close using `prices` dates? Any additional missing-price handling beyond current fills?
6) **Turnover/costs**: Add turnover penalty vs aggregator weights or realized weights now, or defer?
7) **Output normalization**: Keep L1 budget at 1.0 and allow shorting by default ([-1, 1] clamps)?
8) **Naming**: Confirm `trading_engine/aggregators/` + `trading_engine/optimizers/` split, with deprecation shim.

### Acceptance criteria
- Existing backtests run unchanged when using `aggregators=["equal_weight"]` and no portfolio optimizer.
- New path supports `portfolio_optimizers=["mean_variance"]` producing valid weights/simulations with default config.
- No breaking imports for notebooks/scripts; deprecation warnings only.
- Code is fully type-annotated with Sphinx docstrings; no new heavy deps (NumPy only).

### Next steps (after approval)
- Implement folder moves and shims; update imports in `core.py`.
- Add `mean_variance.py` and `PORTFOLIO_OPTIMIZERS` registry.
- Implement `orchestrate_portfolio_aggregation` and `orchestrate_portfolio_optimizations`; update `run_full_backtest`.
- Update docs/notebooks; validate on a small universe.
