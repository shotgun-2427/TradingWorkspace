### Trading Engine: Architecture and Process Flow

This package provides a Polars‑centric, multi‑strategy pipeline. It transforms raw OHLCV data into model signals, aggregates those signals into desired portfolio weights, optionally optimizes those weights using asset risk models, and simulates results end‑to‑end.

## End‑to‑End Flow
```text
Raw Data → Model State → Models → Aggregation → (optional) Optimization → Simulation/Outputs
```

```mermaid
graph TD;
    A[Raw Data] -- prices --> B[Create Model State];
    B -- model state --> C1[Model A];
    B -- model state --> C2[Model B];
    B -- model state --> C3[Model N];
    C1 -- weights --> D[Aggregation];
    C2 -- weights --> D;
    C3 -- weights --> D;
    D -- desired weights --> E[Optimization];
    subgraph Risk;
      R[Risk Model];
    end;
    R -. cov_assets .-> E;
    R -. cov_models .-> D;
    E -- final weights --> F[Simulation and Outputs];
```

- **Data → Model State**
  - Read lazy price data and compute features into a tidy table (model state).
  - File: `trading_engine/core.py` → `read_data`, `create_model_state`.

- **Model Signals**
  - Each model consumes a subset of the model state and outputs daily weights per traded ticker.
  - Registry: `trading_engine/models/registry.py` (maps names → callables + input spec).
  - Contract: returns wide `['date', <tickers...>]`, weights in [-1, 1].

- **Aggregation (model‑wise)**
  - Combine per‑model weights into a single desired portfolio (e.g., equal‑weight, drawdown‑aware).
  - Registry: `trading_engine/aggregators/registry.py`.
  - Contract: input `{model_name: LazyFrame}` → one wide weights table. Orchestrator handles float coercion, universe padding, clamping, and L1 budget.

- **Optimization (asset‑level, optional)**
  - Refine aggregated weights using an asset risk model (covariances) to produce final portfolio weights.
  - Registry: `trading_engine/optimizers/registry.py`.
  - Contract: input `(prices_df, desired_weights_df, config)` → wide `LazyFrame`. Orchestrator applies clamp/L1.

- **Simulation**
  - Turn prices + weights into backtest results and summary metrics (`HawkBacktester`).

## Orchestration (Wiring)
File: `trading_engine/core.py`
- `create_model_state`: build `(model_state, prices)` from raw data.
- `orchestrate_model_backtests`: run models; normalize outputs to the universe.
- `orchestrate_model_simulations`: simulate each model’s standalone portfolio.
- `orchestrate_portfolio_aggregation`: apply aggregators; central clamp + L1.
- `orchestrate_portfolio_optimizations` (optional): apply optimizers; central post‑processing.
- `orchestrate_portfolio_simulations`: simulate aggregated/optimized portfolios.
- `run_full_backtest`: one‑shot entry that returns all artifacts.

## Components at a Glance
| Directory | Purpose | Registry |
|---|---|---|
| `trading_engine/models/` | Model signal generators | `models/registry.py` |
| `trading_engine/aggregators/` | Combine model signals into desired weights | `aggregators/registry.py` |
| `trading_engine/optimizers/` | Refine desired weights using risk models | `optimizers/registry.py` |
| `trading_engine/risk/` | Covariance and risk model providers | `risk/registry.py` |
| `trading_engine/core.py` | Orchestration helpers and full pipeline | — |

## Data & Weight Conventions
- **Shape**: wide tables with `date` (string) + tickers (universe order).
- **Post‑processing**: orchestrator clamps to [-1, 1] and applies an L1 budget to keep components simple and consistent.
- **Alignment**: orchestrator pads/drops/reorders tickers to match the configured universe.

## Configuration & Production
- **Config**: `src/production/paper/config.yaml`
  - **keys**: `model_state_features`, `models`, `aggregators`, optional `portfolio_optimizers`, `universe`, `start_date`, `end_date`.
- **Entrypoint**: `src/production/paper/main.py`
  - Runs the full pipeline, persists artifacts (e.g., to GCS), and prepares execution instructions.

## Extending the System
- **Add a model**: implement under `models/catalogue/`, register in `models/registry.py`.
- **Add an aggregator**: implement under `aggregators/catalogue/`, register in `aggregators/registry.py`.
- **Add an optimizer**: implement under `optimizers/catalogue/`, register in `optimizers/registry.py`.
- **Add a risk model**: implement under `risk/catalogue/`, register in `risk/registry.py`.

## Research Usage
- Notebook: `src/research/04_quickstart_v2.ipynb` (end‑to‑end v2 quickstart).
- Prefer `run_full_backtest` for a single‑call pipeline with explicit inputs.

## Optimizers (Purpose)
- Optimizers take aggregated, model‑wise desired weights and refine them using an asset‑level risk model (e.g., covariance), balancing expected return, risk, and optional tracking to the aggregated portfolio. Result: a risk‑aware final portfolio that preserves the core signal structure.

## Notes & Assumptions
- **Global clamps/budget** are applied centrally after aggregation/optimization. Component‑specific constraints (e.g., long‑only) can be added per component or via registry flags.
- **Risk inputs** use daily log returns and rolling windows by default; alternate horizons/frequencies can be added via the risk registry.


