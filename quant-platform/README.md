# Quant Platform

A local-first quantitative trading platform with modular components for:

- **Data ingestion** from Financial Modeling Prep (FMP)
- **Feature generation** and model-driven portfolio weights
- **Local backtesting** with reusable metrics
- **Execution** against Interactive Brokers TWS via `ib_insync`
- **Visualization** through a Streamlit dashboard

## Quickstart

```bash
pip install -r requirements.txt
cp config/secrets.example.yaml config/secrets.yaml
python scripts/download_prices.py
python scripts/run_backtest.py
streamlit run src/dashboard/app.py
```

## Running the trading engine

```bash
python scripts/run_trading_engine.py
```

## Environment variables

```bash
export FMP_API_KEY="INSERT_FMP_API_KEY"
```

## Artifacts

Backtest and production artifacts are written to local filesystem only:

- `artifacts/simulations/`
- `artifacts/production_runs/YYYY-MM-DD/`

No cloud services are used.
