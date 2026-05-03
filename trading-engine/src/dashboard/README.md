# Capital Fund Dashboard

A FastAPI backend with a vanilla-JS frontend. Replaces the previous Streamlit
shell. No build step. Charts via Chart.js loaded from CDN.

## Layout

```
src/dashboard/
  app.py              FastAPI app - run this with uvicorn
  _cache.py           Drop-in replacement for the streamlit cache decorator
  routers/            JSON routes (one file per nav page + sidebar.py)
  services/           Broker, order, pipeline, risk, state - unchanged
  utils/              data_loaders.py, slippage.py - unchanged
  static/             index.html, style.css, js/{main, lib/*, pages/*}.js
```

## Run

```
cd /Users/tradingworkspace/TradingWorkspace/trading-engine
poetry install                       # ensures fastapi + uvicorn are installed
uvicorn src.dashboard.app:app --host 127.0.0.1 --port 8501 --reload
```

Open http://127.0.0.1:8501/ . API docs are at `/api/docs`.

## What changed

- **Streamlit is gone.** `streamlit_entrypoint.py` is a deprecation stub that
  prints a redirect message. Every interactive feature is now an HTTP route
  under `/api/...` rendered by the JS frontend.
- **No emojis anywhere** in the new UI. Status cells use plain `PASS / WARN /
  FAIL` tags; the sidebar uses words instead of icons.
- **Same data, same files.** `services/` and `utils/` are reused unchanged;
  the only edit is replacing `import streamlit as st` with a tiny shim
  (`src/dashboard/_cache.py`) so the cache decorator keeps working without
  Streamlit installed.
- **Cache lives in-process.** `cache_data.clear()` is wired to the sidebar's
  Clear cache button and to the Run-Audit / Refresh-ETF actions, just like
  before.

## Cleanup

The new app does not import any of the legacy Streamlit code. After you have
verified the new dashboard works on real data, run:

```
rm src/dashboard/streamlit_entrypoint.py
rm -rf src/dashboard/.streamlit src/dashboard/screens
rm src/dashboard/Dockerfile src/dashboard/docker-compose.yml
rm src/dashboard/requirements.txt   # only if you weren't using it elsewhere
```

(I could not run `rm` from this session - the workspace mount is read-only
for deletes - so the files are still on disk as deprecation stubs.)

## Pages

| URL hash      | Backend route                        | Replaces                    |
| ------------- | ------------------------------------ | --------------------------- |
| `#/portfolio` | `/api/portfolio/summary`             | screens/paper_performance.py |
| `#/model`     | `/api/model/{meta,models,prices}`    | screens/backtest_charts.py   |
| `#/asset`     | `/api/asset/{positions,composition,attribution}` | screens/portfolio_analytics.py + positions.py + paper_composition.py + portfolio_attribution.py |
| `#/slippage`  | `/api/slippage/{meta,report}`        | screens/slippage_analysis.py |
| `#/audit`     | `/api/audit/run`                     | screens/system_audit.py      |
