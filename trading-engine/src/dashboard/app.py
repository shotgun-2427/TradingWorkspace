"""
app.py - Capital Fund Dashboard, FastAPI edition.

Replaces the Streamlit shell. Mounts a tiny vanilla-JS frontend under ``/`` and
exposes JSON routes under ``/api/``.

Run:
    cd /Users/tradingworkspace/TradingWorkspace/trading-engine
    uvicorn src.dashboard.app:app --host 127.0.0.1 --port 8501 --reload
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _build_app() -> FastAPI:
    application = FastAPI(
        title="Capital Fund Dashboard",
        version="2.0.0",
        docs_url="/api/docs",
        redoc_url=None,
    )

    # JSON routes. Imported here so module-level startup is cheap.
    from src.dashboard.routers import (
        asset,
        audit,
        model,
        portfolio,
        sidebar,
        slippage,
    )

    application.include_router(sidebar.router, prefix="/api", tags=["sidebar"])
    application.include_router(portfolio.router, prefix="/api/portfolio", tags=["portfolio"])
    application.include_router(model.router, prefix="/api/model", tags=["model"])
    application.include_router(asset.router, prefix="/api/asset", tags=["asset"])
    application.include_router(slippage.router, prefix="/api/slippage", tags=["slippage"])
    application.include_router(audit.router, prefix="/api/audit", tags=["audit"])

    @application.get("/api/health")
    def health() -> dict:
        return {"ok": True, "version": application.version}

    static_dir = Path(__file__).parent / "static"
    application.mount(
        "/static",
        StaticFiles(directory=str(static_dir)),
        name="static",
    )

    # During development we --reload on every code change. The browser
    # otherwise hangs onto cached copies of style.css / *.js. Tell it not to.
    @application.middleware("http")
    async def no_cache_static(request: Request, call_next):
        response = await call_next(request)
        path = request.url.path
        if path == "/" or path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response

    # Cache-buster: stamp every server start so even an aggressive browser
    # cache (looking at you Safari) is forced to refetch /static/*.
    import time as _time

    cache_token = str(int(_time.time()))

    @application.get("/")
    def root() -> HTMLResponse:
        html = (static_dir / "index.html").read_text(encoding="utf-8")
        html = html.replace("/static/style.css", f"/static/style.css?v={cache_token}")
        html = html.replace("/static/js/main.js", f"/static/js/main.js?v={cache_token}")
        return HTMLResponse(content=html)

    @application.get("/favicon.ico")
    def favicon() -> JSONResponse:
        return JSONResponse({}, status_code=204)

    return application


app = _build_app()
