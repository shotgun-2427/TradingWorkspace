"""
streamlit_entrypoint.py — Capital Fund Dashboard shell.

Run:
    cd /Users/tradingworkspace/TradingWorkspace/trading-engine
    streamlit run src/dashboard/streamlit_entrypoint.py --server.port 8501
"""
from __future__ import annotations

import os
import sys
from datetime import datetime
from importlib import import_module
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ── Page registry ─────────────────────────────────────────────────────────────

PAGE_REGISTRY: dict[str, str] = {
    "Home":                       "src.dashboard.screens.home",
    "Run Pipeline":               "src.dashboard.screens.run_pipeline",
    "Basket Review":              "src.dashboard.screens.basket_review",
    "Submit Orders":              "src.dashboard.screens.submit_orders",
    "Orders & Fills":             "src.dashboard.screens.live_orders",
    "Positions":                  "src.dashboard.screens.positions",
    "Risk Monitor":               "src.dashboard.screens.risk_monitor",
    "Portfolio Performance":      "src.dashboard.screens.paper_performance",
    "Backtest Charts":            "src.dashboard.screens.backtest_charts",
}

PAGE_GROUPS: dict[str, list[str]] = {
    "⚡ Workflow": [
        "Home",
        "Run Pipeline",
        "Basket Review",
        "Submit Orders",
        "Orders & Fills",
    ],
    "📊 Monitoring": [
        "Positions",
        "Risk Monitor",
    ],
    "🔬 Analytics": [
        "Portfolio Performance",
        "Backtest Charts",
    ],
}

PAGE_ICONS: dict[str, str] = {
    "Home":                  "🏠",
    "Run Pipeline":          "⚙️",
    "Basket Review":         "🧺",
    "Submit Orders":         "🚀",
    "Orders & Fills":        "📋",
    "Positions":             "📌",
    "Risk Monitor":          "⚠️",
    "Portfolio Performance": "📈",
    "Backtest Charts":       "🔬",
}

DEFAULT_PAPER_HOST = os.getenv("IBKR_HOST", "127.0.0.1")
DEFAULT_PAPER_PORT = int(os.getenv("IBKR_PORT_PAPER", "4002"))
DEFAULT_LIVE_PORT  = int(os.getenv("IBKR_PORT_LIVE",  "4001"))

# ── Session state ─────────────────────────────────────────────────────────────

def init_session_state() -> None:
    defaults: dict = {
        "mode":               "paper",
        "ibkr_paper_host":    DEFAULT_PAPER_HOST,
        "ibkr_paper_port":    DEFAULT_PAPER_PORT,
        "ibkr_live_port":     DEFAULT_LIVE_PORT,
        "selected_page":      "Home",
        "submit_locked":      False,
        "last_pipeline_run":  None,
        "last_submit_time":   None,
        "last_broker_refresh": None,
        "last_error":         None,
        "last_submit_action": None,
        "last_submit_result": None,
        "last_pipeline_action": None,
        "last_pipeline_result": None,
        "last_basket_path":   None,
        "active_run_id":      None,
        "append_result":      None,
        "targets_result":     None,
    }
    for key, val in defaults.items():
        st.session_state.setdefault(key, val)

# ── CSS ───────────────────────────────────────────────────────────────────────

def inject_css() -> None:
    st.markdown("""
<style>
/* ── Sidebar ── */
[data-testid="stSidebar"] { background: #0d1117; }
[data-testid="stSidebar"] .block-container {
    padding: 1.2rem 0.8rem 1rem 0.8rem;
}

/* Nav buttons */
[data-testid="stSidebar"] .stButton > button {
    width: 100%;
    text-align: left;
    border: none;
    border-radius: 8px;
    padding: 0.45rem 0.75rem;
    font-size: 0.9rem;
    background: transparent;
    color: #c9d1d9;
    margin-bottom: 2px;
    transition: background 0.15s;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(255,255,255,0.07);
    color: #fff;
}
[data-testid="stSidebar"] .stButton.active-page > button {
    background: rgba(99,179,237,0.15);
    color: #63b3ed;
    font-weight: 600;
}

/* Status pill */
.pill {
    display: inline-block;
    border-radius: 999px;
    padding: 2px 10px;
    font-size: 0.78rem;
    font-weight: 600;
    letter-spacing: 0.03em;
}
.pill-green { background: rgba(72,187,120,0.15); color: #68d391; }
.pill-red   { background: rgba(245,101,101,0.15); color: #fc8181; }
.pill-yellow{ background: rgba(246,173,85,0.15);  color: #f6ad55; }
.pill-blue  { background: rgba(99,179,237,0.15);  color: #63b3ed; }

/* Sidebar info card */
.sb-card {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 10px;
    padding: 0.75rem 0.85rem;
    margin: 0.5rem 0;
    font-size: 0.82rem;
}
.sb-card .sb-label {
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 0.68rem;
    color: #586069;
    margin-bottom: 0.5rem;
}
.sb-card .sb-row {
    display: flex;
    justify-content: space-between;
    margin: 0.2rem 0;
    color: #c9d1d9;
}
.sb-card .sb-row span:last-child { color: #fff; font-weight: 500; }

/* Section header inside sidebar */
.sb-section {
    font-size: 0.7rem;
    letter-spacing: 0.09em;
    text-transform: uppercase;
    color: #444d56;
    margin: 1rem 0 0.35rem 0.1rem;
}

/* Page content area */
.page-hero {
    padding: 0.5rem 0 1.25rem 0;
    border-bottom: 1px solid rgba(255,255,255,0.06);
    margin-bottom: 1.5rem;
}
.page-hero h1 {
    font-size: 1.75rem;
    font-weight: 700;
    margin: 0 0 0.2rem 0;
    line-height: 1.2;
}
.page-hero .subtitle {
    font-size: 0.92rem;
    color: #586069;
}

/* Mode banner */
.mode-paper {
    background: rgba(72,187,120,0.08);
    border: 1px solid rgba(72,187,120,0.2);
    border-radius: 8px;
    padding: 0.5rem 0.9rem;
    color: #68d391;
    font-size: 0.88rem;
    margin-bottom: 1rem;
}
.mode-live {
    background: rgba(245,101,101,0.1);
    border: 1px solid rgba(245,101,101,0.25);
    border-radius: 8px;
    padding: 0.5rem 0.9rem;
    color: #fc8181;
    font-size: 0.88rem;
    margin-bottom: 1rem;
}
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────

def _mode_card() -> None:
    mode = st.session_state.get("mode", "paper")
    host = st.session_state.get("ibkr_paper_host", "127.0.0.1")
    port = st.session_state.get("ibkr_paper_port" if mode == "paper" else "ibkr_live_port", 4002)
    last_refresh = st.session_state.get("last_broker_refresh") or "—"
    last_pipeline = st.session_state.get("last_pipeline_run") or "—"
    last_submit   = st.session_state.get("last_submit_time")   or "—"
    lock_status   = "🔒 ON" if st.session_state.get("submit_locked") else "🟢 OFF"

    mode_label = "PAPER" if mode == "paper" else "⚠️ LIVE"

    st.sidebar.markdown(f"""
<div class="sb-card">
    <div class="sb-label">System</div>
    <div class="sb-row"><span>Mode</span><span>{mode_label}</span></div>
    <div class="sb-row"><span>Endpoint</span><span>{host}:{port}</span></div>
    <div class="sb-row"><span>Submit Lock</span><span>{lock_status}</span></div>
    <div class="sb-row"><span>Last Data Pull</span><span>{last_refresh}</span></div>
    <div class="sb-row"><span>Last Pipeline</span><span>{last_pipeline}</span></div>
    <div class="sb-row"><span>Last Submit</span><span>{last_submit}</span></div>
</div>
""", unsafe_allow_html=True)


def sidebar() -> str:
    with st.sidebar:
        # Title
        st.markdown("## Capital Fund")
        st.caption("Algo Trading Dashboard")

        # Mode selector
        st.selectbox("Trading Mode", ["paper", "live"], key="mode")
        _mode_card()

        # Quick actions
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True):
                st.rerun()
        with col2:
            if st.session_state.get("submit_locked"):
                if st.button("🔓 Unlock", use_container_width=True):
                    st.session_state["submit_locked"] = False
                    st.rerun()
            else:
                if st.button("🔒 Lock", use_container_width=True):
                    st.session_state["submit_locked"] = True
                    st.rerun()

        st.divider()

        # Navigation
        current_page = st.session_state.get("selected_page", "Home")

        for group_label, pages in PAGE_GROUPS.items():
            st.markdown(f'<div class="sb-section">{group_label}</div>', unsafe_allow_html=True)
            for page in pages:
                icon = PAGE_ICONS.get(page, "•")
                label = f"{icon}  {page}"
                is_active = page == current_page

                # Streamlit doesn't support per-button CSS classes natively,
                # so we use a workaround: different button labels signal active state
                if st.button(label, key=f"nav_{page}", use_container_width=True):
                    st.session_state["selected_page"] = page
                    st.rerun()

        if st.session_state.get("last_error"):
            st.divider()
            st.error("⚠️ Last page had an error")

    return st.session_state.get("selected_page", "Home")


# ── Page render ───────────────────────────────────────────────────────────────

def _mode_banner() -> None:
    mode = st.session_state.get("mode", "paper")
    host = st.session_state.get("ibkr_paper_host", "127.0.0.1")
    port = st.session_state.get("ibkr_paper_port" if mode == "paper" else "ibkr_live_port", 4002)
    if mode == "paper":
        st.markdown(
            f'<div class="mode-paper">📄 Paper mode — IBKR endpoint: {host}:{port}</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div class="mode-live">⚠️ LIVE mode — IBKR endpoint: {host}:{port} — '
            'Real orders will be submitted!</div>',
            unsafe_allow_html=True,
        )


def _page_header(name: str) -> None:
    icon = PAGE_ICONS.get(name, "")
    st.markdown(f"""
<div class="page-hero">
    <h1>{icon}&nbsp;&nbsp;{name}</h1>
    <div class="subtitle">Capital Fund · Auto-Trading Dashboard</div>
</div>
""", unsafe_allow_html=True)


def render_page(page_name: str) -> None:
    module_path = PAGE_REGISTRY.get(page_name)
    if not module_path:
        st.error(f"Unknown page: {page_name}")
        return

    try:
        module = import_module(module_path)
    except ModuleNotFoundError as exc:
        st.error(f"Page module not found: `{module_path}`")
        st.code(str(exc))
        return
    except Exception as exc:
        st.error(f"Failed to import `{module_path}`")
        st.exception(exc)
        st.session_state["last_error"] = str(exc)
        return

    fn = getattr(module, "render", None) or getattr(module, "app", None)
    if fn is None:
        st.error(f"`{module_path}` has no `render()` or `app()` function.")
        return

    _page_header(page_name)
    _mode_banner()

    try:
        fn()
        st.session_state["last_error"] = None
    except Exception as exc:
        st.session_state["last_error"] = str(exc)
        st.error(f"Error rendering {page_name}")
        st.exception(exc)


# ── App entry ─────────────────────────────────────────────────────────────────

def main() -> None:
    st.set_page_config(
        page_title="Capital Fund",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    inject_css()
    init_session_state()

    page = sidebar()
    render_page(page)


if __name__ == "__main__":
    main()
