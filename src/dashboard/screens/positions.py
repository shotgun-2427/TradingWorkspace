from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import streamlit as st


def _latest_csv(folder: Path, pattern: str) -> Path | None:
    files = sorted(folder.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _normalize_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def _first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {_normalize_name(c): c for c in df.columns}
    for candidate in candidates:
        actual = normalized.get(_normalize_name(candidate))
        if actual:
            return actual
    return None


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False).str.replace("$", "", regex=False), errors="coerce")


def _read_latest_nav(account_dir: Path) -> float | None:
    latest_account = _latest_csv(account_dir, "paper_account_summary_*.csv")
    if latest_account is None:
        return None

    try:
        df = pd.read_csv(latest_account)
    except Exception:
        return None

    tag_col = _first_existing_col(df, ["tag", "key", "field", "name"])
    value_col = _first_existing_col(df, ["value", "amount", "val"])
    if tag_col is None or value_col is None:
        return None

    temp = df.copy()
    temp[tag_col] = temp[tag_col].astype(str)

    for tag in ["NetLiquidation", "EquityWithLoanValue", "Net Liquidation", "Equity With Loan Value"]:
        hit = temp.loc[temp[tag_col].str.lower() == tag.lower()]
        if not hit.empty:
            try:
                return float(str(hit.iloc[0][value_col]).replace(",", "").replace("$", ""))
            except Exception:
                return None
    return None


def _load_positions(path: Path, nav: float | None) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame()

    symbol_col = _first_existing_col(df, ["symbol", "ticker", "localsymbol", "contract", "fin_instr"])
    shares_col = _first_existing_col(df, ["position", "pos", "shares", "quantity", "qty"])
    avg_col = _first_existing_col(df, ["avg_cost", "average_cost", "avgcost", "avg_px", "avgpx"])
    last_col = _first_existing_col(df, ["market_price", "last_price", "last", "price", "mkt_price", "mktpx"])
    mv_col = _first_existing_col(df, ["market_value", "marketvalue", "mkt_val", "marketval", "mktvl"])
    unreal_col = _first_existing_col(df, ["unrealized_pnl", "unrealizedpnl", "unrealized", "upnl"])
    realized_col = _first_existing_col(df, ["realized_pnl", "realizedpnl", "realized", "rpnl"])

    out = pd.DataFrame()
    out["symbol"] = df[symbol_col].astype(str).str.upper() if symbol_col else "UNKNOWN"
    out["shares"] = _to_num(df[shares_col]) if shares_col else 0.0
    out["avg_price"] = _to_num(df[avg_col]) if avg_col else pd.NA
    out["last_price"] = _to_num(df[last_col]) if last_col else pd.NA
    out["market_value"] = _to_num(df[mv_col]) if mv_col else pd.NA
    out["unrealized_pnl"] = _to_num(df[unreal_col]) if unreal_col else pd.NA
    out["realized_pnl"] = _to_num(df[realized_col]) if realized_col else 0.0

    if out["market_value"].isna().all() and not out["last_price"].isna().all():
        out["market_value"] = out["shares"] * out["last_price"]

    if out["unrealized_pnl"].isna().all() and not out["last_price"].isna().all() and not out["avg_price"].isna().all():
        out["unrealized_pnl"] = out["shares"] * (out["last_price"] - out["avg_price"])

    out = out.fillna(0.0)
    out = out.loc[out["shares"] != 0].copy()

    if nav and nav != 0:
        out["weight_pct"] = 100.0 * out["market_value"] / nav
    else:
        out["weight_pct"] = 0.0

    out["abs_market_value"] = out["market_value"].abs()
    out = out.sort_values(["abs_market_value", "symbol"], ascending=[False, True]).reset_index(drop=True)
    return out


def render() -> None:
    root = Path(__file__).resolve().parents[3]
    positions_dir = root / "data" / "broker" / "positions"
    account_dir = root / "data" / "broker" / "account"

    st.title("Positions")
    st.caption("Current ETF positions and simple position-level PnL")

    latest_positions = _latest_csv(positions_dir, "paper_positions_snapshot*.csv")
    if latest_positions is None:
        st.warning("No positions snapshot found.")
        st.info("Use Run Pipeline -> Refresh Broker Snapshot after submitting orders.")
        return

    nav = _read_latest_nav(account_dir)
    positions = _load_positions(latest_positions, nav=nav)

    if positions.empty:
        st.info("No open positions found in the latest snapshot.")
        return

    gross_mv = float(positions["abs_market_value"].sum())
    net_mv = float(positions["market_value"].sum())
    total_unreal = float(positions["unrealized_pnl"].sum())
    total_realized = float(positions["realized_pnl"].sum()) if "realized_pnl" in positions else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Open positions", f"{len(positions):,}")
    c2.metric("Gross market value", f"${gross_mv:,.2f}")
    c3.metric("Net market value", f"${net_mv:,.2f}")
    c4.metric("Total unrealized PnL", f"${total_unreal:,.2f}")

    if nav is not None:
        st.caption(f"Latest NAV used for weights: ${nav:,.2f}")

    col1, col2 = st.columns([1.15, 0.85])

    with col1:
        st.subheader("PnL by ETF")
        chart_df = positions[["symbol", "unrealized_pnl"]].set_index("symbol")
        st.bar_chart(chart_df, use_container_width=True)

    with col2:
        st.subheader("Top / Bottom")
        winners = positions.sort_values("unrealized_pnl", ascending=False)[["symbol", "unrealized_pnl"]].head(5)
        losers = positions.sort_values("unrealized_pnl", ascending=True)[["symbol", "unrealized_pnl"]].head(5)

        st.write("**Best**")
        st.dataframe(winners, use_container_width=True, hide_index=True)

        st.write("**Worst**")
        st.dataframe(losers, use_container_width=True, hide_index=True)

    st.subheader("Positions Table")
    display = positions[[
        "symbol",
        "shares",
        "avg_price",
        "last_price",
        "market_value",
        "unrealized_pnl",
        "realized_pnl",
        "weight_pct",
    ]].copy()

    display = display.rename(
        columns={
            "avg_price": "avg_price",
            "last_price": "last_price",
            "market_value": "market_value",
            "unrealized_pnl": "unrealized_pnl",
            "realized_pnl": "realized_pnl",
            "weight_pct": "weight_pct",
        }
    )

    st.dataframe(display, use_container_width=True, hide_index=True)
    st.caption(str(latest_positions))


def app() -> None:
    render()
