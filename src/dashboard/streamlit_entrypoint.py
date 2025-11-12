import streamlit as st

def main():
    st.set_page_config(page_title="Capital Fund Dashboard")
    st.sidebar.header("Pages")

    page = st.sidebar.radio(
        "Navigation",
        (
            "Backtests",
            "Paper Portfolio",
        ),
    )

    if page == "Backtests":
        from src.dashboard.screens import backtest_charts

        backtest_charts.app()
    elif page == "Paper Portfolio":
        from src.dashboard.screens import paper_performance

        paper_performance.app()


if __name__ == "__main__":
    main()