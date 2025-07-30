from models.catalogue.momentum import MomentumModel

MODELS = {
    "RXI_TLT_pml_10": {
        "tickers": ["RXI-US", "TLT-US"],
        "columns": ["close_momentum_10"],
        "function": MomentumModel(
            trade_ticker="RXI-US",
            signal_ticker="TLT-US",
            momentum_column="close_momentum_10",
            inverse=False,
        ),
    },
    "GLD_USO_nml_10": {
        "tickers": ["GLD-US", "USO-US"],
        "columns": ["close_momentum_10"],
        "function": MomentumModel(
            trade_ticker="GLD-US",
            signal_ticker="USO-US",
            momentum_column="close_momentum_10",
            inverse=True,
        ),
    },
    "IXJ_USO_pml_10": {
        "tickers": ["IXJ-US", "USO-US"],
        "columns": ["close_momentum_10"],
        "function": MomentumModel(
            trade_ticker="IXJ-US",
            signal_ticker="USO-US",
            momentum_column="close_momentum_10",
            inverse=False,
        ),
    },
    "TLT_IXC_nml_10": {
        "tickers": ["TLT-US", "IXC-US"],
        "columns": ["close_momentum_10"],
        "function": MomentumModel(
            trade_ticker="TLT-US",
            signal_ticker="IXC-US",
            momentum_column="close_momentum_10",
            inverse=True,
        ),
    },
    "IXJ_KXI_pml_10": {
        "tickers": ["IXJ-US", "KXI-US"],
        "columns": ["close_momentum_10"],
        "function": MomentumModel(
            trade_ticker="IXJ-US",
            signal_ticker="KXI-US",
            momentum_column="close_momentum_10",
            inverse=False,
        ),
    },
}
