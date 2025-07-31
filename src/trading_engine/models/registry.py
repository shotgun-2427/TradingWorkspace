from trading_engine.models.catalogue.inverse_momentum_mean_reversion import InverseMomentumMeanReversionModel
from trading_engine.models.catalogue.momentum import MomentumModel
from trading_engine.models.catalogue.natr_mean_reversion import NATRMeanReversionModel

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
    "etf_mr_gld_10_0.1": {
        "tickers": ["GLD-US"],
        "columns": ["close_momentum_10"],
        "function": InverseMomentumMeanReversionModel(
            tickers=["GLD-US"],
            momentum_column="close_momentum_10",
            threshold=0.1
        )
    },
    "etf_mr_gld_60_0.1": {
        "tickers": ["GLD-US"],
        "columns": ["close_momentum_60"],
        "function": InverseMomentumMeanReversionModel(
            tickers=["GLD-US"],
            momentum_column="close_momentum_60",
            threshold=0.1
        )
    },
    "etf_mr_ixn_10_0.1": {
        "tickers": ["IXN-US"],
        "columns": ["close_momentum_10"],
        "function": InverseMomentumMeanReversionModel(
            tickers=["IXN-US"],
            momentum_column="close_momentum_10",
            threshold=0.1
        )
    },
    "etf_mr_jxi_60_0.001": {
        "tickers": ["JXI-US"],
        "columns": ["close_momentum_60"],
        "function": InverseMomentumMeanReversionModel(
            tickers=["JXI-US"],
            momentum_column="close_momentum_60",
            threshold=0.001
        )
    },
    "etf_mr_jxi_240_0.05": {
        "tickers": ["JXI-US"],
        "columns": ["close_momentum_240"],
        "function": InverseMomentumMeanReversionModel(
            tickers=["JXI-US"],
            momentum_column="close_momentum_240",
            threshold=0.05
        )
    },
    "etf_mr_jxi_240_0.01": {
        "tickers": ["JXI-US"],
        "columns": ["close_momentum_240"],
        "function": InverseMomentumMeanReversionModel(
            tickers=["JXI-US"],
            momentum_column="close_momentum_240",
            threshold=0.01
        )
    },
    "etf_mr_jxi_240_0.001": {
        "tickers": ["JXI-US"],
        "columns": ["close_momentum_240"],
        "function": InverseMomentumMeanReversionModel(
            tickers=["JXI-US"],
            momentum_column="close_momentum_240",
            threshold=0.001
        )
    },
    "etf_mr_jxi_240_0.005": {
        "tickers": ["JXI-US"],
        "columns": ["close_momentum_240"],
        "function": InverseMomentumMeanReversionModel(
            tickers=["JXI-US"],
            momentum_column="close_momentum_240",
            threshold=0.005
        )
    },
    "natr_7_14": {
        "tickers": ["IXN-US"],
        "columns": ["adjusted_close_1d", "natr_7", "natr_14", "close_momentum_1", "close_momentum_14",
                    "close_momentum_32", "close_momentum_64"],
        "function": NATRMeanReversionModel(trade_ticker="IXN-US")
    }
}
