from trading_engine.optimizers.catalogue.mean_variance import MeanVarianceOptimizer
from trading_engine.risk.registry import RISK_MODELS

PORTFOLIO_OPTIMIZERS = {
    "mean_variance": {
        "function": MeanVarianceOptimizer(
            cov_window_days=240,
            gamma=1.0,
            lambda_te=0.5,
            risk_model=RISK_MODELS["naive_dcc"]["function"],
            kappa=1.0,
        )
    },
    "mean_variance_constrained": {
        "function": MeanVarianceOptimizer(
            cov_window_days=240,
            gamma=1.0,
            lambda_te=0.5,
            risk_model=RISK_MODELS["naive_dcc"]["function"],
            kappa=1.0,
            min_position_delta=0.03,  # 3% minimum trade size
            asset_weight_bounds={
                # US Equity - Allow concentrated positions
                "SPY-US": {"min": -0.5, "max": 0.5},
                # "IXN-US": {"min": -0.3, "max": 0.3},
                # Fixed Income - Conservative bounds
                "TLT-US": {"min": -0.3, "max": 0.5},
                "IEI-US": {"min": -0.2, "max": 0.5},
                "SHY-US": {"min": -0.2, "max": 0.5},
                "BIL-US": {"min": -0.2, "max": 0.5},
                # International Equity - Moderate
                "EWJ-US": {"min": -0.2, "max": 0.3},
                "INDA-US": {"min": -0.2, "max": 0.3},
                "MCHI-US": {"min": -0.2, "max": 0.3},
                "EZU-US": {"min": -0.2, "max": 0.3},
                # Precious Metals
                "GLD-US": {"min": -0.2, "max": 0.3},
                "SLV-US": {"min": -0.15, "max": 0.25},
                # Commodities - Tighter bounds due to volatility
                "USO-US": {"min": -0.15, "max": 0.2},
                "UNG-US": {"min": -0.1, "max": 0.15},
                # Volatility - Very tight
                "VIXY-US": {"min": -0.1, "max": 0.1},
                # Crypto - Emerging
                "IBIT-US": {"min": -0.15, "max": 0.2},
                "ETHA-US": {"min": -0.15, "max": 0.2},
            },
        )
    },
}
