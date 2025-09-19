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
    }
}
