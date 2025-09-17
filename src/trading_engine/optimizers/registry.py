from trading_engine.optimizers.catalogue.mean_variance import MeanVarianceOptimizer

PORTFOLIO_OPTIMIZERS = {
    "mean_variance": {
        "function": MeanVarianceOptimizer(
            cov_window_days=250,
            gamma=1.0,
            lambda_te=1.0,
            ridge=1e-2,
        )
    }
}
