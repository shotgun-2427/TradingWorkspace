from trading_engine.optimizers.catalogue.mean_variance import MeanVarianceOptimizer

PORTFOLIO_OPTIMIZERS = {
    "mean_variance": {
        "function": MeanVarianceOptimizer(
            cov_window_days=250,
            risk_aversion=1.0,
            solve_mode="mu",
            normalize_mu="none",
            ridge=1e-2,
        )
    }
}
