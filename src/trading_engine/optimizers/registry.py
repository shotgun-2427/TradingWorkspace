from trading_engine.optimizers.catalogue.equal_weight import EqualWeightOptimizer
from trading_engine.optimizers.catalogue.min_avg_drawdown import MinAvgDrawdownOptimizer
from trading_engine.optimizers.catalogue.mean_variance import MeanVarianceOptimizer


OPTIMIZERS = {
    "equal_weight": {
        "function": EqualWeightOptimizer(),
    },
    "min_avg_drawdown": {
        "function": MinAvgDrawdownOptimizer(window_days=252)
    },
    "mean_variance": {
        "function": MeanVarianceOptimizer(gamma = 5.0, lookback=252, allow_short = True, short_limit = -0.2, max_position=0.25)
    }
}
