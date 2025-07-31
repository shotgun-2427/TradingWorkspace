from trading_engine.optimizers.catalogue.equal_weight import EqualWeightOptimizer
from trading_engine.optimizers.catalogue.min_avg_drawdown import MinAvgDrawdownOptimizer

OPTIMIZERS = {
    "equal_weight": {
        "function": EqualWeightOptimizer(),
    },
    "min_avg_drawdown": {
        "function": MinAvgDrawdownOptimizer(window_days=252)
    }

}
