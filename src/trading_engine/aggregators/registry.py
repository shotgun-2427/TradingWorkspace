from trading_engine.aggregators.catalogue.equal_weight import EqualWeightAggregator
from trading_engine.aggregators.catalogue.min_avg_drawdown import (
    MinAvgDrawdownAggregator,
)

AGGREGATORS = {
    "equal_weight": {
        "function": EqualWeightAggregator(),
    },
    "min_avg_drawdown": {"function": MinAvgDrawdownAggregator(window_days=252)},
}
