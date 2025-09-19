from trading_engine.aggregators.catalogue.equal_weight import EqualWeightAggregator
from trading_engine.aggregators.catalogue.min_avg_drawdown import (
    MinAvgDrawdownAggregator,
)
from trading_engine.aggregators.catalogue.mvo_aggregator import MVOAggregator
from trading_engine.risk.registry import RISK_MODELS

"""
Alternatively, we can import the risk model from the risk registry.

```python
from trading_engine.risk.registry import RISK_MODELS

risk_model = RISK_MODELS["sample_with_ridge"]["function"]
```

"""

AGGREGATORS = {
    # "equal_weight": {"function": EqualWeightAggregator()},
    # "min_avg_drawdown": {"function": MinAvgDrawdownAggregator(window_days=252)},
    "model_mvo": {
        "function": MVOAggregator(
            cov_window_days=240,
            risk_model=RISK_MODELS["naive_dcc"]["function"],
            fallback="zero",
            turnover_lambda=0.0,
            kappa=1.0,
            hl_mean=120,
            hl_vol=60,
            winsor_pct=0.01,
            vol_floor=1e-4,
            mu_scale=1.0,
            long_only=True,
        )
    },
}
