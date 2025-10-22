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
    "model_mvo_amma_constrained": {
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
            model_weight_bounds={
                # Fixed Income - Conservative allocations
                "TLT_AMMA": {"min": 0.0, "max": 0.25},  # Long-term Treasury
                "IEI_AMMA": {"min": 0.0, "max": 0.25},  # Intermediate Treasury
                "SHY_AMMA": {"min": 0.0, "max": 0.25},  # Short-term Treasury
                "BIL_AMMA": {"min": 0.0, "max": 0.20},  # T-bills
                # US Equity - Core holding
                "SPY_AMMA": {"min": 0.0, "max": 0.40},  # S&P 500
                # International Equity - Moderate diversification
                "EWJ_AMMA": {"min": 0.0, "max": 0.15},  # Japan
                "INDA_AMMA": {"min": 0.0, "max": 0.15},  # India
                "MCHI_AMMA": {"min": 0.0, "max": 0.15},  # China
                "EZU_AMMA": {"min": 0.0, "max": 0.15},  # Eurozone
                # Precious Metals - Alternative assets
                "GLD_AMMA": {"min": 0.0, "max": 0.15},  # Gold
                "SLV_AMMA": {"min": 0.0, "max": 0.10},  # Silver (more volatile)
                # Commodities - Smaller allocation due to volatility
                "USO_AMMA": {"min": 0.0, "max": 0.10},  # Oil
                "UNG_AMMA": {"min": 0.0, "max": 0.08},  # Natural Gas (very volatile)
                # Volatility - Hedge only
                "VIXY_AMMA": {"min": 0.0, "max": 0.05},  # VIX futures
                # Crypto - Emerging, volatile
                "IBIT_AMMA": {"min": 0.0, "max": 0.10},  # Bitcoin ETF
                "ETHA_AMMA": {"min": 0.0, "max": 0.08},  # Ethereum ETF
            },
        )
    },
    "equal_weight": {"function": EqualWeightAggregator()},
}
