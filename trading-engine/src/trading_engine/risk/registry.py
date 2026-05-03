from trading_engine.risk.catalogue.sample import (
    SampleCovariance,
    SampleCovarianceWithRidge,
)
from trading_engine.risk.catalogue.naive_dcc import NaiveDCC
from trading_engine.risk.catalogue.stat_model import (
    ConstantCorrelationShrinkage,
    ExponentiallyWeightedSample,
    LedoitWolf,
)

RISK_MODELS = {
    "sample": {"function": SampleCovariance(), "lookback": 0},
    "sample_with_ridge": {
        "function": SampleCovarianceWithRidge(ridge=1e-3),
        "lookback": 0,
    },
    "naive_dcc": {
        "function": NaiveDCC(
            half_life_vol=21,
            half_life_corr=120,
            ridge=1e-3,
            center=True,
        ),
        "lookback": 0,
    },
    "ledoit_wolf": {
        "function": LedoitWolf(ridge=1e-8),
        "lookback": 0,
    },
    "constant_correlation_shrinkage": {
        "function": ConstantCorrelationShrinkage(
            target_correlation=0.3, shrinkage=0.5
        ),
        "lookback": 0,
    },
    "ewma_60": {
        "function": ExponentiallyWeightedSample(half_life=60),
        "lookback": 0,
    },
    "ewma_21": {
        "function": ExponentiallyWeightedSample(half_life=21),
        "lookback": 0,
    },
}
