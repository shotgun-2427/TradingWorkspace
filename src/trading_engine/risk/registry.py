from trading_engine.risk.catalogue.sample import (
    SampleCovariance,
    SampleCovarianceWithRidge,
)

RISK_MODELS = {
    "sample": {
        "function": SampleCovariance(),
    },
    "sample_with_ridge": {
        "function": SampleCovarianceWithRidge(ridge=1e-2),
    },
}
