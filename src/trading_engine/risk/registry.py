from trading_engine.risk.catalogue.sample import (
    SampleCovariance,
    SampleCovarianceWithRidge,
)
from trading_engine.risk.catalogue.naive_dcc import NaiveDCC

RISK_MODELS = {
    "sample": {"function": SampleCovariance()},
    "sample_with_ridge": {"function": SampleCovarianceWithRidge(ridge=1e-3)},
    "naive_dcc": {
        "function": NaiveDCC(beta_vol=0.94, beta_corr=0.97, ridge=1e-3, center=True)
    },
}
