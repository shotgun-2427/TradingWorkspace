"""
Model factory catalogue.

Each module in this package exports a model **factory** — a higher-order
function that takes parameters (lookback, threshold, ticker, etc.) and
returns a callable signal generator. The registry (`models/registry.py`)
binds factory invocations to friendly names that YAML configs can
reference.

Adding a new model type:
  1. Add ``my_model.py`` here exporting a factory function.
  2. Import it in ``models/registry.py``.
  3. Register one or more named parameterizations.

Naming convention for factories: ``PascalCaseModel`` (e.g. ``MomentumModel``).
"""

# Re-export factories so callers can `from trading_engine.models.catalogue import X`.
from trading_engine.models.catalogue.base import (
    BundleRunner,
    LazyFrameRunner,
    ModelFactory,
    ModelRunner,
    validate_weight_frame,
)
from trading_engine.models.catalogue.amma import AMMA
from trading_engine.models.catalogue.bollinger_band import BollingerBandModel
from trading_engine.models.catalogue.donchian_breakout import DonchianBreakoutModel
from trading_engine.models.catalogue.dual_ma_crossover import DualMACrossoverModel
from trading_engine.models.catalogue.inverse_momentum_mean_reversion import (
    InverseMomentumMeanReversionModel,
)
from trading_engine.models.catalogue.low_volatility import LowVolatilityModel
from trading_engine.models.catalogue.macd import MACDModel
from trading_engine.models.catalogue.momentum import MomentumModel
from trading_engine.models.catalogue.natr_mean_reversion import NATRMeanReversionModel
from trading_engine.models.catalogue.quality_sharpe import QualitySharpeModel
from trading_engine.models.catalogue.relative_strength import RelativeStrengthModel
from trading_engine.models.catalogue.rsi_mean_reversion import RSIMeanReversionModel
from trading_engine.models.catalogue.trend_strength import TrendStrengthModel
from trading_engine.models.catalogue.volatility_target import VolatilityTargetModel

__all__ = [
    # Contract
    "ModelRunner",
    "LazyFrameRunner",
    "BundleRunner",
    "ModelFactory",
    "validate_weight_frame",
    # Factories
    "AMMA",
    "BollingerBandModel",
    "DonchianBreakoutModel",
    "DualMACrossoverModel",
    "InverseMomentumMeanReversionModel",
    "LowVolatilityModel",
    "MACDModel",
    "MomentumModel",
    "NATRMeanReversionModel",
    "QualitySharpeModel",
    "RelativeStrengthModel",
    "RSIMeanReversionModel",
    "TrendStrengthModel",
    "VolatilityTargetModel",
]
