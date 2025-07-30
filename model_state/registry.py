from typing import Dict

from constants import ProcessingMode
from model_state.features import *

FEATURES: Dict[str, dict] = {
    "close_ma_10": {
        "func": moving_average("adjusted_close_1d", "close_ma_10", window=10),
        "mode": ProcessingMode.LAZY,
        "lookback": 10,
    },
    "close_momentum_1": {
        "func": momentum("adjusted_close_1d", "close_momentum_1", window=1),
        "mode": ProcessingMode.LAZY,
        "lookback": 1,
    },
    "close_momentum_5": {
        "func": momentum("adjusted_close_1d", "close_momentum_5", window=5),
        "mode": ProcessingMode.LAZY,
        "lookback": 5,
    },
    "close_momentum_10": {
        "func": momentum("adjusted_close_1d", "close_momentum_10", window=10),
        "mode": ProcessingMode.LAZY,
        "lookback": 10,
    },
    "close_momentum_20": {
        "func": momentum("adjusted_close_1d", "close_momentum_20", window=20),
        "mode": ProcessingMode.LAZY,
        "lookback": 20,
    },
    "close_momentum_30": {
        "func": momentum("adjusted_close_1d", "close_momentum_30", window=30),
        "mode": ProcessingMode.LAZY,
        "lookback": 30,
    },
    "close_momentum_60": {
        "func": momentum("adjusted_close_1d", "close_momentum_60", window=60),
        "mode": ProcessingMode.LAZY,
        "lookback": 60,
    },
    "close_momentum_90": {
        "func": momentum("adjusted_close_1d", "close_momentum_90", window=90),
        "mode": ProcessingMode.LAZY,
        "lookback": 90,
    },
    "close_momentum_120": {
        "func": momentum("adjusted_close_1d", "close_momentum_120", window=120),
        "mode": ProcessingMode.LAZY,
        "lookback": 120,
    },
    "close_rsi_14": {
        "func": rsi("adjusted_close_1d", "close_rsi_14", window=14),
        "mode": ProcessingMode.EAGER,
        "lookback": 14,
    },
}
