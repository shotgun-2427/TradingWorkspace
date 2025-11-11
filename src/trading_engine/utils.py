import math


def calculate_calendar_lookback(lookback_days: int, cushion_days: int = 2) -> int:
    # 252 trading days ≈ 365 calendar days; add a small cushion for holidays/halts
    return int(math.ceil(lookback_days * 365 / 252)) + cushion_days
