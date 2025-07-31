import math


def calculate_calendar_lookback(lookback_days: int, cushion_days: int = 2) -> int:
    # 5 trading days ≈ 7 calendar days; add a small cushion for holidays/halts
    return int(math.ceil(lookback_days * 7 / 5)) + cushion_days
