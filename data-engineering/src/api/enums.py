"""
@description: Enums used by the API layer.
"""
from enum import Enum


class BackfillPipeline(str, Enum):
    EQUITIES = "equities"
