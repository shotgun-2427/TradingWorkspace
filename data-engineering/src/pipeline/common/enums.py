"""
@description: Enums for consistency.
@author: Rithwik Babu
"""
from enum import Enum


class Environment(Enum):
    """
    Enum for environment
    """
    DEVELOPMENT = 'DEVELOPMENT'
    PRODUCTION = 'PRODUCTION'

    @property
    def lower(self):
        return self.value.lower()


class WriteMode(Enum):
    """
    Enum for write output mode
    """
    BIGQUERY = 'BIGQUERY'
    CSV = 'CSV'
    LOG = 'LOG'


class FieldType(str, Enum):
    INT = "int"
    DOUBLE = "double"
    STRING = "char"


class IdentifierType(str, Enum):
    TICKER = "TICKER"
    CUSIP = "CUSIP"
    ISIN = "ISIN"
    FIGI = "FIGI"
    SEDOL = "SEDOL"


class AssetClass(str, Enum):
    EQUITIES = "equities"
    FUTURES = "futures"
    ICE_BOFA_BOND_INDICES = "ice_bofa_bond_indices"
    GLOBAL_INDICES = "global_indices"
    OTHER = "other"
