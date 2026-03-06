"""
@description: Base enum class.
@author: Rithwik Babu
"""

from enum import Enum


class BaseEnum(str, Enum):
    """
    Base enum class used by all enum classes.

    Note: Inheriting from str is necessary to correctly serialize output of enum
    """

    pass
