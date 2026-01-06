"""
Actuarial Module
================

Chain Ladder reserving and IBNR calculations.
Implements industry-standard actuarial methods for loss reserve estimation.

Owner: Sarah (Actuarial Data Scientist)
"""

from .chain_ladder.model import ChainLadderModel
from .chain_ladder.reserves import ReserveCalculator

__all__ = ["ChainLadderModel", "ReserveCalculator"]








