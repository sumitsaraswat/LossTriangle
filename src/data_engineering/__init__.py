"""
Data Engineering Module
=======================

Handles the Bronze and Silver layers of the Medallion Architecture.
Responsible for data ingestion, transformation, and triangle construction.

Owner: Marcus (Data Engineer)
"""

from .bronze.ingestion import ClaimsIngestion
from .silver.triangle_builder import TriangleBuilder

__all__ = ["ClaimsIngestion", "TriangleBuilder"]








