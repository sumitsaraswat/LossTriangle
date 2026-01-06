"""
Bronze Layer - Raw Data Ingestion
=================================

Handles raw data ingestion from various sources into Delta Lake.
Preserves full history with append-only mode.
"""

from .ingestion import ClaimsIngestion
from .schema import ClaimsSchema, PaymentSchema

__all__ = ["ClaimsIngestion", "ClaimsSchema", "PaymentSchema"]








