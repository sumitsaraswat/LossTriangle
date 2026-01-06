"""
Bronze Layer Schemas
====================

Defines the schema for raw claims and payment data.
These schemas are used for validation during ingestion.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
    TimestampType,
    IntegerType,
)


class ClaimsSchema:
    """Schema definitions for claims data."""

    @staticmethod
    def get_raw_schema() -> StructType:
        """
        Raw claims schema as received from source systems.
        
        Returns:
            StructType: PySpark schema for raw claims
        """
        return StructType([
            StructField("claim_id", StringType(), False),
            StructField("policy_id", StringType(), False),
            StructField("loss_date", DateType(), False),
            StructField("report_date", DateType(), False),
            StructField("claimant_name", StringType(), True),
            StructField("loss_type", StringType(), False),
            StructField("loss_description", StringType(), True),
            StructField("adjuster_notes", StringType(), True),
            StructField("claim_status", StringType(), False),
            StructField("county", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zip_code", StringType(), True),
            StructField("catastrophe_code", StringType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ])


class PaymentSchema:
    """Schema definitions for payment transactions."""

    @staticmethod
    def get_raw_schema() -> StructType:
        """
        Raw payment schema as received from source systems.
        
        Returns:
            StructType: PySpark schema for raw payments
        """
        return StructType([
            StructField("payment_id", StringType(), False),
            StructField("claim_id", StringType(), False),
            StructField("payment_date", DateType(), False),
            StructField("payment_type", StringType(), False),
            StructField("payment_amount", DoubleType(), False),
            StructField("cumulative_paid", DoubleType(), True),
            StructField("reserve_amount", DoubleType(), True),
            StructField("payment_status", StringType(), False),
            StructField("check_number", StringType(), True),
            StructField("payee_name", StringType(), True),
            StructField("expense_type", StringType(), True),
            StructField("created_at", TimestampType(), False),
        ])


class EnrichedClaimSchema:
    """Schema for enriched claims with calculated fields."""
    
    @staticmethod
    def get_schema() -> StructType:
        """
        Enriched claims schema with origin year and development period.
        
        Returns:
            StructType: PySpark schema for enriched claims
        """
        return StructType([
            StructField("claim_id", StringType(), False),
            StructField("policy_id", StringType(), False),
            StructField("loss_date", DateType(), False),
            StructField("report_date", DateType(), False),
            StructField("origin_year", IntegerType(), False),
            StructField("origin_quarter", IntegerType(), False),
            StructField("development_month", IntegerType(), False),
            StructField("loss_type", StringType(), False),
            StructField("adjuster_notes", StringType(), True),
            StructField("claim_status", StringType(), False),
            StructField("county", StringType(), True),
            StructField("state", StringType(), True),
            StructField("catastrophe_code", StringType(), True),
            StructField("cumulative_paid", DoubleType(), False),
            StructField("incurred_amount", DoubleType(), False),
            StructField("created_at", TimestampType(), False),
        ])








