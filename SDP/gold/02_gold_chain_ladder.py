# Databricks notebook source
# Delta Live Tables (DLT) Pipeline - Python
# Layer: Gold
# Purpose: Reserve Summary (DLT-compatible Spark operations only)
# Author: Sarah (Actuarial Data Scientist)

# =============================================================================
# GOLD LAYER: Reserve Summary
# 
# IMPORTANT NOTE:
# The Chain Ladder model requires pandas/numpy operations which are NOT 
# supported in DLT (no .collect() or .toPandas() allowed).
# 
# The following tables are created by a SEPARATE JOB (chain_ladder_job.py):
#   - gold_reserve_estimates
#   - gold_development_factors_python
# 
# This file only contains gold_reserve_summary which uses pure Spark operations.
# =============================================================================

import dlt
from pyspark.sql.functions import col, lit, current_timestamp, current_date, when, coalesce
from pyspark.sql.functions import sum as spark_sum


# -----------------------------------------------------------------------------
# Gold Table: Reserve Summary
# This table uses pure Spark operations (no .collect() or .toPandas())
# It reads from gold_reserve_estimates which is created by chain_ladder_job.py
# -----------------------------------------------------------------------------
@dlt.table(
    name="gold_reserve_summary",
    comment="Executive reserve summary with AI adjustment - Gold Layer",
    table_properties={"quality": "gold"}
)
def gold_reserve_summary():
    """
    Calculate summary metrics for executive reporting.
    Uses pure Spark operations (no .collect()) for DLT compatibility.
    
    Note: This table depends on gold_reserve_estimates which must be 
    populated by running chain_ladder_job.py BEFORE this DLT pipeline.
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    # Read from the gold_reserve_estimates table (created by separate job)
    # Using spark.read.table() since it's created outside DLT
    # NOTE: chain_ladder_job.py writes to unified_reserves.gold
    # IMPORTANT: Filter to only 'ALL' loss type to avoid double counting
    # (the table contains both 'ALL' and individual loss types like Fire, Water Damage, etc.)
    try:
        reserves_df = spark.read.table("unified_reserves.losstriangle.gold_reserve_estimates").filter(col("loss_type") == "ALL")
    except Exception:
        # If table doesn't exist yet, return empty
        from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, TimestampType
        empty_schema = StructType([
            StructField("as_of_date", DateType(), True),
            StructField("triangle_type", StringType(), True),
            StructField("loss_type", StringType(), True),
            StructField("total_paid", DoubleType(), True),
            StructField("total_ibnr", DoubleType(), True),
            StructField("total_ultimate", DoubleType(), True),
            StructField("held_reserves", DoubleType(), True),
            StructField("reserve_adequacy", DoubleType(), True),
            StructField("reserve_margin", DoubleType(), True),
            StructField("ai_adjustment_factor", DoubleType(), True),
            StructField("_calculated_at", TimestampType(), True),
        ])
        return spark.createDataFrame([], empty_schema)
    
    # Read risk summary for AI adjustment (this is from DLT)
    risk_df = dlt.read("silver_risk_summary_nlp")
    
    # Calculate reserve totals (single row DataFrame)
    reserve_totals = reserves_df.agg(
        coalesce(spark_sum("latest_cumulative"), lit(0.0)).alias("total_paid"),
        coalesce(spark_sum("ibnr"), lit(0.0)).alias("total_ibnr"),
        coalesce(spark_sum("ultimate_loss"), lit(0.0)).alias("total_ultimate")
    )
    
    # Calculate risk totals (single row DataFrame)
    risk_totals = risk_df.agg(
        coalesce(spark_sum("high_risk_incurred"), lit(0.0)).alias("high_risk"),
        coalesce(spark_sum("total_incurred"), lit(0.0)).alias("risk_total")
    )
    
    # Cross join to combine (both are single-row DataFrames)
    combined = reserve_totals.crossJoin(risk_totals)
    
    # Calculate AI adjustment and final metrics using Spark expressions
    result = combined.select(
        current_date().alias("as_of_date"),
        lit("paid").alias("triangle_type"),
        lit("ALL").alias("loss_type"),
        col("total_paid"),
        col("total_ibnr"),
        col("total_ultimate"),
        # AI adjustment: 1.0 + (high_risk_ratio * 0.50)
        (lit(1.0) + (
            when(col("risk_total") > 0, col("high_risk") / col("risk_total"))
            .otherwise(lit(0.0)) * lit(0.50)
        )).alias("ai_adjustment_factor"),
    )
    
    # Add calculated fields based on ai_adjustment_factor
    result = result.withColumn(
        "held_reserves", 
        col("total_ibnr") * col("ai_adjustment_factor")
    ).withColumn(
        "reserve_adequacy",
        col("ai_adjustment_factor")
    ).withColumn(
        "reserve_margin",
        col("total_ibnr") * (col("ai_adjustment_factor") - lit(1.0))
    ).withColumn(
        "_calculated_at",
        current_timestamp()
    )
    
    return result
