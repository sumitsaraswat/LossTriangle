"""
Helper Utilities
================

Common helper functions for data engineering.
"""

import os
from typing import Optional
from pyspark.sql import SparkSession
from loguru import logger


def get_spark_session(
    app_name: str = "LossTriangle",
    catalog: Optional[str] = None,
) -> SparkSession:
    """
    Get or create a SparkSession.
    
    Args:
        app_name: Application name
        catalog: Default catalog to use
        
    Returns:
        Active SparkSession
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Check if running on Databricks
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        logger.info("Running on Databricks - using existing session")
        spark = builder.getOrCreate()
    else:
        logger.info("Running locally - configuring Databricks Connect")
        # Databricks Connect configuration
        spark = builder.getOrCreate()
    
    if catalog:
        spark.sql(f"USE CATALOG {catalog}")
        logger.info(f"Using catalog: {catalog}")
    
    return spark


def get_catalog_config(env: str = "dev") -> dict:
    """
    Get Unity Catalog configuration for environment.
    
    Args:
        env: Environment name (dev, staging, prod)
        
    Returns:
        Configuration dictionary
    """
    configs = {
        "dev": {
            "catalog": "unified_reserves",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold",
        },
        "staging": {
            "catalog": "unified_reserves",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold",
        },
        "prod": {
            "catalog": "unified_reserves",
            "bronze_schema": "bronze",
            "silver_schema": "silver",
            "gold_schema": "gold",
        },
    }
    
    return configs.get(env, configs["dev"])


def format_bytes(size_bytes: int) -> str:
    """Format bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

