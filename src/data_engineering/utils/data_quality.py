"""
Data Quality Checker
====================

Data quality validation utilities for the pipeline.
"""

from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnull, isnan
from loguru import logger


class DataQualityChecker:
    """
    Validates data quality across pipeline stages.
    """
    
    def __init__(self, strict_mode: bool = False):
        """
        Initialize data quality checker.
        
        Args:
            strict_mode: Raise exceptions on failures
        """
        self.strict_mode = strict_mode
        self.results = []
    
    def check_nulls(
        self,
        df: DataFrame,
        columns: list[str],
        threshold: float = 0.0,
    ) -> dict:
        """
        Check for null values in specified columns.
        
        Args:
            df: DataFrame to check
            columns: Columns to validate
            threshold: Maximum allowed null percentage (0-1)
            
        Returns:
            Dictionary of null counts per column
        """
        total_rows = df.count()
        null_counts = {}
        
        for col_name in columns:
            if col_name not in df.columns:
                logger.warning(f"Column '{col_name}' not found in DataFrame")
                continue
                
            null_count = df.filter(
                isnull(col(col_name)) | (col(col_name) == "")
            ).count()
            
            null_pct = null_count / total_rows if total_rows > 0 else 0
            null_counts[col_name] = {
                "null_count": null_count,
                "null_pct": null_pct,
                "passed": null_pct <= threshold,
            }
            
            if null_pct > threshold:
                msg = f"Column '{col_name}' has {null_pct:.2%} nulls (threshold: {threshold:.2%})"
                if self.strict_mode:
                    raise ValueError(msg)
                logger.warning(msg)
        
        self.results.append({"check": "nulls", "results": null_counts})
        return null_counts
    
    def check_duplicates(
        self,
        df: DataFrame,
        key_columns: list[str],
    ) -> dict:
        """
        Check for duplicate records.
        
        Args:
            df: DataFrame to check
            key_columns: Columns that should be unique together
            
        Returns:
            Dictionary with duplicate statistics
        """
        total_rows = df.count()
        distinct_rows = df.select(key_columns).distinct().count()
        duplicates = total_rows - distinct_rows
        
        result = {
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_rows": duplicates,
            "duplicate_pct": duplicates / total_rows if total_rows > 0 else 0,
            "passed": duplicates == 0,
        }
        
        if duplicates > 0:
            msg = f"Found {duplicates} duplicate rows ({result['duplicate_pct']:.2%})"
            if self.strict_mode:
                raise ValueError(msg)
            logger.warning(msg)
        
        self.results.append({"check": "duplicates", "results": result})
        return result
    
    def check_value_range(
        self,
        df: DataFrame,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> dict:
        """
        Check if values are within expected range.
        
        Args:
            df: DataFrame to check
            column: Column to validate
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Returns:
            Dictionary with range check results
        """
        stats = df.agg({column: "min", column: "max"}).collect()[0]
        actual_min = stats[0]
        actual_max = stats[1]
        
        violations = 0
        if min_val is not None:
            violations += df.filter(col(column) < min_val).count()
        if max_val is not None:
            violations += df.filter(col(column) > max_val).count()
        
        result = {
            "column": column,
            "actual_min": actual_min,
            "actual_max": actual_max,
            "expected_min": min_val,
            "expected_max": max_val,
            "violations": violations,
            "passed": violations == 0,
        }
        
        if violations > 0:
            msg = f"Column '{column}' has {violations} values outside range [{min_val}, {max_val}]"
            if self.strict_mode:
                raise ValueError(msg)
            logger.warning(msg)
        
        self.results.append({"check": "value_range", "results": result})
        return result
    
    def get_summary(self) -> dict:
        """Get summary of all quality checks."""
        total_checks = len(self.results)
        passed_checks = sum(
            1 for r in self.results 
            if isinstance(r["results"], dict) and r["results"].get("passed", True)
        )
        
        return {
            "total_checks": total_checks,
            "passed": passed_checks,
            "failed": total_checks - passed_checks,
            "details": self.results,
        }








