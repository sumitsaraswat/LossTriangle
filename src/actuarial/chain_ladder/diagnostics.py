"""
Triangle Diagnostics
====================

Diagnostic tools for analyzing loss triangles and
validating Chain Ladder assumptions.
"""

from typing import Optional
import numpy as np
import pandas as pd
from loguru import logger


class TriangleDiagnostics:
    """
    Diagnostic tools for loss triangle analysis.
    
    Helps validate Chain Ladder assumptions:
    1. Development pattern stability
    2. No significant outliers
    3. Reasonable data volume
    """
    
    def __init__(self, triangle: np.ndarray):
        """
        Initialize diagnostics with a triangle.
        
        Args:
            triangle: Loss triangle as numpy array
        """
        self.triangle = triangle
        self.n_origins, self.n_devs = triangle.shape
    
    def check_completeness(self) -> dict:
        """
        Check triangle completeness.
        
        Returns:
            Dictionary with completeness metrics
        """
        total_cells = self.n_origins * self.n_devs
        
        # Upper triangle should have data
        upper_triangle_cells = sum(
            min(self.n_devs, self.n_devs - i)
            for i in range(self.n_origins)
        )
        
        non_null = np.sum(~np.isnan(self.triangle))
        
        return {
            "n_origins": self.n_origins,
            "n_development_periods": self.n_devs,
            "expected_cells": upper_triangle_cells,
            "actual_cells": int(non_null),
            "completeness_pct": non_null / upper_triangle_cells * 100,
        }
    
    def check_development_stability(self) -> pd.DataFrame:
        """
        Analyze stability of development patterns.
        
        Calculates individual ATA factors and their variability.
        High variability suggests unstable patterns.
        
        Returns:
            DataFrame with development factor statistics
        """
        triangle = self.triangle
        n_devs = self.n_devs
        
        results = []
        
        for j in range(n_devs - 1):
            col_current = triangle[:, j]
            col_next = triangle[:, j + 1]
            
            # Calculate individual factors
            valid = ~np.isnan(col_current) & ~np.isnan(col_next) & (col_current > 0)
            
            if np.sum(valid) > 0:
                factors = col_next[valid] / col_current[valid]
                
                results.append({
                    "dev_period": j + 1,
                    "n_observations": int(np.sum(valid)),
                    "mean_factor": np.mean(factors),
                    "median_factor": np.median(factors),
                    "std_factor": np.std(factors),
                    "min_factor": np.min(factors),
                    "max_factor": np.max(factors),
                    "cv": np.std(factors) / np.mean(factors) if np.mean(factors) > 0 else 0,
                })
            else:
                results.append({
                    "dev_period": j + 1,
                    "n_observations": 0,
                    "mean_factor": None,
                    "median_factor": None,
                    "std_factor": None,
                    "min_factor": None,
                    "max_factor": None,
                    "cv": None,
                })
        
        return pd.DataFrame(results)
    
    def detect_outliers(
        self,
        threshold_std: float = 2.0,
    ) -> pd.DataFrame:
        """
        Detect outlier development factors.
        
        Args:
            threshold_std: Number of standard deviations for outlier
            
        Returns:
            DataFrame of detected outliers
        """
        triangle = self.triangle
        outliers = []
        
        for j in range(self.n_devs - 1):
            col_current = triangle[:, j]
            col_next = triangle[:, j + 1]
            
            valid = ~np.isnan(col_current) & ~np.isnan(col_next) & (col_current > 0)
            
            if np.sum(valid) < 3:
                continue
            
            factors = col_next[valid] / col_current[valid]
            mean_f = np.mean(factors)
            std_f = np.std(factors)
            
            # Check each origin
            for i in range(self.n_origins):
                if valid[i]:
                    factor = col_next[i] / col_current[i]
                    z_score = (factor - mean_f) / std_f if std_f > 0 else 0
                    
                    if abs(z_score) > threshold_std:
                        outliers.append({
                            "origin_period": i + 1,
                            "dev_period": j + 1,
                            "factor": factor,
                            "mean_factor": mean_f,
                            "z_score": z_score,
                            "outlier_type": "HIGH" if z_score > 0 else "LOW",
                        })
        
        return pd.DataFrame(outliers)
    
    def calculate_link_ratio_diagnostics(self) -> dict:
        """
        Calculate comprehensive link ratio diagnostics.
        
        Returns:
            Dictionary with diagnostic summaries
        """
        stability = self.check_development_stability()
        outliers = self.detect_outliers()
        completeness = self.check_completeness()
        
        # Overall assessment
        high_cv_periods = stability[stability["cv"] > 0.3]["dev_period"].tolist()
        n_outliers = len(outliers)
        
        issues = []
        if len(high_cv_periods) > 0:
            issues.append(f"High variability in periods: {high_cv_periods}")
        if n_outliers > 0:
            issues.append(f"{n_outliers} outlier factors detected")
        if completeness["completeness_pct"] < 90:
            issues.append("Triangle has missing data")
        
        return {
            "completeness": completeness,
            "stability": stability.to_dict("records"),
            "outliers": outliers.to_dict("records"),
            "issues": issues,
            "overall_quality": (
                "GOOD" if len(issues) == 0
                else "FAIR" if len(issues) == 1
                else "POOR"
            ),
        }
    
    def get_triangle_summary(self) -> dict:
        """
        Get summary statistics of the triangle.
        
        Returns:
            Dictionary with summary statistics
        """
        triangle = self.triangle
        valid_values = triangle[~np.isnan(triangle)]
        
        # Get diagonal (latest values per origin)
        diagonal = []
        for i in range(self.n_origins):
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            if len(valid_idx) > 0:
                diagonal.append(row[valid_idx[-1]])
        
        return {
            "shape": f"{self.n_origins} x {self.n_devs}",
            "total_value": float(np.sum(valid_values)),
            "min_value": float(np.min(valid_values)),
            "max_value": float(np.max(valid_values)),
            "mean_value": float(np.mean(valid_values)),
            "diagonal_sum": float(np.sum(diagonal)),
            "first_period_total": float(np.nansum(triangle[:, 0])),
        }








