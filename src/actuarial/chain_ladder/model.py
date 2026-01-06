"""
Chain Ladder Model
==================

Implementation of the Chain Ladder actuarial method for
loss reserve estimation and IBNR calculation.

The Chain Ladder method uses historical loss development patterns
to project ultimate losses for immature accident years.

Owner: Sarah (Actuarial Data Scientist)
"""

from typing import Optional, Union
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from loguru import logger

# Optional: use chainladder library if available
try:
    import chainladder as cl
    CHAINLADDER_AVAILABLE = True
except ImportError:
    CHAINLADDER_AVAILABLE = False
    logger.warning("chainladder library not available - using custom implementation")


class ChainLadderModel:
    """
    Chain Ladder model for loss reserve estimation.
    
    The Chain Ladder (or Development) method assumes that
    losses develop in a predictable pattern based on historical
    age-to-age (ATA) factors.
    
    Key Formula:
        Ultimate Loss = Latest Cumulative Loss × CDF
        IBNR = Ultimate Loss - Latest Cumulative Loss
        
    Where CDF (Cumulative Development Factor) is the product
    of all remaining ATA factors.
    """
    
    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        n_periods: int = 10,
        tail_factor: float = 1.0,
    ):
        """
        Initialize the Chain Ladder model.
        
        Args:
            spark: Optional SparkSession for Delta operations
            n_periods: Number of development periods
            tail_factor: Factor for development beyond observed periods
        """
        self.spark = spark
        self.n_periods = n_periods
        self.tail_factor = tail_factor
        
        # Model state
        self._triangle: Optional[pd.DataFrame] = None
        self._ata_factors: Optional[np.ndarray] = None
        self._cdf: Optional[np.ndarray] = None
        self._ultimate: Optional[np.ndarray] = None
        self._ibnr: Optional[np.ndarray] = None
        self._is_fitted = False
    
    @property
    def is_fitted(self) -> bool:
        """Check if model has been fitted."""
        return self._is_fitted
    
    def fit(
        self,
        triangle: Union[pd.DataFrame, DataFrame],
        value_col: Optional[str] = None,
    ) -> "ChainLadderModel":
        """
        Fit the Chain Ladder model to a loss triangle.
        
        Args:
            triangle: Loss triangle (DataFrame or pd.DataFrame)
            value_col: Column name for values (if long format)
            
        Returns:
            Self (fitted model)
        """
        logger.info("Fitting Chain Ladder model")
        
        # Convert to pandas if needed
        if hasattr(triangle, 'toPandas'):
            triangle = triangle.toPandas()
        
        # Store triangle
        self._triangle = self._prepare_triangle(triangle, value_col)
        
        # Calculate ATA factors
        self._ata_factors = self._calculate_ata_factors()
        
        # Calculate cumulative development factors
        self._cdf = self._calculate_cdf()
        
        # Project ultimate losses
        self._ultimate = self._project_ultimate()
        
        # Calculate IBNR
        self._ibnr = self._calculate_ibnr()
        
        self._is_fitted = True
        logger.info("Chain Ladder model fitted successfully")
        
        return self
    
    def _prepare_triangle(
        self,
        triangle: pd.DataFrame,
        value_col: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Prepare triangle for analysis.
        
        Converts long format to wide format if needed.
        """
        # If already in wide format (origin periods as index, dev periods as columns)
        if triangle.index.name or len(triangle.columns) > 3:
            return triangle.values if isinstance(triangle, pd.DataFrame) else triangle
        
        # Long format - need to pivot
        if value_col is None:
            # Find numeric column
            numeric_cols = triangle.select_dtypes(include=[np.number]).columns
            value_col = numeric_cols[-1]  # Last numeric column
        
        # Identify origin and dev columns
        cols = [c for c in triangle.columns if c != value_col]
        origin_col, dev_col = cols[0], cols[1]
        
        pivoted = triangle.pivot(
            index=origin_col,
            columns=dev_col,
            values=value_col
        )
        
        return pivoted.values
    
    def _calculate_ata_factors(self) -> np.ndarray:
        """
        Calculate Age-to-Age (ATA) development factors.
        
        ATA Factor = Sum(Loss[t+1]) / Sum(Loss[t])
        
        Uses volume-weighted average across all origin periods.
        """
        triangle = self._triangle
        n_rows, n_cols = triangle.shape
        ata = np.zeros(n_cols - 1)
        
        for j in range(n_cols - 1):
            # Get pairs of adjacent columns where both values exist
            col_current = triangle[:, j]
            col_next = triangle[:, j + 1]
            
            # Mask for valid pairs (both non-null)
            valid = ~np.isnan(col_current) & ~np.isnan(col_next)
            
            if np.sum(valid) > 0:
                ata[j] = np.sum(col_next[valid]) / np.sum(col_current[valid])
            else:
                ata[j] = 1.0
        
        logger.debug(f"ATA Factors: {ata}")
        return ata
    
    def _calculate_cdf(self) -> np.ndarray:
        """
        Calculate Cumulative Development Factors (CDF).
        
        CDF[j] = Product of ATA[j:] × tail_factor
        
        The CDF represents how much a loss at age j will
        develop to ultimate.
        """
        ata = self._ata_factors
        n = len(ata)
        cdf = np.zeros(n + 1)
        
        # CDF at ultimate is the tail factor
        cdf[-1] = self.tail_factor
        
        # Work backwards
        for j in range(n - 1, -1, -1):
            cdf[j] = ata[j] * cdf[j + 1]
        
        logger.debug(f"CDF: {cdf}")
        return cdf
    
    def _project_ultimate(self) -> np.ndarray:
        """
        Project ultimate losses for each origin period.
        
        Ultimate = Latest Diagonal × CDF at current age
        """
        triangle = self._triangle
        cdf = self._cdf
        n_rows = triangle.shape[0]
        ultimate = np.zeros(n_rows)
        
        for i in range(n_rows):
            # Find latest non-null value (diagonal position)
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            
            if len(valid_idx) > 0:
                latest_idx = valid_idx[-1]
                latest_value = row[latest_idx]
                ultimate[i] = latest_value * cdf[latest_idx]
            else:
                ultimate[i] = 0
        
        return ultimate
    
    def _calculate_ibnr(self) -> np.ndarray:
        """
        Calculate IBNR (Incurred But Not Reported) reserves.
        
        IBNR = Ultimate Loss - Latest Cumulative Loss
        """
        triangle = self._triangle
        ultimate = self._ultimate
        n_rows = triangle.shape[0]
        ibnr = np.zeros(n_rows)
        
        for i in range(n_rows):
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            
            if len(valid_idx) > 0:
                latest_value = row[valid_idx[-1]]
                ibnr[i] = ultimate[i] - latest_value
            else:
                ibnr[i] = ultimate[i]
        
        return ibnr
    
    def get_ata_factors(self) -> pd.DataFrame:
        """
        Get Age-to-Age factors as DataFrame.
        
        Returns:
            DataFrame with ATA factors by development period
        """
        if not self._is_fitted:
            raise ValueError("Model must be fitted first")
        
        return pd.DataFrame({
            "development_period": range(1, len(self._ata_factors) + 1),
            "ata_factor": self._ata_factors,
        })
    
    def get_cdf(self) -> pd.DataFrame:
        """
        Get Cumulative Development Factors as DataFrame.
        
        Returns:
            DataFrame with CDF by development period
        """
        if not self._is_fitted:
            raise ValueError("Model must be fitted first")
        
        return pd.DataFrame({
            "development_period": range(1, len(self._cdf) + 1),
            "cdf": self._cdf,
        })
    
    def get_results(self) -> pd.DataFrame:
        """
        Get complete results as DataFrame.
        
        Returns:
            DataFrame with origin period, latest, ultimate, and IBNR
        """
        if not self._is_fitted:
            raise ValueError("Model must be fitted first")
        
        triangle = self._triangle
        n_rows = triangle.shape[0]
        
        # Get latest values
        latest = []
        for i in range(n_rows):
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            latest.append(row[valid_idx[-1]] if len(valid_idx) > 0 else 0)
        
        return pd.DataFrame({
            "origin_period": range(1, n_rows + 1),
            "latest_cumulative": latest,
            "ultimate_loss": self._ultimate,
            "ibnr": self._ibnr,
            "percent_reported": [l / u if u > 0 else 1.0 for l, u in zip(latest, self._ultimate)],
        })
    
    def get_total_ibnr(self) -> float:
        """Get total IBNR across all origin periods."""
        if not self._is_fitted:
            raise ValueError("Model must be fitted first")
        return float(np.sum(self._ibnr))
    
    def get_total_ultimate(self) -> float:
        """Get total ultimate losses across all origin periods."""
        if not self._is_fitted:
            raise ValueError("Model must be fitted first")
        return float(np.sum(self._ultimate))
    
    def predict_development(
        self,
        origin_period: int,
        current_value: float,
        current_age: int,
    ) -> dict:
        """
        Predict future development for a single origin period.
        
        Args:
            origin_period: Origin period identifier
            current_value: Current cumulative loss
            current_age: Current development period
            
        Returns:
            Dictionary with projected values
        """
        if not self._is_fitted:
            raise ValueError("Model must be fitted first")
        
        if current_age < 1 or current_age > len(self._cdf):
            raise ValueError(f"Invalid age: {current_age}")
        
        cdf_at_age = self._cdf[current_age - 1]
        ultimate = current_value * cdf_at_age
        ibnr = ultimate - current_value
        
        return {
            "origin_period": origin_period,
            "current_value": current_value,
            "current_age": current_age,
            "cdf_to_ultimate": cdf_at_age,
            "projected_ultimate": ultimate,
            "projected_ibnr": ibnr,
            "percent_reported": current_value / ultimate if ultimate > 0 else 1.0,
        }


def fit_with_chainladder(
    triangle_df: pd.DataFrame,
    origin_col: str = "origin_period",
    dev_col: str = "development_period",
    value_col: str = "cumulative_loss",
) -> dict:
    """
    Fit using the chainladder library (if available).
    
    Args:
        triangle_df: Triangle data in long format
        origin_col: Origin period column
        dev_col: Development period column
        value_col: Value column
        
    Returns:
        Dictionary with model results
    """
    if not CHAINLADDER_AVAILABLE:
        raise ImportError("chainladder library not installed")
    
    # Create chainladder triangle
    tri = cl.Triangle(
        data=triangle_df,
        origin=origin_col,
        development=dev_col,
        columns=value_col,
    )
    
    # Fit model
    model = cl.Chainladder().fit(tri)
    
    return {
        "ultimate": model.ultimate_.to_frame(),
        "ibnr": model.ibnr_.to_frame(),
        "ldf": model.ldf_.to_frame(),
        "cdf": model.cdf_.to_frame(),
    }








