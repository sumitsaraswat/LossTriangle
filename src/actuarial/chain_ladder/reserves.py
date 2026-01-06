"""
Reserve Calculator
==================

Calculates and manages loss reserves using Chain Ladder results.
Writes reserve estimates to the Gold layer.

Owner: Sarah (Actuarial Data Scientist)
"""

from typing import Optional
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, sum as spark_sum,
    when, coalesce,
)
from loguru import logger

from .model import ChainLadderModel


class ReserveCalculator:
    """
    Calculates and manages insurance loss reserves.
    
    Orchestrates the Chain Ladder model and writes results
    to the Gold layer for executive reporting and Genie access.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "unified_reserves",
        silver_schema: str = "silver",
        gold_schema: str = "gold",
    ):
        """
        Initialize the reserve calculator.
        
        Args:
            spark: Active SparkSession
            catalog: Unity Catalog name
            silver_schema: Silver layer schema
            gold_schema: Gold layer schema
        """
        self.spark = spark
        self.catalog = catalog
        self.silver_schema = silver_schema
        self.gold_schema = gold_schema
        
        self._model = ChainLadderModel(spark=spark)
    
    @property
    def triangle_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.loss_triangles"
    
    @property
    def reserves_table(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.reserve_estimates"
    
    @property
    def reserve_summary_table(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.reserve_summary"
    
    @property
    def development_factors_table(self) -> str:
        return f"{self.catalog}.{self.gold_schema}.development_factors"
    
    def calculate_reserves(
        self,
        triangle_type: str = "paid",
        loss_type: Optional[str] = None,
        segment: Optional[str] = None,
        tail_factor: float = 1.0,
    ) -> pd.DataFrame:
        """
        Calculate reserves for a specific triangle.
        
        Args:
            triangle_type: Type of triangle ('paid', 'incurred')
            loss_type: Filter by loss type
            segment: Additional segmentation
            tail_factor: Tail factor for beyond-observed development
            
        Returns:
            DataFrame with reserve estimates
        """
        logger.info(f"Calculating reserves: type={triangle_type}, loss_type={loss_type}")
        
        # Load triangle from Silver layer
        triangle_df = self._load_triangle(triangle_type, loss_type)
        
        if triangle_df.count() == 0:
            logger.warning("No triangle data found")
            return pd.DataFrame()
        
        # Fit Chain Ladder model
        self._model.tail_factor = tail_factor
        self._model.fit(triangle_df.toPandas())
        
        # Get results
        results = self._model.get_results()
        
        # Add metadata
        results["triangle_type"] = triangle_type
        results["loss_type"] = loss_type or "ALL"
        results["segment"] = segment or "ALL"
        results["tail_factor"] = tail_factor
        
        logger.info(f"Total IBNR: ${self._model.get_total_ibnr():,.0f}")
        logger.info(f"Total Ultimate: ${self._model.get_total_ultimate():,.0f}")
        
        return results
    
    def _load_triangle(
        self,
        triangle_type: str,
        loss_type: Optional[str] = None,
    ) -> DataFrame:
        """Load triangle from Silver layer."""
        df = self.spark.table(self.triangle_table)
        
        df = df.filter(col("triangle_type") == triangle_type)
        
        if loss_type:
            df = df.filter(col("loss_type") == loss_type)
        
        # Pivot to wide format
        triangle = (
            df
            .groupBy("origin_period")
            .pivot("development_period")
            .agg(spark_sum("cumulative_value"))
            .orderBy("origin_period")
        )
        
        return triangle
    
    def calculate_all_segments(
        self,
        segments: list[dict],
        tail_factor: float = 1.0,
    ) -> pd.DataFrame:
        """
        Calculate reserves for multiple segments.
        
        Args:
            segments: List of segment definitions
                      [{"triangle_type": "paid", "loss_type": "Water Damage"}, ...]
            tail_factor: Tail factor to use
            
        Returns:
            Combined DataFrame with all segment results
        """
        all_results = []
        
        for seg in segments:
            results = self.calculate_reserves(
                triangle_type=seg.get("triangle_type", "paid"),
                loss_type=seg.get("loss_type"),
                segment=seg.get("segment"),
                tail_factor=tail_factor,
            )
            all_results.append(results)
        
        if all_results:
            return pd.concat(all_results, ignore_index=True)
        return pd.DataFrame()
    
    def calculate_standard_vs_ai_adjusted(
        self,
        risk_adjusted_triangle: DataFrame,
        standard_triangle: DataFrame,
    ) -> dict:
        """
        Compare standard Chain Ladder with AI-adjusted reserves.
        
        This is the key innovation: running side-by-side models
        to show how AI risk detection impacts reserve estimates.
        
        Args:
            risk_adjusted_triangle: Triangle with only high-risk claims
            standard_triangle: Standard triangle with all claims
            
        Returns:
            Comparison dictionary with both estimates
        """
        logger.info("Running Standard vs AI-Adjusted comparison")
        
        # Standard model
        standard_model = ChainLadderModel()
        standard_model.fit(standard_triangle.toPandas())
        standard_results = standard_model.get_results()
        
        # AI-Adjusted model (high-risk claims only)
        adjusted_model = ChainLadderModel()
        adjusted_model.fit(risk_adjusted_triangle.toPandas())
        adjusted_results = adjusted_model.get_results()
        
        comparison = {
            "standard": {
                "total_ibnr": standard_model.get_total_ibnr(),
                "total_ultimate": standard_model.get_total_ultimate(),
                "results": standard_results,
            },
            "ai_adjusted": {
                "total_ibnr": adjusted_model.get_total_ibnr(),
                "total_ultimate": adjusted_model.get_total_ultimate(),
                "results": adjusted_results,
            },
            "difference": {
                "ibnr_increase": (
                    adjusted_model.get_total_ibnr() - standard_model.get_total_ibnr()
                ),
                "ibnr_increase_pct": (
                    (adjusted_model.get_total_ibnr() / standard_model.get_total_ibnr() - 1) * 100
                    if standard_model.get_total_ibnr() > 0 else 0
                ),
            },
        }
        
        logger.info(
            f"Standard IBNR: ${comparison['standard']['total_ibnr']:,.0f}, "
            f"AI-Adjusted IBNR: ${comparison['ai_adjusted']['total_ibnr']:,.0f}, "
            f"Difference: {comparison['difference']['ibnr_increase_pct']:.1f}%"
        )
        
        return comparison
    
    def write_reserves_to_gold(
        self,
        reserves_df: pd.DataFrame,
        mode: str = "append",
    ) -> None:
        """
        Write reserve estimates to Gold layer.
        
        Args:
            reserves_df: Reserve estimates DataFrame
            mode: Write mode
        """
        logger.info(f"Writing reserves to {self.reserves_table}")
        
        # Convert to Spark DataFrame
        spark_df = self.spark.createDataFrame(reserves_df)
        spark_df = spark_df.withColumn("_calculated_at", current_timestamp())
        
        spark_df.write.format("delta").mode(mode).saveAsTable(self.reserves_table)
        
        logger.info("Reserves written successfully")
    
    def create_gold_tables(self) -> None:
        """Create Gold layer tables if they don't exist."""
        logger.info("Creating Gold layer tables")
        
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.gold_schema}")
        
        # Reserve estimates table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.reserves_table} (
                origin_period INT NOT NULL,
                latest_cumulative DOUBLE,
                ultimate_loss DOUBLE,
                ibnr DOUBLE,
                percent_reported DOUBLE,
                triangle_type STRING NOT NULL,
                loss_type STRING,
                segment STRING,
                tail_factor DOUBLE,
                _calculated_at TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (triangle_type, loss_type)
            COMMENT 'Chain Ladder reserve estimates - Gold Layer'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'quality' = 'gold'
            )
        """)
        
        # Reserve summary table (for Genie)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.reserve_summary_table} (
                as_of_date DATE NOT NULL,
                triangle_type STRING NOT NULL,
                loss_type STRING,
                total_paid DOUBLE,
                total_ibnr DOUBLE,
                total_ultimate DOUBLE,
                reserve_adequacy DOUBLE,
                held_reserves DOUBLE,
                reserve_margin DOUBLE,
                ai_adjustment_factor DOUBLE,
                _calculated_at TIMESTAMP
            )
            USING DELTA
            COMMENT 'Reserve summary for executive reporting - Gold Layer'
            TBLPROPERTIES (
                'quality' = 'gold'
            )
        """)
        
        # Development factors table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.development_factors_table} (
                triangle_type STRING NOT NULL,
                loss_type STRING,
                development_period INT NOT NULL,
                ata_factor DOUBLE,
                cdf_to_ultimate DOUBLE,
                _calculated_at TIMESTAMP
            )
            USING DELTA
            COMMENT 'Development factors by triangle - Gold Layer'
            TBLPROPERTIES (
                'quality' = 'gold'
            )
        """)
        
        logger.info("Gold layer tables created successfully")
    
    def calculate_reserve_adequacy(
        self,
        held_reserves: float,
        loss_type: Optional[str] = None,
    ) -> dict:
        """
        Calculate reserve adequacy metrics.
        
        Reserve Adequacy = Held Reserves / Estimated Ultimate Loss
        
        Args:
            held_reserves: Current held reserves
            loss_type: Filter by loss type
            
        Returns:
            Adequacy metrics dictionary
        """
        if not self._model.is_fitted:
            raise ValueError("Must calculate reserves first")
        
        ultimate = self._model.get_total_ultimate()
        ibnr = self._model.get_total_ibnr()
        
        adequacy = held_reserves / ultimate if ultimate > 0 else 1.0
        margin = held_reserves - ibnr
        
        return {
            "held_reserves": held_reserves,
            "estimated_ultimate": ultimate,
            "estimated_ibnr": ibnr,
            "reserve_adequacy": adequacy,
            "reserve_margin": margin,
            "adequacy_status": (
                "ADEQUATE" if adequacy >= 1.0
                else "MARGINAL" if adequacy >= 0.85
                else "DEFICIENT"
            ),
        }

