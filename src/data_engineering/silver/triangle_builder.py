"""
Silver Layer - Triangle Builder
===============================

Builds actuarial loss triangles from raw claims and payment data.
Handles the pivot from linear transactions to 2D triangle matrix.

Owner: Marcus (Data Engineer)
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, quarter,
    sum as spark_sum, count, max as spark_max,
    pivot, coalesce, lit, current_timestamp,
)
from loguru import logger

from .transformations import TriangleTransformations


class TriangleBuilder:
    """
    Builds loss triangles from transactional claims data.
    
    Creates both incremental and cumulative triangles at
    various granularities (annual, quarterly, monthly).
    """
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "unified_reserves",
        bronze_schema: str = "bronze",
        silver_schema: str = "silver",
    ):
        """
        Initialize the triangle builder.
        
        Args:
            spark: Active SparkSession
            catalog: Unity Catalog name
            bronze_schema: Bronze layer schema name
            silver_schema: Silver layer schema name
        """
        self.spark = spark
        self.catalog = catalog
        self.bronze_schema = bronze_schema
        self.silver_schema = silver_schema
        self._transformations = TriangleTransformations()
        
    @property
    def claims_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.raw_claims"
    
    @property
    def payments_table(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}.raw_payments"
    
    @property
    def triangle_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.loss_triangles"
    
    @property
    def enriched_claims_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.enriched_claims"
    
    def build_paid_triangle(
        self,
        loss_type: Optional[str] = None,
        state: Optional[str] = None,
        period_type: str = "year",
        max_dev_periods: int = 10,
        as_of_date: Optional[str] = None,
    ) -> DataFrame:
        """
        Build a cumulative paid loss triangle.
        
        Args:
            loss_type: Filter by loss type (e.g., 'Water Damage')
            state: Filter by state
            period_type: Granularity ('year', 'quarter', 'month')
            max_dev_periods: Maximum development periods
            as_of_date: Evaluation date (defaults to current)
            
        Returns:
            DataFrame in triangle format (origin x development)
        """
        logger.info(f"Building paid triangle: type={loss_type}, state={state}")
        
        # Load and join claims with payments
        claims_df = self.spark.table(self.claims_table)
        payments_df = self.spark.table(self.payments_table)
        
        # Apply filters
        if loss_type:
            claims_df = claims_df.filter(col("loss_type") == loss_type)
        if state:
            claims_df = claims_df.filter(col("state") == state)
            
        # Join claims with payments
        joined_df = payments_df.join(
            claims_df.select("claim_id", "loss_date", "loss_type", "state", "county"),
            on="claim_id",
            how="inner"
        )
        
        # Add origin and development periods
        triangle_df = self._transformations.add_origin_period(
            joined_df, "loss_date", period_type
        )
        triangle_df = self._transformations.add_development_period(
            triangle_df, "loss_date", "payment_date", period_type
        )
        
        # Filter valid development periods
        triangle_df = self._transformations.filter_valid_development(
            triangle_df, max_dev_periods
        )
        
        # Aggregate to cell level (cumulative)
        cell_df = (
            triangle_df
            .groupBy("origin_period", "development_period")
            .agg(
                spark_sum("cumulative_paid").alias("cumulative_loss"),
                count("claim_id").alias("claim_count"),
            )
        )
        
        # Pivot to triangle format
        triangle = (
            cell_df
            .groupBy("origin_period")
            .pivot("development_period")
            .agg(spark_sum("cumulative_loss"))
            .orderBy("origin_period")
        )
        
        logger.info(f"Built triangle with {triangle.count()} origin periods")
        return triangle
    
    def build_incurred_triangle(
        self,
        loss_type: Optional[str] = None,
        period_type: str = "year",
        max_dev_periods: int = 10,
    ) -> DataFrame:
        """
        Build an incurred loss triangle (paid + reserves).
        
        Args:
            loss_type: Filter by loss type
            period_type: Granularity
            max_dev_periods: Maximum development periods
            
        Returns:
            DataFrame in triangle format
        """
        logger.info(f"Building incurred triangle: type={loss_type}")
        
        # Similar to paid triangle but includes reserve_amount
        claims_df = self.spark.table(self.claims_table)
        payments_df = self.spark.table(self.payments_table)
        
        if loss_type:
            claims_df = claims_df.filter(col("loss_type") == loss_type)
        
        joined_df = payments_df.join(
            claims_df.select("claim_id", "loss_date", "loss_type"),
            on="claim_id",
            how="inner"
        )
        
        # Calculate incurred = paid + reserve
        joined_df = joined_df.withColumn(
            "incurred_amount",
            col("cumulative_paid") + coalesce(col("reserve_amount"), lit(0))
        )
        
        triangle_df = self._transformations.add_origin_period(
            joined_df, "loss_date", period_type
        )
        triangle_df = self._transformations.add_development_period(
            triangle_df, "loss_date", "payment_date", period_type
        )
        
        triangle_df = self._transformations.filter_valid_development(
            triangle_df, max_dev_periods
        )
        
        # Get latest incurred value per origin/dev combination
        cell_df = (
            triangle_df
            .groupBy("origin_period", "development_period")
            .agg(spark_max("incurred_amount").alias("incurred_loss"))
        )
        
        triangle = (
            cell_df
            .groupBy("origin_period")
            .pivot("development_period")
            .agg(spark_sum("incurred_loss"))
            .orderBy("origin_period")
        )
        
        return triangle
    
    def build_enriched_claims(self) -> DataFrame:
        """
        Build enriched claims dataset for NLP processing.
        
        Joins claims with latest payment status and prepares
        for risk flagging by the AI module.
        
        Returns:
            DataFrame with enriched claim data
        """
        logger.info("Building enriched claims dataset")
        
        claims_df = self.spark.table(self.claims_table)
        payments_df = self.spark.table(self.payments_table)
        
        # Get latest payment per claim
        latest_payments = (
            payments_df
            .groupBy("claim_id")
            .agg(
                spark_max("payment_date").alias("latest_payment_date"),
                spark_sum("payment_amount").alias("total_paid"),
                spark_max("reserve_amount").alias("current_reserve"),
            )
        )
        
        # Join and add development info
        enriched = claims_df.join(latest_payments, on="claim_id", how="left")
        
        enriched = self._transformations.add_origin_period(enriched, "loss_date", "year")
        
        # Add calculated fields
        enriched = enriched.withColumn(
            "incurred_amount",
            coalesce(col("total_paid"), lit(0)) + 
            coalesce(col("current_reserve"), lit(0))
        )
        
        enriched = enriched.withColumn("_processed_at", current_timestamp())
        
        logger.info(f"Built enriched dataset with {enriched.count()} claims")
        return enriched
    
    def create_silver_tables(self) -> None:
        """Create Silver layer tables if they don't exist."""
        logger.info("Creating Silver layer tables")
        
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.silver_schema}")
        
        # Enriched claims table (for NLP processing)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.enriched_claims_table} (
                claim_id STRING NOT NULL,
                policy_id STRING NOT NULL,
                loss_date DATE NOT NULL,
                report_date DATE NOT NULL,
                origin_year INT NOT NULL,
                origin_period INT NOT NULL,
                loss_type STRING NOT NULL,
                loss_description STRING,
                adjuster_notes STRING,
                claim_status STRING NOT NULL,
                county STRING,
                state STRING,
                catastrophe_code STRING,
                total_paid DOUBLE,
                current_reserve DOUBLE,
                incurred_amount DOUBLE,
                risk_category STRING,
                risk_score DOUBLE,
                risk_keywords ARRAY<STRING>,
                _processed_at TIMESTAMP
            )
            USING DELTA
            COMMENT 'Enriched claims with risk flags - Silver Layer'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'quality' = 'silver'
            )
        """)
        
        # Loss triangles table (long format for flexibility)
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.triangle_table} (
                triangle_id STRING NOT NULL,
                triangle_type STRING NOT NULL,
                loss_type STRING,
                state STRING,
                county STRING,
                origin_period INT NOT NULL,
                development_period INT NOT NULL,
                cumulative_value DOUBLE,
                incremental_value DOUBLE,
                claim_count INT,
                period_type STRING NOT NULL,
                as_of_date DATE NOT NULL,
                _created_at TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (triangle_type, loss_type)
            COMMENT 'Loss triangles in long format - Silver Layer'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'quality' = 'silver'
            )
        """)
        
        logger.info("Silver layer tables created successfully")
    
    def write_triangle(
        self,
        triangle_df: DataFrame,
        triangle_type: str,
        loss_type: Optional[str] = None,
        mode: str = "append",
    ) -> None:
        """
        Write triangle to Delta table.
        
        Args:
            triangle_df: Triangle DataFrame
            triangle_type: Type of triangle ('paid', 'incurred')
            loss_type: Loss type filter used
            mode: Write mode
        """
        logger.info(f"Writing {triangle_type} triangle to Delta")
        
        triangle_df.write.format("delta").mode(mode).saveAsTable(
            self.triangle_table
        )

