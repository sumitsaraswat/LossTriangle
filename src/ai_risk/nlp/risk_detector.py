"""
Risk Detector
=============

Main risk detection engine combining keyword extraction
with optional ML-based classification.

Owner: Anya (AI Engineer)
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp,
    sum as spark_sum, count, avg,
)
from loguru import logger

from .keyword_extractor import KeywordExtractor


class RiskDetector:
    """
    Detects hidden risks in insurance claims using NLP.
    
    Combines rule-based keyword extraction with optional
    ML models for more sophisticated risk classification.
    
    The "Early Warning System" - flags claims with potential
    long-tail risks before they develop into expensive losses.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "unified_reserves",
        silver_schema: str = "silver",
        use_ml_model: bool = False,
        model_path: Optional[str] = None,
    ):
        """
        Initialize the risk detector.
        
        Args:
            spark: Active SparkSession
            catalog: Unity Catalog name
            silver_schema: Silver layer schema
            use_ml_model: Whether to use ML model
            model_path: Path to trained model
        """
        self.spark = spark
        self.catalog = catalog
        self.silver_schema = silver_schema
        self.use_ml_model = use_ml_model
        self.model_path = model_path
        
        self._keyword_extractor = KeywordExtractor()
        self._model = None
        
        if use_ml_model and model_path:
            self._load_model()
    
    @property
    def enriched_claims_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.enriched_claims"
    
    @property
    def risk_summary_table(self) -> str:
        return f"{self.catalog}.{self.silver_schema}.risk_summary"
    
    def _load_model(self) -> None:
        """Load pre-trained ML model for risk classification."""
        try:
            # Placeholder for ML model loading
            # Could use MLflow, Hugging Face, or custom model
            logger.info(f"Loading model from {self.model_path}")
            # self._model = mlflow.pyfunc.load_model(self.model_path)
            logger.warning("ML model loading not implemented - using keyword-based detection")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            self._model = None
    
    def analyze_claims(
        self,
        df: Optional[DataFrame] = None,
        text_columns: list[str] = ["adjuster_notes", "loss_description"],
    ) -> DataFrame:
        """
        Analyze claims for risk indicators.
        
        Args:
            df: Input DataFrame (or reads from enriched_claims table)
            text_columns: Columns to analyze for risk
            
        Returns:
            DataFrame with risk analysis results
        """
        if df is None:
            logger.info(f"Reading from {self.enriched_claims_table}")
            df = self.spark.table(self.enriched_claims_table)
        
        logger.info(f"Analyzing {df.count()} claims for risk indicators")
        
        # Apply keyword-based risk detection
        df = self._keyword_extractor.enrich_dataframe(
            df,
            text_col=text_columns[0],
            include_description=len(text_columns) > 1,
        )
        
        # Apply ML model if available
        if self._model is not None:
            df = self._apply_ml_model(df)
        
        # Add processing metadata
        df = df.withColumn("_risk_analyzed_at", current_timestamp())
        
        return df
    
    def _apply_ml_model(self, df: DataFrame) -> DataFrame:
        """
        Apply ML model for enhanced risk detection.
        
        Args:
            df: Input DataFrame with text columns
            
        Returns:
            DataFrame with ML-based risk scores
        """
        # Placeholder for ML model inference
        # Would typically use a transformer model or custom classifier
        logger.info("Applying ML model for risk classification")
        
        # For now, just add placeholder columns
        df = df.withColumn("ml_risk_score", lit(None).cast("double"))
        df = df.withColumn("ml_risk_category", lit(None).cast("string"))
        
        return df
    
    def generate_risk_summary(
        self,
        df: DataFrame,
        group_by: list[str] = ["origin_year", "loss_type"],
    ) -> DataFrame:
        """
        Generate risk summary statistics.
        
        Args:
            df: Analyzed claims DataFrame
            group_by: Columns to group by
            
        Returns:
            Summary DataFrame with risk metrics
        """
        logger.info(f"Generating risk summary grouped by {group_by}")
        
        summary = df.groupBy(*group_by).agg(
            count("*").alias("total_claims"),
            spark_sum(when(col("high_risk_flag"), 1).otherwise(0)).alias("high_risk_claims"),
            avg("risk_score").alias("avg_risk_score"),
            spark_sum("incurred_amount").alias("total_incurred"),
            spark_sum(
                when(col("high_risk_flag"), col("incurred_amount")).otherwise(0)
            ).alias("high_risk_incurred"),
        )
        
        # Calculate percentages
        summary = summary.withColumn(
            "high_risk_pct",
            col("high_risk_claims") / col("total_claims") * 100
        )
        summary = summary.withColumn(
            "high_risk_incurred_pct",
            col("high_risk_incurred") / col("total_incurred") * 100
        )
        
        return summary.orderBy(*group_by)
    
    def flag_emerging_risks(
        self,
        df: DataFrame,
        threshold_score: float = 0.5,
        threshold_incurred: float = 50000,
    ) -> DataFrame:
        """
        Flag claims with emerging risk patterns.
        
        Identifies claims that:
        1. Have high risk scores
        2. Are above threshold incurred amounts
        3. Show characteristics of long-tail development
        
        Args:
            df: Analyzed claims DataFrame
            threshold_score: Minimum risk score
            threshold_incurred: Minimum incurred amount
            
        Returns:
            DataFrame of flagged claims
        """
        logger.info("Flagging emerging risk claims")
        
        flagged = df.filter(
            (col("risk_score") >= threshold_score) |
            (
                (col("incurred_amount") >= threshold_incurred) &
                (col("risk_score") >= threshold_score * 0.5)
            )
        )
        
        # Add priority ranking
        flagged = flagged.withColumn(
            "risk_priority",
            when(col("risk_score") >= 0.8, "CRITICAL")
            .when(col("risk_score") >= 0.6, "HIGH")
            .when(col("risk_score") >= 0.4, "MEDIUM")
            .otherwise("LOW")
        )
        
        logger.info(f"Flagged {flagged.count()} emerging risk claims")
        return flagged
    
    def get_risk_by_category(self, df: DataFrame) -> DataFrame:
        """
        Get risk breakdown by category.
        
        Args:
            df: Analyzed claims DataFrame
            
        Returns:
            DataFrame with risk category breakdown
        """
        return (
            df
            .filter(col("risk_category") != "low_risk")
            .groupBy("risk_category")
            .agg(
                count("*").alias("claim_count"),
                avg("risk_score").alias("avg_severity"),
                spark_sum("incurred_amount").alias("total_exposure"),
            )
            .orderBy(col("total_exposure").desc())
        )
    
    def write_analyzed_claims(
        self,
        df: DataFrame,
        mode: str = "overwrite",
    ) -> None:
        """
        Write analyzed claims back to Delta table.
        
        Args:
            df: Analyzed claims DataFrame
            mode: Write mode
        """
        logger.info(f"Writing analyzed claims to {self.enriched_claims_table}")
        
        df.write.format("delta").mode(mode).saveAsTable(
            self.enriched_claims_table
        )
        
        logger.info("Write complete")
    
    def write_risk_summary(
        self,
        summary_df: DataFrame,
        mode: str = "overwrite",
    ) -> None:
        """
        Write risk summary to Delta table.
        
        Args:
            summary_df: Risk summary DataFrame
            mode: Write mode
        """
        # Create table if not exists
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.risk_summary_table} (
                origin_year INT,
                loss_type STRING,
                total_claims BIGINT,
                high_risk_claims BIGINT,
                avg_risk_score DOUBLE,
                total_incurred DOUBLE,
                high_risk_incurred DOUBLE,
                high_risk_pct DOUBLE,
                high_risk_incurred_pct DOUBLE,
                _created_at TIMESTAMP
            )
            USING DELTA
            COMMENT 'Risk summary by origin year and loss type'
        """)
        
        summary_df = summary_df.withColumn("_created_at", current_timestamp())
        
        summary_df.write.format("delta").mode(mode).saveAsTable(
            self.risk_summary_table
        )
        
        logger.info(f"Risk summary written to {self.risk_summary_table}")

