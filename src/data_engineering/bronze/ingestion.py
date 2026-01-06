"""
Bronze Layer - Claims Ingestion
===============================

Handles raw data ingestion using Auto Loader for streaming
and batch ingestion for historical data.

Owner: Marcus (Data Engineer)
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from loguru import logger

from .schema import ClaimsSchema, PaymentSchema


class ClaimsIngestion:
    """
    Ingests raw claims and payment data into the Bronze layer.
    
    Uses Delta Lake with append-only mode to preserve full history.
    Supports both streaming (Auto Loader) and batch ingestion.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "unified_reserves",
        schema: str = "bronze",
    ):
        """
        Initialize the claims ingestion handler.
        
        Args:
            spark: Active SparkSession
            catalog: Unity Catalog name
            schema: Database/schema name for bronze layer
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self._claims_schema = ClaimsSchema()
        self._payment_schema = PaymentSchema()
        
    @property
    def claims_table(self) -> str:
        """Full path to claims table."""
        return f"{self.catalog}.{self.schema}.raw_claims"
    
    @property
    def payments_table(self) -> str:
        """Full path to payments table."""
        return f"{self.catalog}.{self.schema}.raw_payments"
    
    def ingest_claims_batch(
        self,
        source_path: str,
        file_format: str = "csv",
    ) -> DataFrame:
        """
        Batch ingest claims from a file path.
        
        Args:
            source_path: Path to source files
            file_format: File format (csv, parquet, json)
            
        Returns:
            DataFrame: Ingested claims data
        """
        logger.info(f"Ingesting claims from {source_path}")
        
        df = (
            self.spark.read
            .format(file_format)
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(self._claims_schema.get_raw_schema())
            .load(source_path)
        )
        
        # Add ingestion metadata
        df = df.withColumn("_ingested_at", current_timestamp())
        df = df.withColumn("_source_file", input_file_name())
        df = df.withColumn("_ingestion_type", lit("batch"))
        
        logger.info(f"Ingested {df.count()} claims records")
        return df
    
    def ingest_claims_streaming(
        self,
        source_path: str,
        checkpoint_path: str,
        file_format: str = "csv",
    ) -> None:
        """
        Stream ingest claims using Auto Loader.
        
        Args:
            source_path: Path to source files
            checkpoint_path: Checkpoint location for streaming
            file_format: File format (csv, parquet, json)
        """
        logger.info(f"Starting streaming ingestion from {source_path}")
        
        stream_df = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
            .option("cloudFiles.inferColumnTypes", "false")
            .schema(self._claims_schema.get_raw_schema())
            .load(source_path)
        )
        
        # Add ingestion metadata
        stream_df = stream_df.withColumn("_ingested_at", current_timestamp())
        stream_df = stream_df.withColumn("_source_file", input_file_name())
        stream_df = stream_df.withColumn("_ingestion_type", lit("streaming"))
        
        # Write to Delta table
        query = (
            stream_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{checkpoint_path}/claims")
            .trigger(availableNow=True)
            .toTable(self.claims_table)
        )
        
        query.awaitTermination()
        logger.info("Streaming ingestion completed")
    
    def ingest_payments_batch(
        self,
        source_path: str,
        file_format: str = "csv",
    ) -> DataFrame:
        """
        Batch ingest payment transactions.
        
        Args:
            source_path: Path to source files
            file_format: File format (csv, parquet, json)
            
        Returns:
            DataFrame: Ingested payments data
        """
        logger.info(f"Ingesting payments from {source_path}")
        
        df = (
            self.spark.read
            .format(file_format)
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(self._payment_schema.get_raw_schema())
            .load(source_path)
        )
        
        # Add ingestion metadata
        df = df.withColumn("_ingested_at", current_timestamp())
        df = df.withColumn("_source_file", input_file_name())
        
        logger.info(f"Ingested {df.count()} payment records")
        return df
    
    def write_to_delta(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "append",
    ) -> None:
        """
        Write DataFrame to Delta table.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            mode: Write mode (append, overwrite)
        """
        full_table = f"{self.catalog}.{self.schema}.{table_name}"
        logger.info(f"Writing to {full_table} with mode={mode}")
        
        (
            df.write
            .format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .saveAsTable(full_table)
        )
        
        logger.info(f"Successfully wrote to {full_table}")
    
    def create_bronze_tables(self) -> None:
        """Create Bronze layer tables if they don't exist."""
        logger.info("Creating Bronze layer tables")
        
        # Create catalog and schema if not exists
        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
        
        # Create claims table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.claims_table} (
                claim_id STRING NOT NULL,
                policy_id STRING NOT NULL,
                loss_date DATE NOT NULL,
                report_date DATE NOT NULL,
                claimant_name STRING,
                loss_type STRING NOT NULL,
                loss_description STRING,
                adjuster_notes STRING,
                claim_status STRING NOT NULL,
                county STRING,
                state STRING,
                zip_code STRING,
                catastrophe_code STRING,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                _ingested_at TIMESTAMP,
                _source_file STRING,
                _ingestion_type STRING
            )
            USING DELTA
            COMMENT 'Raw claims data - Bronze Layer'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'quality' = 'bronze'
            )
        """)
        
        # Create payments table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.payments_table} (
                payment_id STRING NOT NULL,
                claim_id STRING NOT NULL,
                payment_date DATE NOT NULL,
                payment_type STRING NOT NULL,
                payment_amount DOUBLE NOT NULL,
                cumulative_paid DOUBLE,
                reserve_amount DOUBLE,
                payment_status STRING NOT NULL,
                check_number STRING,
                payee_name STRING,
                expense_type STRING,
                created_at TIMESTAMP NOT NULL,
                _ingested_at TIMESTAMP,
                _source_file STRING
            )
            USING DELTA
            COMMENT 'Raw payment transactions - Bronze Layer'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'quality' = 'bronze'
            )
        """)
        
        logger.info("Bronze layer tables created successfully")

