"""
Silver Layer Transformations
============================

Core transformation logic for preparing data for triangle construction.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, year, month, quarter,
    months_between, floor, ceil,
    sum as spark_sum, count, avg,
    when, lit, coalesce,
    datediff, to_date,
)
from pyspark.sql.window import Window


class TriangleTransformations:
    """
    Transformation utilities for loss triangle construction.
    
    Handles the complex logic of:
    - Calculating development periods
    - Aggregating by origin period
    - Creating cumulative loss measures
    """
    
    @staticmethod
    def add_origin_period(
        df: DataFrame,
        loss_date_col: str = "loss_date",
        period_type: str = "year",
    ) -> DataFrame:
        """
        Add origin period columns based on loss date.
        
        Args:
            df: Input DataFrame
            loss_date_col: Name of the loss date column
            period_type: 'year', 'quarter', or 'month'
            
        Returns:
            DataFrame with origin period columns
        """
        df = df.withColumn("origin_year", year(col(loss_date_col)))
        df = df.withColumn("origin_quarter", quarter(col(loss_date_col)))
        df = df.withColumn("origin_month", month(col(loss_date_col)))
        
        # Create period key based on granularity
        if period_type == "year":
            df = df.withColumn("origin_period", col("origin_year"))
        elif period_type == "quarter":
            df = df.withColumn(
                "origin_period",
                col("origin_year") * 10 + col("origin_quarter")
            )
        else:  # month
            df = df.withColumn(
                "origin_period",
                col("origin_year") * 100 + col("origin_month")
            )
            
        return df
    
    @staticmethod
    def add_development_period(
        df: DataFrame,
        loss_date_col: str = "loss_date",
        eval_date_col: str = "payment_date",
        period_type: str = "year",
    ) -> DataFrame:
        """
        Calculate development period (age) from origin to evaluation.
        
        Args:
            df: Input DataFrame
            loss_date_col: Name of the loss date column
            eval_date_col: Name of the evaluation/payment date column
            period_type: 'year', 'quarter', or 'month'
            
        Returns:
            DataFrame with development period column
        """
        # Calculate months between loss and evaluation
        df = df.withColumn(
            "_months_developed",
            months_between(col(eval_date_col), col(loss_date_col))
        )
        
        # Convert to development period
        if period_type == "year":
            df = df.withColumn(
                "development_period",
                (floor(col("_months_developed") / 12) + 1).cast("int")
            )
        elif period_type == "quarter":
            df = df.withColumn(
                "development_period",
                (floor(col("_months_developed") / 3) + 1).cast("int")
            )
        else:  # month
            df = df.withColumn(
                "development_period",
                (floor(col("_months_developed")) + 1).cast("int")
            )
        
        # Clean up intermediate column
        df = df.drop("_months_developed")
        
        return df
    
    @staticmethod
    def calculate_cumulative_losses(
        df: DataFrame,
        partition_cols: list[str],
        order_col: str,
        amount_col: str,
    ) -> DataFrame:
        """
        Calculate cumulative losses over development periods.
        
        Args:
            df: Input DataFrame
            partition_cols: Columns to partition by (e.g., claim_id)
            order_col: Column to order by (e.g., development_period)
            amount_col: Column containing loss amounts
            
        Returns:
            DataFrame with cumulative loss column
        """
        window = (
            Window
            .partitionBy(*partition_cols)
            .orderBy(order_col)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        
        df = df.withColumn(
            "cumulative_loss",
            spark_sum(col(amount_col)).over(window)
        )
        
        return df
    
    @staticmethod
    def aggregate_to_triangle_cell(
        df: DataFrame,
        origin_col: str = "origin_period",
        dev_col: str = "development_period",
        value_col: str = "cumulative_loss",
        agg_type: str = "sum",
    ) -> DataFrame:
        """
        Aggregate data to triangle cell level.
        
        Args:
            df: Input DataFrame
            origin_col: Origin period column
            dev_col: Development period column
            value_col: Value column to aggregate
            agg_type: Aggregation type ('sum', 'count', 'avg')
            
        Returns:
            DataFrame aggregated to triangle cell level
        """
        group_cols = [origin_col, dev_col]
        
        if agg_type == "sum":
            agg_df = df.groupBy(*group_cols).agg(
                spark_sum(col(value_col)).alias("triangle_value"),
                count("*").alias("claim_count"),
            )
        elif agg_type == "count":
            agg_df = df.groupBy(*group_cols).agg(
                count("*").alias("triangle_value"),
            )
        else:  # avg
            agg_df = df.groupBy(*group_cols).agg(
                avg(col(value_col)).alias("triangle_value"),
                count("*").alias("claim_count"),
            )
        
        return agg_df.orderBy(origin_col, dev_col)
    
    @staticmethod
    def filter_valid_development(
        df: DataFrame,
        max_dev_period: int = 10,
        dev_col: str = "development_period",
    ) -> DataFrame:
        """
        Filter to valid development periods.
        
        Args:
            df: Input DataFrame
            max_dev_period: Maximum development period to include
            dev_col: Development period column name
            
        Returns:
            Filtered DataFrame
        """
        return df.filter(
            (col(dev_col) >= 1) & (col(dev_col) <= max_dev_period)
        )








