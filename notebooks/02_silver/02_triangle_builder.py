# Databricks notebook source
# MAGIC %md
# MAGIC # 📐 Silver Layer: Triangle Builder
# MAGIC 
# MAGIC **Owner: Marcus (Data Engineer)**
# MAGIC 
# MAGIC This notebook transforms raw claims and payments into actuarial loss triangles.
# MAGIC The key transformation is pivoting linear data into the 2D matrix required for Chain Ladder analysis.
# MAGIC 
# MAGIC ## Actuarial Methodology
# MAGIC 
# MAGIC Based on Loss Data Analytics Chapter 11 (https://openacttexts.github.io/Loss-Data-Analytics/ChapLossReserves.html):
# MAGIC 
# MAGIC - **C_ij**: Cumulative paid amount up to development period j for all claims from origin year i
# MAGIC - **X_ij**: Incremental payments in development period j for all claims from origin year i
# MAGIC - **Development Period**: Uses 0-indexed convention (0 = year of occurrence, 1 = 1 year after, etc.)
# MAGIC - **Origin Year (i)**: The accident/occurrence year when the insured event happened
# MAGIC - **Observable Triangle**: Cells where i + j ≤ I (current year) are observed
# MAGIC - **Lower Triangle**: Cells where i + j > I must be predicted using Chain Ladder

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "unified_reserves"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------

# Set catalog
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✅ Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

from pyspark.sql.functions import (
    col, year, month, floor, months_between,
    sum as spark_sum, count, countDistinct, max as spark_max,
    coalesce, lit, current_timestamp, current_date, first
)
from pyspark.sql.window import Window
import uuid

# COMMAND ----------

# Load claims and payments from Bronze
claims_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_claims")
payments_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_payments")

print(f"Claims: {claims_df.count()} records")
print(f"Payments: {payments_df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Enriched Claims Dataset

# COMMAND ----------

# Get latest payment per claim
latest_payments = payments_df.groupBy("claim_id").agg(
    spark_max("payment_date").alias("latest_payment_date"),
    spark_sum("payment_amount").alias("total_paid"),
    spark_max("reserve_amount").alias("current_reserve"),
)

# Join claims with payment info
enriched_claims = claims_df.join(latest_payments, on="claim_id", how="left")

# Add origin period and calculated fields
enriched_claims = enriched_claims \
    .withColumn("origin_year", year(col("loss_date"))) \
    .withColumn("origin_period", year(col("loss_date"))) \
    .withColumn(
        "incurred_amount",
        coalesce(col("total_paid"), lit(0)) + coalesce(col("current_reserve"), lit(0))
    ) \
    .withColumn("_processed_at", current_timestamp())

display(enriched_claims.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Loss Triangles

# COMMAND ----------

# Join claims with payments for triangle building
joined_df = payments_df.join(
    claims_df.select("claim_id", "loss_date", "loss_type", "state", "county", "catastrophe_code"),
    on="claim_id",
    how="inner"
)

# Add origin and development periods
# Per actuarial convention (Loss Data Analytics Ch 11):
# - Development period 0 = payments in the year of occurrence
# - Development period j = payments made j years after occurrence
# Formula: j = floor(months_between(payment_date, loss_date) / 12)
triangle_df = joined_df \
    .withColumn("origin_year", year(col("loss_date"))) \
    .withColumn(
        "development_period",
        floor(months_between(col("payment_date"), col("loss_date")) / 12).cast("int")
    )

# Filter valid development periods (0-9, i.e., 10 years of development)
# Sanity check: payment_date must be >= loss_date
triangle_df = triangle_df.filter(
    (col("development_period") >= 0) & 
    (col("development_period") <= 9) &
    (col("payment_date") >= col("loss_date"))
)

display(triangle_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paid Loss Triangle (All Types)
# MAGIC 
# MAGIC Per Loss Data Analytics Chapter 11, we build:
# MAGIC - **Incremental Triangle (X_ij)**: Sum of payment_amount in each cell
# MAGIC - **Cumulative Triangle (C_ij)**: Running sum of X_ij over development periods
# MAGIC 
# MAGIC The cumulative value C_ij represents total paid up to and including development period j.

# COMMAND ----------

# Step 1: Calculate INCREMENTAL payments (X_ij) per cell
# This is the sum of all payment_amount values for each (origin_year, development_period)
incremental_df = triangle_df.groupBy("origin_year", "development_period").agg(
    spark_sum("payment_amount").alias("incremental_value"),  # X_ij
    countDistinct("claim_id").alias("claim_count"),
)

# Step 2: Calculate CUMULATIVE values (C_ij) as running sum of incremental
# C_ij = sum of X_i0 + X_i1 + ... + X_ij
# Using window function to calculate running sum within each origin_year
window_spec = Window.partitionBy("origin_year").orderBy("development_period").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cell_df = incremental_df \
    .withColumn("cumulative_value", spark_sum("incremental_value").over(window_spec))

# Pivot to triangle format (cumulative values for Chain Ladder)
paid_triangle = cell_df \
    .groupBy("origin_year") \
    .pivot("development_period") \
    .agg(first("cumulative_value")) \
    .orderBy("origin_year")

print("📐 Paid Loss Triangle (All Types):")
display(paid_triangle)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Water Damage Triangle

# COMMAND ----------

# Build triangle for Water Damage claims specifically
water_damage_df = triangle_df.filter(col("loss_type") == "Water Damage")

# Same methodology as all-types triangle: incremental first, then cumulative
water_incremental_df = water_damage_df \
    .groupBy("origin_year", "development_period") \
    .agg(
        spark_sum("payment_amount").alias("incremental_value"),
        countDistinct("claim_id").alias("claim_count")
    )

water_cell_df = water_incremental_df \
    .withColumn("cumulative_value", spark_sum("incremental_value").over(window_spec))

water_triangle = water_cell_df \
    .groupBy("origin_year") \
    .pivot("development_period") \
    .agg(first("cumulative_value")) \
    .orderBy("origin_year")

print("📐 Water Damage Triangle:")
display(water_triangle)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save to Silver Layer

# COMMAND ----------

# Function to convert wide triangle to long format
def triangle_to_long(wide_triangle_df, cell_df, triangle_type, loss_type=None):
    """
    Convert wide triangle to long format for storage.
    
    Per Loss Data Analytics Ch 11 notation:
    - origin_period (i): Accident/occurrence year
    - development_period (j): Development delay (0 = year of occurrence)
    - cumulative_value (C_ij): Total paid up to period j
    - incremental_value (X_ij): Amount paid in period j
    """
    # Get development period columns (all except origin_year)
    dev_cols = [c for c in wide_triangle_df.columns if c != "origin_year"]
    
    # Unpivot using stack
    stack_expr = ", ".join([f"'{c}', `{c}`" for c in dev_cols])
    
    long_df = wide_triangle_df.selectExpr(
        "origin_year as origin_period",
        f"stack({len(dev_cols)}, {stack_expr}) as (development_period, cumulative_value)"
    )
    
    # Join with cell_df to get incremental values and claim counts
    long_df = long_df \
        .withColumn("development_period", col("development_period").cast("int")) \
        .join(
            cell_df.select(
                col("origin_year").alias("origin_period"),
                "development_period",
                "incremental_value",
                "claim_count"
            ),
            on=["origin_period", "development_period"],
            how="left"
        )
    
    # Add metadata
    long_df = long_df \
        .withColumn("triangle_id", lit(str(uuid.uuid4()))) \
        .withColumn("triangle_type", lit(triangle_type)) \
        .withColumn("loss_type", lit(loss_type)) \
        .withColumn("state", lit(None).cast("string")) \
        .withColumn("county", lit(None).cast("string")) \
        .withColumn("period_type", lit("year")) \
        .withColumn("as_of_date", current_date()) \
        .withColumn("_created_at", current_timestamp())
    
    return long_df

# COMMAND ----------

# Convert and save paid triangle (all types)
# Pass both wide triangle and cell_df to include incremental values
paid_long = triangle_to_long(paid_triangle, cell_df, "paid", "ALL")

paid_long.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.loss_triangles")

print(f"✅ Saved paid triangle to {CATALOG}.{SILVER_SCHEMA}.loss_triangles")

# COMMAND ----------

# Save Water Damage triangle
water_long = triangle_to_long(water_triangle, water_cell_df, "paid", "Water Damage")

water_long.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.loss_triangles")

print(f"✅ Saved Water Damage triangle")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Saved Triangles

# COMMAND ----------

# Check saved triangles
saved_triangles = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.loss_triangles")
display(saved_triangles.groupBy("triangle_type", "loss_type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Triangle Building Complete!
# MAGIC 
# MAGIC Loss triangles have been created and saved to the Silver layer. Proceed to:
# MAGIC - **02_silver/03_risk_detection** - Run NLP risk analysis
# MAGIC - **03_gold/04_chain_ladder** - Calculate reserves
