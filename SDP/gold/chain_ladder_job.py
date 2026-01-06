# Databricks notebook source
# =============================================================================
# Chain Ladder Reserve Calculations - Multi-Segment
# =============================================================================
#
# This notebook calculates IBNR (Incurred But Not Reported) reserves using 
# the Chain Ladder method - the most widely used actuarial technique for 
# estimating outstanding claim liabilities.
#
# UPDATED: Now calculates reserves for ALL aggregate AND each individual 
# loss type (Fire, Water Damage, Wind, Theft, Liability) separately.
#
# Why a Separate Notebook?
# Delta Live Tables (DLT) doesn't support .collect() or .toPandas() which 
# are required for matrix operations. This notebook runs AFTER the DLT 
# pipeline completes.
#
# Output Tables:
#   - gold_reserve_estimates: IBNR reserves by origin year AND loss type
#   - gold_development_factors_python: ATA and CDF factors by loss type
# =============================================================================

# COMMAND ----------

# Configuration
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col
from datetime import date

spark = SparkSession.builder.getOrCreate()

# IMPORTANT: Must match DLT pipeline target catalog and schema
# The DLT pipeline publishes to unified_reserves.losstriangle
CATALOG = "unified_reserves"
SCHEMA = "losstriangle"        # DLT pipeline target schema
SILVER_SCHEMA = "losstriangle"  # Same schema for all tables

print(f"Target location: {CATALOG}.{SCHEMA}")
print(f"Source location: {CATALOG}.{SILVER_SCHEMA}")

# COMMAND ----------

# =============================================================================
# Chain Ladder Model Implementation
# =============================================================================

class ChainLadderModel:
    """
    Distribution-free Chain Ladder model for loss reserve estimation.
    Based on Mack (1993) methodology.
    
    The Chain Ladder method projects ultimate losses using historical 
    development patterns observed in a loss triangle.
    
    Key Steps:
    1. Calculate Age-to-Age (ATA) factors: ratio of cumulative losses 
       from one development period to the next
    2. Calculate Cumulative Development Factors (CDF): product of all 
       remaining ATA factors
    3. Project Ultimate = Latest Cumulative × CDF
    4. IBNR = Ultimate - Latest Cumulative
    """
    
    def __init__(self, tail_factor=1.0):
        self.tail_factor = tail_factor
        self._triangle = None
        self._ata_factors = None
        self._cdf = None
        self._ultimate = None
        self._ibnr = None
        self._is_fitted = False
    
    def fit(self, triangle):
        """Fit the Chain Ladder model to a cumulative loss triangle."""
        self._triangle = np.array(triangle, dtype=float)
        self._ata_factors = self._calculate_ata_factors()
        self._cdf = self._calculate_cdf()
        self._ultimate = self._project_ultimate()
        self._ibnr = self._calculate_ibnr()
        self._is_fitted = True
        return self
    
    def _calculate_ata_factors(self):
        """
        Calculate volume-weighted Age-to-Age (ATA) factors.
        Formula: ATA_j = Σ C_{i,j+1} / Σ C_{i,j}
        """
        triangle = self._triangle
        n_rows, n_cols = triangle.shape
        ata = np.zeros(n_cols - 1)
        
        for j in range(n_cols - 1):
            col_current = triangle[:, j]
            col_next = triangle[:, j + 1]
            valid = ~np.isnan(col_current) & ~np.isnan(col_next) & (col_current > 0)
            
            if np.sum(valid) > 0:
                ata[j] = np.sum(col_next[valid]) / np.sum(col_current[valid])
            else:
                ata[j] = 1.0
        return ata
    
    def _calculate_cdf(self):
        """
        Calculate Cumulative Development Factors (CDF).
        CDF_j = ATA_j × ATA_{j+1} × ... × ATA_{n-1} × tail_factor
        """
        ata = self._ata_factors
        n = len(ata)
        cdf = np.zeros(n + 1)
        cdf[-1] = self.tail_factor
        
        for j in range(n - 1, -1, -1):
            cdf[j] = ata[j] * cdf[j + 1]
        return cdf
    
    def _project_ultimate(self):
        """Project ultimate losses: Ultimate_i = Latest_i × CDF"""
        triangle = self._triangle
        cdf = self._cdf
        n_rows = triangle.shape[0]
        ultimate = np.zeros(n_rows)
        
        for i in range(n_rows):
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            if len(valid_idx) > 0:
                latest_idx = valid_idx[-1]
                latest_value = row[latest_idx]
                ultimate[i] = latest_value * cdf[latest_idx]
        return ultimate
    
    def _calculate_ibnr(self):
        """Calculate IBNR: IBNR_i = Ultimate_i - Latest_i"""
        triangle = self._triangle
        ultimate = self._ultimate
        n_rows = triangle.shape[0]
        ibnr = np.zeros(n_rows)
        
        for i in range(n_rows):
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            if len(valid_idx) > 0:
                latest_value = row[valid_idx[-1]]
                ibnr[i] = max(0, ultimate[i] - latest_value)
        return ibnr
    
    def get_latest_values(self):
        """Get the latest cumulative values from the triangle diagonal."""
        triangle = self._triangle
        n_rows = triangle.shape[0]
        latest = []
        for i in range(n_rows):
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            latest.append(row[valid_idx[-1]] if len(valid_idx) > 0 else 0)
        return np.array(latest)

# COMMAND ----------

# =============================================================================
# Helper Function: Calculate Reserves for a Single Loss Type
# =============================================================================

def calculate_reserves_for_loss_type(triangle_df, loss_type_name, tail_factor=1.0):
    """
    Calculate Chain Ladder reserves for a given triangle DataFrame.
    
    Args:
        triangle_df: pandas DataFrame with origin_period and development columns
        loss_type_name: Name of the loss type (e.g., "ALL", "Fire", "Water Damage")
        tail_factor: Tail factor for beyond-observed development
        
    Returns:
        Tuple of (reserve_results_list, factor_results_list)
    """
    if triangle_df.empty:
        print(f"  ⚠ No data for {loss_type_name}")
        return [], []
    
    # Extract origin years and triangle values
    origin_years = triangle_df["origin_period"].tolist()
    
    # Get development period columns (exclude origin_period and any metadata)
    dev_cols = [c for c in triangle_df.columns if c not in ['origin_period', 'loss_type']]
    triangle_values = triangle_df[dev_cols].astype(float).values
    
    # Handle sparse data: if insufficient development periods, use simple approach
    if triangle_values.shape[1] < 2:
        print(f"  ⚠ Limited data for {loss_type_name} - using simple estimation (no IBNR)")
        # For sparse data, assume 100% developed (no IBNR)
        reserve_results = []
        for i, origin in enumerate(origin_years):
            # Get the latest non-NaN value for this origin year
            row = triangle_values[i, :]
            valid_values = row[~np.isnan(row)]
            latest_value = float(valid_values[-1]) if len(valid_values) > 0 else 0.0
            
            reserve_results.append({
                "origin_period": int(origin),
                "latest_cumulative": latest_value,
                "ultimate_loss": latest_value,  # Assume fully developed
                "ibnr": 0.0,  # No IBNR for sparse data
                "percent_reported": 1.0,
                "triangle_type": "paid",
                "loss_type": loss_type_name,
                "tail_factor": tail_factor,
            })
        
        total_paid = sum(r["latest_cumulative"] for r in reserve_results)
        print(f"  ✓ {loss_type_name}: Paid=${total_paid:,.0f}, Ultimate=${total_paid:,.0f}, IBNR=$0 (sparse data)")
        return reserve_results, []
    
    # Fit Chain Ladder model
    model = ChainLadderModel(tail_factor=tail_factor)
    model.fit(triangle_values)
    
    # Get results
    latest = model.get_latest_values()
    ultimate = model._ultimate
    ibnr = model._ibnr
    pct_reported = [l / u if u > 0 else 1.0 for l, u in zip(latest, ultimate)]
    
    # Create reserve estimates
    reserve_results = []
    for i, origin in enumerate(origin_years):
        reserve_results.append({
            "origin_period": int(origin),
            "latest_cumulative": float(latest[i]),
            "ultimate_loss": float(ultimate[i]),
            "ibnr": float(ibnr[i]),
            "percent_reported": float(pct_reported[i]),
            "triangle_type": "paid",
            "loss_type": loss_type_name,
            "tail_factor": tail_factor,
        })
    
    # Create development factors
    ata = model._ata_factors
    cdf = model._cdf
    factor_results = []
    for i in range(len(ata)):
        factor_results.append({
            "development_period": i + 1,
            "ata_factor": float(ata[i]),
            "cdf_to_ultimate": float(cdf[i]),
            "percent_developed": float(1.0 / cdf[i] * 100) if cdf[i] > 0 else 100.0,
            "triangle_type": "paid",
            "loss_type": loss_type_name,
        })
    
    # Print summary
    total_ibnr = sum(ibnr)
    total_ultimate = sum(ultimate)
    total_paid = sum(latest)
    print(f"  ✓ {loss_type_name}: Paid=${total_paid:,.0f}, Ultimate=${total_ultimate:,.0f}, IBNR=${total_ibnr:,.0f}")
    
    return reserve_results, factor_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Calculate Reserves for Each Loss Type

# COMMAND ----------

# Load per-loss-type triangles from silver_triangle_by_loss_type
all_results = []
all_factors = []

source_table_by_type = f"{CATALOG}.{SILVER_SCHEMA}.silver_triangle_by_loss_type"
print(f"Loading per-loss-type triangles from: {source_table_by_type}")

try:
    # Get all unique loss types
    loss_types_df = spark.read.table(source_table_by_type)
    unique_loss_types = [row.loss_type for row in loss_types_df.select("loss_type").distinct().collect()]
    print(f"  Found loss types: {unique_loss_types}")
    
    # Process each loss type
    for loss_type in unique_loss_types:
        print(f"\nProcessing: {loss_type}")
        
        # Filter and pivot for this loss type
        type_data = loss_types_df.filter(col("loss_type") == loss_type)
        
        # Pivot to wide format
        triangle_pivot = (
            type_data
            .groupBy("origin_period")
            .pivot("development_period")
            .agg({"cumulative_value": "sum"})
            .orderBy("origin_period")
        ).toPandas()
        
        # Calculate reserves for this loss type
        reserves, factors = calculate_reserves_for_loss_type(triangle_pivot, loss_type)
        all_results.extend(reserves)
        all_factors.extend(factors)
        
except Exception as e:
    print(f"  ⚠ Error processing per-loss-type triangles: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Calculate "ALL" as Sum of Individual Loss Types
# MAGIC 
# MAGIC Instead of using a separate aggregation (silver_triangle_pivot), we calculate "ALL" 
# MAGIC as the true grand total of all individual loss types to ensure consistency.

# COMMAND ----------

# Create "ALL" loss type by summing individual loss type results
print("\nCalculating ALL as sum of individual loss types...")

if all_results:
    # Convert to DataFrame for aggregation
    results_pdf = pd.DataFrame(all_results)
    
    # Group by origin_period and sum the values across all loss types
    all_aggregated = results_pdf.groupby("origin_period").agg({
        "latest_cumulative": "sum",
        "ultimate_loss": "sum",
        "ibnr": "sum"
    }).reset_index()
    
    # Calculate percent_reported for aggregated data
    all_aggregated["percent_reported"] = all_aggregated["latest_cumulative"] / all_aggregated["ultimate_loss"]
    all_aggregated["percent_reported"] = all_aggregated["percent_reported"].fillna(1.0)
    
    # Create ALL reserve results
    for _, row in all_aggregated.iterrows():
        all_results.append({
            "origin_period": int(row["origin_period"]),
            "latest_cumulative": float(row["latest_cumulative"]),
            "ultimate_loss": float(row["ultimate_loss"]),
            "ibnr": float(row["ibnr"]),
            "percent_reported": float(row["percent_reported"]),
            "triangle_type": "paid",
            "loss_type": "ALL",
            "tail_factor": 1.0,
        })
    
    # Summary for ALL
    total_paid = all_aggregated["latest_cumulative"].sum()
    total_ultimate = all_aggregated["ultimate_loss"].sum()
    total_ibnr = all_aggregated["ibnr"].sum()
    print(f"  ✓ ALL (aggregated): Paid=${total_paid:,.0f}, Ultimate=${total_ultimate:,.0f}, IBNR=${total_ibnr:,.0f}")
    print(f"  ✓ ALL has {len(all_aggregated)} origin years")
else:
    print("  ⚠ No individual loss type results to aggregate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write Reserve Estimates to Gold Layer

# COMMAND ----------

if all_results:
    # Create combined DataFrame
    result_pdf = pd.DataFrame(all_results)
    result_df = spark.createDataFrame(result_pdf)
    result_df = result_df.withColumn("_calculated_at", current_timestamp())
    
    # Write to gold table
    target_table = f"{CATALOG}.{SCHEMA}.gold_reserve_estimates"
    
    # Drop existing table if it exists
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    
    # Write as Delta table
    result_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    
    print(f"\n✓ Written {len(all_results)} reserve estimate rows to: {target_table}")
    
    # Summary by loss type
    print("\n=== RESERVE SUMMARY BY LOSS TYPE ===")
    summary = result_pdf.groupby("loss_type").agg({
        "latest_cumulative": "sum",
        "ultimate_loss": "sum", 
        "ibnr": "sum"
    }).round(0)
    print(summary.to_string())
    
    # Verify ALL = sum of parts
    all_ibnr = result_pdf[result_pdf["loss_type"] == "ALL"]["ibnr"].sum()
    parts_ibnr = result_pdf[result_pdf["loss_type"] != "ALL"]["ibnr"].sum()
    print(f"\n--- Consistency Check ---")
    print(f"ALL IBNR:           ${all_ibnr:,.0f}")
    print(f"Sum of Parts IBNR:  ${parts_ibnr:,.0f}")
    print(f"Difference:         ${all_ibnr - parts_ibnr:,.0f}")
else:
    print("⚠ No reserve estimates calculated!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write Development Factors to Gold Layer

# COMMAND ----------

if all_factors:
    # Create combined DataFrame
    factors_pdf = pd.DataFrame(all_factors)
    factors_df = spark.createDataFrame(factors_pdf)
    factors_df = factors_df.withColumn("_calculated_at", current_timestamp())
    
    # Write to gold table
    target_table = f"{CATALOG}.{SCHEMA}.gold_development_factors_python"
    
    # Drop existing table if it exists
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    
    # Write as Delta table
    factors_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    
    print(f"✓ Written {len(all_factors)} development factor rows to: {target_table}")
else:
    print("⚠ No development factors calculated!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Results

# COMMAND ----------

print("=== gold_reserve_estimates (by loss type) ===")
display(spark.sql(f"""
    SELECT 
        loss_type,
        COUNT(*) as origin_years,
        ROUND(SUM(latest_cumulative), 0) as total_paid,
        ROUND(SUM(ultimate_loss), 0) as total_ultimate,
        ROUND(SUM(ibnr), 0) as total_ibnr
    FROM {CATALOG}.{SCHEMA}.gold_reserve_estimates
    GROUP BY loss_type
    ORDER BY loss_type
"""))

# COMMAND ----------

print("=== Detailed Reserve Estimates ===")
display(spark.read.table(f"{CATALOG}.{SCHEMA}.gold_reserve_estimates").orderBy("loss_type", "origin_period"))

# COMMAND ----------

print("=== Development Factors by Loss Type ===")
display(spark.sql(f"""
    SELECT 
        loss_type,
        development_period,
        ROUND(ata_factor, 4) as ata_factor,
        ROUND(cdf_to_ultimate, 4) as cdf_to_ultimate,
        ROUND(percent_developed, 1) as percent_developed
    FROM {CATALOG}.{SCHEMA}.gold_development_factors_python
    ORDER BY loss_type, development_period
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC After running this notebook:
# MAGIC 
# MAGIC 1. The `gold_reserve_estimates` table now contains reserves for:
# MAGIC    - `ALL` - Aggregate across all loss types
# MAGIC    - `Fire` - Fire-related claims
# MAGIC    - `Water Damage` - Water damage claims
# MAGIC    - `Wind` - Wind/storm claims
# MAGIC    - `Theft` - Theft claims
# MAGIC    - `Liability` - Liability claims
# MAGIC 
# MAGIC 2. The Streamlit app will now show **real data** for all loss types
# MAGIC 
# MAGIC 3. Re-run the DLT pipeline to update downstream views like `gold_v_ibnr_by_origin`
# MAGIC 
# MAGIC **Recommended Workflow:**
# MAGIC ```
# MAGIC Task 1: DLT Pipeline      → Creates bronze, silver tables
# MAGIC    ↓
# MAGIC Task 2: This Notebook     → Creates gold_reserve_estimates (ALL + per-type)
# MAGIC    ↓
# MAGIC Task 3: DLT Pipeline      → Updates gold views
# MAGIC ```


