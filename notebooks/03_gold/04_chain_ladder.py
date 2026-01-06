# Databricks notebook source
# MAGIC %md
# MAGIC # 📈 Gold Layer: Chain Ladder Reserving
# MAGIC 
# MAGIC **Owner: Sarah (Actuarial Data Scientist)**
# MAGIC 
# MAGIC This notebook applies the **Chain Ladder method** to calculate IBNR reserves.
# MAGIC 
# MAGIC ## Methodology Reference
# MAGIC 
# MAGIC The Chain Ladder method is a standard actuarial technique for loss reserving, with key academic references:
# MAGIC - **Mack (1993)**: Distribution-free calculation of standard error
# MAGIC - **Renshaw & Verrall (1998)**: Stochastic model underlying Chain Ladder
# MAGIC - **England & Verrall (2002)**: Comprehensive review of stochastic claims reserving
# MAGIC 
# MAGIC See: [Loss Data Analytics - Chapter 11](https://openacttexts.github.io/Loss-Data-Analytics/ChapLossReserves.html)
# MAGIC 
# MAGIC ## Key Formulas
# MAGIC 
# MAGIC **Age-to-Age Development Factor (Link Ratio):**
# MAGIC $$\hat{f}_j = \frac{\sum_{i=1}^{I-j} C_{i,j+1}}{\sum_{i=1}^{I-j} C_{i,j}}$$
# MAGIC 
# MAGIC **Cumulative Development Factor:**
# MAGIC $$\hat{F}_j = \prod_{k=j}^{J-1} \hat{f}_k$$
# MAGIC 
# MAGIC **Ultimate Loss Estimate:**
# MAGIC $$\hat{C}_{i,J} = C_{i,I-i+1} \cdot \hat{F}_{I-i+1}$$
# MAGIC 
# MAGIC **IBNR Reserve:**
# MAGIC $$\hat{R}_i = \hat{C}_{i,J} - C_{i,I-i+1}$$

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "unified_reserves"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND ----------

# Set catalog
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✅ Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chain Ladder Implementation
# MAGIC 
# MAGIC Based on the distribution-free Chain Ladder model (Mack, 1993).

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql.functions import col, sum as spark_sum, current_timestamp, current_date, lit

# COMMAND ----------

class ChainLadderModel:
    """
    Distribution-free Chain Ladder model for loss reserve estimation.
    
    Based on Mack (1993): "Distribution-Free Calculation of the Standard 
    Error of Chain Ladder Reserve Estimates"
    
    The Chain Ladder method assumes that losses develop in a predictable 
    pattern based on historical age-to-age (link ratio) factors.
    
    Key assumptions (Mack, 1993):
    1. E[C_{i,j+1} | C_{i,1},...,C_{i,j}] = C_{i,j} * f_j
    2. {C_{i,1},...,C_{i,J}} and {C_{k,1},...,C_{k,J}} are independent for i ≠ k
    3. Var(C_{i,j+1} | C_{i,1},...,C_{i,j}) = C_{i,j} * σ²_j
    
    References:
    - Mack, T. (1993). ASTIN Bulletin 23(2): 213-225
    - Taylor, G. (2000). Loss Reserving: An Actuarial Perspective
    - https://openacttexts.github.io/Loss-Data-Analytics/ChapLossReserves.html
    """
    
    def __init__(self, tail_factor=1.0):
        """
        Initialize Chain Ladder model.
        
        Args:
            tail_factor: Factor for development beyond observed periods (default 1.0 = no tail)
        """
        self.tail_factor = tail_factor
        self._triangle = None
        self._ata_factors = None  # Age-to-Age (Link Ratios)
        self._cdf = None          # Cumulative Development Factors
        self._ultimate = None
        self._ibnr = None
        self._sigma_sq = None     # Mack variance parameters
        self._se_reserve = None   # Standard errors (Mack)
        self._is_fitted = False
    
    def fit(self, triangle):
        """
        Fit the Chain Ladder model to a cumulative loss triangle.
        
        Args:
            triangle: numpy array of cumulative losses with shape (n_origins, n_development)
                     Rows = origin periods (accident years)
                     Columns = development periods
                     Upper triangle contains observed data, lower triangle is NaN
        
        Returns:
            self (fitted model)
        """
        self._triangle = np.array(triangle, dtype=float)
        
        # Step 1: Calculate Age-to-Age factors (Link Ratios) using volume-weighted average
        self._ata_factors = self._calculate_ata_factors()
        
        # Step 2: Calculate Cumulative Development Factors
        self._cdf = self._calculate_cdf()
        
        # Step 3: Project Ultimate Losses
        self._ultimate = self._project_ultimate()
        
        # Step 4: Calculate IBNR Reserves
        self._ibnr = self._calculate_ibnr()
        
        # Step 5: Calculate Mack Standard Errors (optional)
        self._sigma_sq = self._calculate_sigma_squared()
        self._se_reserve = self._calculate_mack_se()
        
        self._is_fitted = True
        return self
    
    def _calculate_ata_factors(self):
        """
        Calculate Age-to-Age (Link Ratio) development factors.
        
        Uses volume-weighted average across all origin periods:
        f_j = Σ C_{i,j+1} / Σ C_{i,j}
        
        This is the standard Chain Ladder estimator.
        """
        triangle = self._triangle
        n_rows, n_cols = triangle.shape
        ata = np.zeros(n_cols - 1)
        
        for j in range(n_cols - 1):
            col_current = triangle[:, j]
            col_next = triangle[:, j + 1]
            
            # Only use pairs where both values exist (upper triangle)
            valid = ~np.isnan(col_current) & ~np.isnan(col_next) & (col_current > 0)
            
            if np.sum(valid) > 0:
                # Volume-weighted average (standard Chain Ladder)
                ata[j] = np.sum(col_next[valid]) / np.sum(col_current[valid])
            else:
                ata[j] = 1.0  # No development if no data
        
        return ata
    
    def _calculate_cdf(self):
        """
        Calculate Cumulative Development Factors to Ultimate.
        
        F_j = f_j * f_{j+1} * ... * f_{J-1} * tail_factor
        
        The CDF represents the total expected development from age j to ultimate.
        """
        ata = self._ata_factors
        n = len(ata)
        cdf = np.zeros(n + 1)
        
        # CDF at ultimate age is the tail factor
        cdf[-1] = self.tail_factor
        
        # Work backwards: CDF_j = ATA_j * CDF_{j+1}
        for j in range(n - 1, -1, -1):
            cdf[j] = ata[j] * cdf[j + 1]
        
        return cdf
    
    def _project_ultimate(self):
        """
        Project ultimate losses for each origin period.
        
        Ultimate_i = Latest_Cumulative_i × CDF_at_current_age
        """
        triangle = self._triangle
        cdf = self._cdf
        n_rows = triangle.shape[0]
        ultimate = np.zeros(n_rows)
        
        for i in range(n_rows):
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            
            if len(valid_idx) > 0:
                latest_idx = valid_idx[-1]  # Current development age
                latest_value = row[latest_idx]
                ultimate[i] = latest_value * cdf[latest_idx]
            else:
                ultimate[i] = 0
        
        return ultimate
    
    def _calculate_ibnr(self):
        """
        Calculate IBNR (Incurred But Not Reported) reserves.
        
        IBNR_i = Ultimate_i - Latest_Cumulative_i
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
                ibnr[i] = max(0, ultimate[i] - latest_value)  # IBNR cannot be negative
            else:
                ibnr[i] = ultimate[i]
        
        return ibnr
    
    def _calculate_sigma_squared(self):
        """
        Calculate variance parameters σ²_j for Mack standard errors.
        
        σ²_j = (1/(n-j-1)) * Σ C_{i,j} * (C_{i,j+1}/C_{i,j} - f_j)²
        
        Based on Mack (1993) equation for unbiased estimation.
        """
        triangle = self._triangle
        ata = self._ata_factors
        n_rows, n_cols = triangle.shape
        sigma_sq = np.zeros(n_cols - 1)
        
        for j in range(n_cols - 1):
            col_current = triangle[:, j]
            col_next = triangle[:, j + 1]
            valid = ~np.isnan(col_current) & ~np.isnan(col_next) & (col_current > 0)
            n_valid = np.sum(valid)
            
            if n_valid > 1:
                individual_ratios = col_next[valid] / col_current[valid]
                weighted_sum = np.sum(col_current[valid] * (individual_ratios - ata[j])**2)
                sigma_sq[j] = weighted_sum / (n_valid - 1)
            else:
                sigma_sq[j] = 0
        
        return sigma_sq
    
    def _calculate_mack_se(self):
        """
        Calculate Mack standard errors for reserve estimates.
        
        This provides a measure of uncertainty in the reserve estimates
        based on Mack (1993) formula.
        """
        triangle = self._triangle
        cdf = self._cdf
        sigma_sq = self._sigma_sq
        ultimate = self._ultimate
        n_rows, n_cols = triangle.shape
        se = np.zeros(n_rows)
        
        for i in range(n_rows):
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            
            if len(valid_idx) > 0:
                latest_idx = valid_idx[-1]
                latest_value = row[latest_idx]
                
                # Sum of variance contributions from each future development period
                var_sum = 0
                for j in range(latest_idx, n_cols - 1):
                    if j < len(sigma_sq) and j < len(self._ata_factors):
                        # Simplified Mack SE formula
                        col_sum = np.nansum(triangle[:, j])
                        if col_sum > 0 and self._ata_factors[j] > 0:
                            var_sum += (sigma_sq[j] / self._ata_factors[j]**2) * (
                                1/latest_value + 1/col_sum
                            )
                
                se[i] = ultimate[i] * np.sqrt(max(0, var_sum))
            else:
                se[i] = 0
        
        return se
    
    def get_results(self, origin_years):
        """
        Get reserve estimation results as a DataFrame.
        
        Args:
            origin_years: List of origin year labels
            
        Returns:
            pandas DataFrame with columns:
            - origin_period: Origin year
            - latest_cumulative: Most recent cumulative loss
            - ultimate_loss: Projected ultimate loss
            - ibnr: IBNR reserve
            - percent_reported: Percentage of ultimate already reported
            - se_ibnr: Mack standard error of IBNR (if calculated)
            - cv: Coefficient of variation (SE/IBNR)
        """
        triangle = self._triangle
        n_rows = triangle.shape[0]
        
        latest = []
        dev_age = []
        for i in range(n_rows):
            row = triangle[i, :]
            valid_idx = np.where(~np.isnan(row))[0]
            latest.append(row[valid_idx[-1]] if len(valid_idx) > 0 else 0)
            dev_age.append(len(valid_idx))
        
        results = pd.DataFrame({
            "origin_period": origin_years,
            "development_age": dev_age,
            "latest_cumulative": latest,
            "ultimate_loss": self._ultimate,
            "ibnr": self._ibnr,
            "percent_reported": [l / u if u > 0 else 1.0 for l, u in zip(latest, self._ultimate)],
            "se_ibnr": self._se_reserve,
            "cv": [se / ibnr if ibnr > 0 else 0 for se, ibnr in zip(self._se_reserve, self._ibnr)],
        })
        
        return results
    
    def get_total_ibnr(self):
        """Get total IBNR across all origin periods."""
        return float(np.sum(self._ibnr))
    
    def get_total_ultimate(self):
        """Get total ultimate loss across all origin periods."""
        return float(np.sum(self._ultimate))
    
    def get_total_se(self):
        """Get total standard error (simplified - not accounting for correlations)."""
        # Note: True total SE requires accounting for parameter uncertainty correlation
        # This is a simplified version
        return float(np.sqrt(np.sum(self._se_reserve**2)))
    
    def get_ata_factors(self):
        """Get Age-to-Age (Link Ratio) factors."""
        return self._ata_factors
    
    def get_cdf(self):
        """Get Cumulative Development Factors."""
        return self._cdf
    
    def get_development_table(self):
        """
        Get development factor table showing ATA and CDF by development period.
        """
        n = len(self._ata_factors)
        return pd.DataFrame({
            "development_period": range(1, n + 1),
            "ata_factor": self._ata_factors,
            "cdf_to_ultimate": self._cdf[:-1],
            "percent_developed": [1/cdf * 100 for cdf in self._cdf[:-1]],
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Triangle Data

# COMMAND ----------

# Load triangles from Silver layer
triangles_df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.loss_triangles")

# Get paid triangle for all loss types
paid_triangle_df = triangles_df \
    .filter((col("triangle_type") == "paid") & (col("loss_type") == "ALL")) \
    .groupBy("origin_period") \
    .pivot("development_period") \
    .agg(spark_sum("cumulative_value")) \
    .orderBy("origin_period")

print("📐 Paid Loss Triangle:")
display(paid_triangle_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit Chain Ladder Model

# COMMAND ----------

# Convert to pandas/numpy for Chain Ladder
triangle_pd = paid_triangle_df.toPandas()
origin_years = triangle_pd["origin_period"].tolist()
triangle_pd = triangle_pd.set_index("origin_period")
triangle_np = triangle_pd.values

print(f"Triangle shape: {triangle_np.shape}")
print(f"Origin years: {origin_years}")
print(f"\nTriangle values:\n{triangle_np}")

# COMMAND ----------

# Fit Chain Ladder model
model = ChainLadderModel(tail_factor=1.0)  # No tail factor for this example
model.fit(triangle_np)

print("✅ Chain Ladder model fitted successfully")
print(f"\nTotal observations in triangle: {np.sum(~np.isnan(triangle_np))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Development Factors
# MAGIC 
# MAGIC The Age-to-Age (ATA) factors show how losses develop from one period to the next.
# MAGIC The Cumulative Development Factor (CDF) shows the total expected development to ultimate.

# COMMAND ----------

# Development factor table
dev_table = model.get_development_table()
print("📊 Development Factor Analysis:")
display(spark.createDataFrame(dev_table))

# COMMAND ----------

# Visual interpretation
print("\n📈 Development Pattern Interpretation:")
for _, row in dev_table.iterrows():
    period = int(row['development_period'])
    ata = row['ata_factor']
    pct = row['percent_developed']
    print(f"  Period {period} → {period+1}: ATA = {ata:.4f} | {pct:.1f}% developed at age {period}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reserve Estimates
# MAGIC 
# MAGIC **IBNR = Ultimate Loss - Cumulative Paid Loss**
# MAGIC 
# MAGIC The standard error provides a measure of uncertainty based on Mack (1993).

# COMMAND ----------

# Get full results with Mack standard errors
results_df = model.get_results(origin_years)
print("📊 Reserve Estimates by Origin Year (with Mack Standard Errors):")
display(spark.createDataFrame(results_df))

# COMMAND ----------

# Summary totals
total_ibnr = model.get_total_ibnr()
total_ultimate = model.get_total_ultimate()
total_se = model.get_total_se()

print(f"""
📊 CHAIN LADDER RESERVE SUMMARY
================================

Total Paid to Date:  ${results_df['latest_cumulative'].sum():,.0f}
Total Ultimate Loss: ${total_ultimate:,.0f}
Total IBNR Reserve:  ${total_ibnr:,.0f}

Standard Error:      ${total_se:,.0f}
Coefficient of Var:  {total_se/total_ibnr*100 if total_ibnr > 0 else 0:.1f}%

95% Confidence Interval for IBNR:
  Lower: ${max(0, total_ibnr - 1.96*total_se):,.0f}
  Upper: ${total_ibnr + 1.96*total_se:,.0f}

Reference: Mack, T. (1993). ASTIN Bulletin 23(2): 213-225
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI-Adjusted Reserves
# MAGIC 
# MAGIC The AI risk detection model identified claims with hidden risks (mold, litigation).
# MAGIC We apply an adjustment factor based on the proportion of high-risk exposure.

# COMMAND ----------

# Load risk summary to calculate AI adjustment
risk_summary = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.risk_summary")

# Calculate high-risk exposure ratio
high_risk_stats = risk_summary.agg(
    spark_sum("high_risk_incurred").alias("total_high_risk"),
    spark_sum("total_incurred").alias("total_all")
).collect()[0]

high_risk_ratio = high_risk_stats["total_high_risk"] / high_risk_stats["total_all"] if high_risk_stats["total_all"] else 0

# AI adjustment factor: high-risk claims tend to develop more adversely
# Based on actuarial judgment that high-risk claims develop 25-50% more
AI_ADJUSTMENT_FACTOR = 1.0 + (high_risk_ratio * 0.50)  # 50% additional development for high-risk

print(f"High Risk Exposure Ratio: {high_risk_ratio:.2%}")
print(f"AI Adjustment Factor: {AI_ADJUSTMENT_FACTOR:.4f}")

# COMMAND ----------

# Apply AI adjustment
results_df["ai_adjusted_ultimate"] = results_df["ultimate_loss"] * AI_ADJUSTMENT_FACTOR
results_df["ai_adjusted_ibnr"] = results_df["ai_adjusted_ultimate"] - results_df["latest_cumulative"]
results_df["ibnr_difference"] = results_df["ai_adjusted_ibnr"] - results_df["ibnr"]

print("📊 Standard vs AI-Adjusted Reserves:")
display(spark.createDataFrame(results_df[[
    "origin_period", "latest_cumulative", "ultimate_loss", "ibnr", 
    "ai_adjusted_ultimate", "ai_adjusted_ibnr", "ibnr_difference"
]]))

# COMMAND ----------

# Comparison summary
standard_ibnr = results_df["ibnr"].sum()
adjusted_ibnr = results_df["ai_adjusted_ibnr"].sum()
difference = adjusted_ibnr - standard_ibnr

print(f"""
📊 STANDARD VS AI-ADJUSTED COMPARISON
=====================================

Standard Chain Ladder IBNR:  ${standard_ibnr:,.0f}
AI-Adjusted IBNR:            ${adjusted_ibnr:,.0f}
Additional Reserve Needed:   ${difference:,.0f} (+{difference/standard_ibnr*100 if standard_ibnr > 0 else 0:.1f}%)

⚠️  The AI model detected hidden risks (mold, litigation) in {high_risk_ratio:.1%}
    of incurred exposure. Historical patterns suggest these claims develop
    more adversely than standard claims.

    Recommendation: Consider holding the AI-adjusted reserve amount
    (${adjusted_ibnr:,.0f}) to account for adverse development risk.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to Gold Layer

# COMMAND ----------

# Prepare results for saving
save_df = results_df.copy()
save_df["triangle_type"] = "paid"
save_df["loss_type"] = "ALL"
save_df["segment"] = None
save_df["tail_factor"] = 1.0

results_spark = spark.createDataFrame(save_df[[
    "origin_period", "latest_cumulative", "ultimate_loss", "ibnr", 
    "percent_reported", "triangle_type", "loss_type", "segment", "tail_factor"
]])

results_spark = results_spark.withColumn("_calculated_at", current_timestamp())

# Write to Gold layer
results_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.reserve_estimates")

print(f"✅ Saved reserve estimates to {CATALOG}.{GOLD_SCHEMA}.reserve_estimates")

# COMMAND ----------

# Save summary
from pyspark.sql import Row
from datetime import date

summary_data = [Row(
    as_of_date=date.today(),
    triangle_type="paid",
    loss_type="ALL",
    total_paid=float(results_df["latest_cumulative"].sum()),
    total_ibnr=float(standard_ibnr),
    total_ultimate=float(total_ultimate),
    held_reserves=float(adjusted_ibnr),  # Hold the AI-adjusted amount
    reserve_adequacy=float(adjusted_ibnr / standard_ibnr) if standard_ibnr > 0 else 1.0,
    reserve_margin=float(adjusted_ibnr - standard_ibnr),
    ai_adjustment_factor=float(AI_ADJUSTMENT_FACTOR),
)]

summary_df = spark.createDataFrame(summary_data)
summary_df = summary_df.withColumn("_calculated_at", current_timestamp())

summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.reserve_summary")

print(f"✅ Saved reserve summary to {CATALOG}.{GOLD_SCHEMA}.reserve_summary")

# COMMAND ----------

# Save development factors
dev_factors_data = []
ata_factors = model.get_ata_factors()
cdf_factors = model.get_cdf()

for i in range(len(ata_factors)):
    dev_factors_data.append(Row(
        triangle_type="paid",
        loss_type="ALL",
        development_period=i+1,
        ata_factor=float(ata_factors[i]),
        cdf_to_ultimate=float(cdf_factors[i]),
    ))

dev_factors_df = spark.createDataFrame(dev_factors_data)
dev_factors_df = dev_factors_df.withColumn("_calculated_at", current_timestamp())

dev_factors_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.development_factors")

print(f"✅ Saved development factors to {CATALOG}.{GOLD_SCHEMA}.development_factors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## References
# MAGIC 
# MAGIC 1. **Mack, T. (1993)**. "Distribution-Free Calculation of the Standard Error of Chain Ladder Reserve Estimates." *ASTIN Bulletin* 23(2): 213-225.
# MAGIC 
# MAGIC 2. **Renshaw, A. and Verrall, R. (1998)**. "A Stochastic Model Underlying the Chain-Ladder Technique." *British Actuarial Journal* 4/4: 903-923.
# MAGIC 
# MAGIC 3. **England, P. and Verrall, R. (2002)**. "Stochastic Claims Reserving in General Insurance." *British Actuarial Journal* 8/3: 443-518.
# MAGIC 
# MAGIC 4. **Taylor, G. (2000)**. *Loss Reserving: An Actuarial Perspective*. Kluwer Academic Publishers.
# MAGIC 
# MAGIC 5. **Wüthrich, M. and Merz, M. (2008)**. *Stochastic Claims Reserving Methods in Insurance*. Wiley Finance.
# MAGIC 
# MAGIC Online Resource: [Loss Data Analytics - Chapter 11](https://openacttexts.github.io/Loss-Data-Analytics/ChapLossReserves.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Chain Ladder Reserving Complete!
# MAGIC 
# MAGIC Reserves have been calculated using the standard actuarial Chain Ladder method with:
# MAGIC - Volume-weighted Age-to-Age factors
# MAGIC - Mack (1993) standard errors for uncertainty quantification
# MAGIC - AI-adjusted reserves for hidden risk exposure
# MAGIC 
# MAGIC Proceed to: **04_analytics/05_genie_setup** - Configure AI/BI Genie
