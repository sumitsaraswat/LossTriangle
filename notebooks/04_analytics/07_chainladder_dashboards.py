# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Chainladder-Inspired Dashboard Tiles
# MAGIC 
# MAGIC **Purpose:** SQL queries for actuarial visualizations inspired by [chainladder-python gallery](https://chainladder-python.readthedocs.io/en/latest/gallery/index.html)
# MAGIC 
# MAGIC ## Dashboard Tiles Available
# MAGIC 
# MAGIC | # | Gallery Example | Visualization Type | Difficulty |
# MAGIC |---|-----------------|-------------------|------------|
# MAGIC | 1 | Loss Development Curves | Multi-line Chart | Easy |
# MAGIC | 2 | Actual vs Expected | Scatter Plot | Medium |
# MAGIC | 3 | IBNR Runoff Projection | Stacked Area Chart | Medium |
# MAGIC | 4 | ATA Factor Diagnostics | Box Plot / Range Chart | Easy |
# MAGIC | 5 | Clark Growth Curve | Line Chart (Fitted vs Actual) | Medium |
# MAGIC | 6 | BF vs Chain Ladder | Grouped Bar Chart | Easy |
# MAGIC | 7 | Value at Risk / Variability | Error Bars / Tornado | Medium |
# MAGIC | 8 | Calendar Year Development | Line Chart | Easy |
# MAGIC | 9 | Tail Factor Sensitivity | Tornado Chart | Easy |
# MAGIC | 10 | Development Exhibit | Formatted Table | Easy |
# MAGIC 
# MAGIC ---

# COMMAND ----------

# Configuration
CATALOG = "unified_reserves"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📈 TILE 1: Loss Development Curves
# MAGIC **Gallery:** [Loss Development Patterns](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_development.html)
# MAGIC 
# MAGIC **Visualization:** Multi-line chart with each origin year as a separate series
# MAGIC - **X-axis:** Development Period
# MAGIC - **Y-axis:** Cumulative Value (or Development Index)
# MAGIC - **Series:** Origin Year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Loss Development Curves - Absolute Values
# MAGIC -- Visualization: Line Chart (X: development_period, Y: cumulative_value, Series: origin_year)
# MAGIC SELECT 
# MAGIC     CAST(origin_year AS STRING) AS origin_year,
# MAGIC     development_period,
# MAGIC     cumulative_value
# MAGIC FROM unified_reserves.gold.loss_development_curves
# MAGIC ORDER BY origin_year, development_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Loss Development Curves - Normalized (Index = 1.0 at Dev 1)
# MAGIC -- Better for comparing development PATTERNS across years
# MAGIC -- Visualization: Line Chart (X: development_period, Y: development_index, Series: origin_year)
# MAGIC SELECT 
# MAGIC     CAST(origin_year AS STRING) AS origin_year,
# MAGIC     development_period,
# MAGIC     ROUND(development_index, 3) AS development_index
# MAGIC FROM unified_reserves.gold.loss_development_curves
# MAGIC ORDER BY origin_year, development_period

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 TILE 2: Actual vs Expected Analysis
# MAGIC **Gallery:** [Actual vs Expected](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_ave.html)
# MAGIC 
# MAGIC **Visualization:** Scatter plot with 45-degree reference line
# MAGIC - Points above line = worse than expected development
# MAGIC - Points below line = better than expected development

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Actual vs Expected Scatter Plot
# MAGIC -- Visualization: Scatter Plot (X: expected_value, Y: actual_value)
# MAGIC -- Add a 45-degree reference line in visualization settings
# MAGIC SELECT 
# MAGIC     expected_value,
# MAGIC     actual_value,
# MAGIC     CONCAT('OY ', origin_period, ' Dev ', development_period) AS label,
# MAGIC     development_flag,
# MAGIC     pct_deviation
# MAGIC FROM unified_reserves.gold.actual_vs_expected
# MAGIC WHERE expected_value > 0
# MAGIC ORDER BY expected_value

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Residual Plot by Development Period
# MAGIC -- Visualization: Bar Chart (X: development_period, Y: avg_residual)
# MAGIC -- Shows if development is consistently above/below expectations
# MAGIC SELECT 
# MAGIC     development_period,
# MAGIC     AVG(residual) AS avg_residual,
# MAGIC     AVG(pct_deviation) AS avg_pct_deviation,
# MAGIC     COUNT(*) AS n_observations,
# MAGIC     SUM(CASE WHEN development_flag = 'UNUSUAL' THEN 1 ELSE 0 END) AS unusual_count
# MAGIC FROM unified_reserves.gold.actual_vs_expected
# MAGIC GROUP BY development_period
# MAGIC ORDER BY development_period

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📉 TILE 3: IBNR Runoff Projection
# MAGIC **Gallery:** [IBNR Runoff](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_ibnr_runoff.html)
# MAGIC 
# MAGIC **Visualization:** Stacked Area Chart showing how IBNR will emerge
# MAGIC - **X-axis:** Future Development Period
# MAGIC - **Y-axis:** Projected Emergence Amount
# MAGIC - **Stacks:** Origin Year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: IBNR Runoff Projection
# MAGIC -- Visualization: Stacked Area Chart (X: future_development_period, Y: projected_emergence, Stack: origin_period)
# MAGIC SELECT 
# MAGIC     CAST(origin_period AS STRING) AS origin_year,
# MAGIC     future_development_period,
# MAGIC     ROUND(projected_emergence, 0) AS projected_emergence
# MAGIC FROM unified_reserves.gold.ibnr_runoff_projection
# MAGIC WHERE projected_emergence > 0
# MAGIC ORDER BY origin_period, future_development_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: IBNR Runoff Summary by Future Period
# MAGIC -- Visualization: Bar Chart showing total emergence by future period
# MAGIC SELECT 
# MAGIC     future_development_period,
# MAGIC     SUM(projected_emergence) AS total_emergence,
# MAGIC     COUNT(DISTINCT origin_period) AS origin_years_contributing
# MAGIC FROM unified_reserves.gold.ibnr_runoff_projection
# MAGIC WHERE projected_emergence > 0
# MAGIC GROUP BY future_development_period
# MAGIC ORDER BY future_development_period

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📏 TILE 4: ATA Factor Diagnostics
# MAGIC **Gallery:** [Tuning Development Patterns](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_development_tuning.html)
# MAGIC 
# MAGIC **Visualization:** Range chart or box plot showing factor spread
# MAGIC - Shows min, max, selected, and variability
# MAGIC - Helps identify unstable development periods

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: ATA Factor Range Chart
# MAGIC -- Visualization: Range Chart or Error Bars
# MAGIC -- (X: development_period, Y: selected_ata with min/max error bars)
# MAGIC SELECT 
# MAGIC     CONCAT('Dev ', development_period, '→', development_period + 1) AS development_age,
# MAGIC     development_period,
# MAGIC     selected_ata,
# MAGIC     simple_average,
# MAGIC     ata_min,
# MAGIC     ata_max,
# MAGIC     n_observations,
# MAGIC     stability_flag
# MAGIC FROM unified_reserves.gold.ata_factor_diagnostics
# MAGIC ORDER BY development_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Factor Variability Heatmap Data
# MAGIC -- Visualization: Single Value Cards or Conditional Table
# MAGIC SELECT 
# MAGIC     development_period,
# MAGIC     ROUND(cv * 100, 1) AS coefficient_of_variation_pct,
# MAGIC     ROUND(range_pct, 1) AS range_as_pct_of_selected,
# MAGIC     stability_flag,
# MAGIC     CASE stability_flag
# MAGIC         WHEN 'STABLE' THEN '🟢'
# MAGIC         WHEN 'LIMITED_DATA' THEN '🟡'
# MAGIC         WHEN 'HIGH_VARIABILITY' THEN '🔴'
# MAGIC     END AS status_indicator
# MAGIC FROM unified_reserves.gold.ata_factor_diagnostics
# MAGIC ORDER BY development_period

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📐 TILE 5: Clark Growth Curve Fit
# MAGIC **Gallery:** [Clark Growth Curves](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_clark.html)
# MAGIC 
# MAGIC **Visualization:** Line chart comparing actual development to fitted curve
# MAGIC - Shows how well a parametric curve fits the data
# MAGIC - Useful for tail extrapolation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Growth Curve - Actual vs Fitted
# MAGIC -- Visualization: Dual Line Chart (X: development_period, Y: actual & fitted)
# MAGIC SELECT 
# MAGIC     development_period,
# MAGIC     ROUND(actual_pct_developed * 100, 1) AS actual_pct,
# MAGIC     ROUND(fitted_pct_developed * 100, 1) AS fitted_pct,
# MAGIC     ROUND(residual * 100, 2) AS residual_pct
# MAGIC FROM unified_reserves.gold.growth_curve_fit
# MAGIC ORDER BY development_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Growth Curve Parameters
# MAGIC -- Visualization: Single Value Cards
# MAGIC SELECT 
# MAGIC     curve_type,
# MAGIC     ROUND(omega_param, 3) AS omega,
# MAGIC     ROUND(theta_param, 3) AS theta,
# MAGIC     ROUND(AVG(ABS(residual)) * 100, 2) AS mean_abs_error_pct
# MAGIC FROM unified_reserves.gold.growth_curve_fit
# MAGIC GROUP BY curve_type, omega_param, theta_param

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ⚖️ TILE 6: BornhuetterFerguson vs Chain Ladder
# MAGIC **Gallery:** [BornhutterFerguson vs Chainladder](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_bf_cl.html)
# MAGIC 
# MAGIC **Visualization:** Grouped bar chart comparing methods
# MAGIC - Shows how different methods produce different estimates
# MAGIC - BF is more stable for recent years, CL is responsive to actual

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Method Comparison - Ultimate Estimates
# MAGIC -- Visualization: Grouped Bar Chart (X: origin_year, Y: ultimate, Groups: method)
# MAGIC SELECT 
# MAGIC     origin_period AS origin_year,
# MAGIC     cl_ultimate AS `Chain Ladder`,
# MAGIC     bf_ultimate AS `Bornhuetter-Ferguson`,
# MAGIC     benktander_ultimate AS `Benktander`
# MAGIC FROM unified_reserves.gold.method_comparison
# MAGIC ORDER BY origin_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Method Comparison - IBNR Estimates
# MAGIC -- Visualization: Grouped Bar Chart comparing IBNR by method
# MAGIC SELECT 
# MAGIC     origin_period AS origin_year,
# MAGIC     ROUND(cl_ibnr, 0) AS `CL IBNR`,
# MAGIC     ROUND(bf_ibnr, 0) AS `BF IBNR`,
# MAGIC     ROUND(cl_ibnr - bf_ibnr, 0) AS `CL - BF Difference`
# MAGIC FROM unified_reserves.gold.method_comparison
# MAGIC ORDER BY origin_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Method Comparison Summary
# MAGIC -- Visualization: Summary Table
# MAGIC SELECT 
# MAGIC     'Chain Ladder' AS method,
# MAGIC     SUM(cl_ibnr) AS total_ibnr
# MAGIC FROM unified_reserves.gold.method_comparison
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Bornhuetter-Ferguson',
# MAGIC     SUM(bf_ibnr)
# MAGIC FROM unified_reserves.gold.method_comparison
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Benktander',
# MAGIC     SUM(benktander_ultimate) - SUM(paid_to_date)
# MAGIC FROM unified_reserves.gold.method_comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 TILE 7: Value at Risk / Reserve Variability
# MAGIC **Gallery:** [Value at Risk](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_value_at_risk.html)
# MAGIC 
# MAGIC **Visualization:** Error bars or tornado chart showing uncertainty range
# MAGIC - Shows confidence intervals around point estimates
# MAGIC - Useful for solvency and risk management

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Reserve Variability with Confidence Intervals
# MAGIC -- Visualization: Error Bar Chart (X: origin_year, Y: ibnr_mid with low/high bars)
# MAGIC SELECT 
# MAGIC     origin_period AS origin_year,
# MAGIC     ROUND(ibnr_low_95, 0) AS `95% Low`,
# MAGIC     ROUND(ibnr_mid, 0) AS `Point Estimate`,
# MAGIC     ROUND(ibnr_75th_pct, 0) AS `75th Percentile`,
# MAGIC     ROUND(ibnr_high_95, 0) AS `95% High`
# MAGIC FROM unified_reserves.gold.reserve_variability
# MAGIC ORDER BY origin_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Total Reserve Range
# MAGIC -- Visualization: Single Value Cards or Gauge
# MAGIC SELECT 
# MAGIC     ROUND(SUM(ibnr_low_95), 0) AS total_ibnr_low,
# MAGIC     ROUND(SUM(ibnr_mid), 0) AS total_ibnr_point,
# MAGIC     ROUND(SUM(ibnr_75th_pct), 0) AS total_ibnr_75th,
# MAGIC     ROUND(SUM(ibnr_high_95), 0) AS total_ibnr_high,
# MAGIC     ROUND(SUM(range_95), 0) AS total_uncertainty_range
# MAGIC FROM unified_reserves.gold.reserve_variability

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📅 TILE 8: Calendar Year Development
# MAGIC **Gallery:** [Triangle Slicing](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_triangle_slicing.html)
# MAGIC 
# MAGIC **Visualization:** Line chart showing emergence by calendar year
# MAGIC - Useful for identifying calendar year trends (inflation, etc.)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Calendar Year Incremental Emergence
# MAGIC -- Visualization: Bar or Line Chart (X: calendar_year, Y: total_incremental)
# MAGIC SELECT 
# MAGIC     calendar_year,
# MAGIC     ROUND(total_incremental, 0) AS total_emergence,
# MAGIC     n_origin_years,
# MAGIC     ROUND(avg_incremental, 0) AS avg_per_origin_year
# MAGIC FROM unified_reserves.gold.calendar_year_development
# MAGIC WHERE calendar_year >= 2015
# MAGIC ORDER BY calendar_year

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🌪️ TILE 9: Tail Factor Sensitivity
# MAGIC **Gallery:** [Bondy Tail Sensitivity](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_bondy_sensitivity.html)
# MAGIC 
# MAGIC **Visualization:** Horizontal bar chart (tornado) or table
# MAGIC - Shows impact of tail factor assumptions on total reserves

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Tail Factor Sensitivity Analysis
# MAGIC -- Visualization: Horizontal Bar Chart (Tornado style)
# MAGIC SELECT 
# MAGIC     scenario,
# MAGIC     tail_factor,
# MAGIC     ROUND(adjusted_ibnr, 0) AS adjusted_ibnr,
# MAGIC     ROUND(tail_ibnr_addition, 0) AS additional_ibnr,
# MAGIC     ROUND(pct_increase, 1) AS pct_increase
# MAGIC FROM unified_reserves.gold.tail_sensitivity
# MAGIC ORDER BY tail_factor

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📋 TILE 10: Development Factor Exhibit
# MAGIC **Gallery:** [MackChainladder Example](https://chainladder-python.readthedocs.io/en/latest/gallery/plot_mack.html)
# MAGIC 
# MAGIC **Visualization:** Formatted table for actuarial reports
# MAGIC - Standard exhibit format used in actuarial reports

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Complete Development Factor Exhibit
# MAGIC -- Visualization: Table with conditional formatting
# MAGIC SELECT 
# MAGIC     development_age,
# MAGIC     ata_selected,
# MAGIC     ata_simple_avg,
# MAGIC     ata_min,
# MAGIC     ata_max,
# MAGIC     cdf_to_ultimate,
# MAGIC     CONCAT(pct_developed, '%') AS pct_developed,
# MAGIC     n_observations,
# MAGIC     CONCAT(cv_pct, '%') AS cv_pct,
# MAGIC     selection_method
# MAGIC FROM unified_reserves.gold.development_exhibit
# MAGIC ORDER BY development_period

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 Dashboard Building Summary
# MAGIC 
# MAGIC ## Recommended Dashboards
# MAGIC 
# MAGIC ### Dashboard 1: "Actuarial Development Analysis"
# MAGIC | Tile | Visualization | Source Query |
# MAGIC |------|--------------|--------------|
# MAGIC | Loss Development Curves | Multi-line Chart | Tile 1 |
# MAGIC | ATA Factor Range | Range/Error Bars | Tile 4 |
# MAGIC | Development Exhibit | Table | Tile 10 |
# MAGIC | Calendar Year Trend | Line Chart | Tile 8 |
# MAGIC 
# MAGIC ### Dashboard 2: "Reserve Estimation & Comparison"
# MAGIC | Tile | Visualization | Source Query |
# MAGIC |------|--------------|--------------|
# MAGIC | BF vs CL Comparison | Grouped Bars | Tile 6 |
# MAGIC | IBNR Runoff | Stacked Area | Tile 3 |
# MAGIC | Reserve Variability | Error Bars | Tile 7 |
# MAGIC | Tail Sensitivity | Tornado Chart | Tile 9 |
# MAGIC 
# MAGIC ### Dashboard 3: "Diagnostic & Quality"
# MAGIC | Tile | Visualization | Source Query |
# MAGIC |------|--------------|--------------|
# MAGIC | Actual vs Expected | Scatter Plot | Tile 2 |
# MAGIC | Growth Curve Fit | Dual Line | Tile 5 |
# MAGIC | Residual Analysis | Bar Chart | Tile 2 |
# MAGIC | Factor Stability | Status Table | Tile 4 |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Quick Reference: Visualization Types
# MAGIC 
# MAGIC | Chainladder Plot | Databricks Equivalent |
# MAGIC |-----------------|----------------------|
# MAGIC | `triangle.plot()` | Table with heatmap colors |
# MAGIC | `.plot(kind='line')` | Line Chart |
# MAGIC | Development curves | Multi-series Line Chart |
# MAGIC | Residual plots | Scatter or Bar Chart |
# MAGIC | Bootstrap distribution | Histogram |
# MAGIC | VaR/Confidence intervals | Error Bars or Tornado |
