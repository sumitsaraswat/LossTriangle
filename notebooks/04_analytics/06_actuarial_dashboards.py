# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Actuarial Dashboard Queries
# MAGIC 
# MAGIC **Purpose:** SQL queries for building Databricks Dashboards for the Actuarial Team
# MAGIC 
# MAGIC ## Dashboard Categories
# MAGIC 1. **Executive Summary** - High-level reserve health
# MAGIC 2. **Reserve Adequacy** - Solvency monitoring
# MAGIC 3. **Loss Development** - Triangle analysis
# MAGIC 4. **IBNR Analysis** - Reserve breakdown
# MAGIC 5. **Risk Exposure** - AI-detected risks
# MAGIC 6. **Claims Monitoring** - Operational metrics
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## How to Create Dashboards
# MAGIC 1. Run each query below
# MAGIC 2. Click the chart icon to create a visualization
# MAGIC 3. Pin visualizations to a new Dashboard
# MAGIC 4. Or use **SQL Editor** → **Dashboards** → **Create Dashboard**

# COMMAND ----------

# Configuration
CATALOG = "unified_reserves"

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📈 DASHBOARD 1: Executive Summary
# MAGIC 
# MAGIC **Audience:** CFO, Board of Directors
# MAGIC 
# MAGIC **Key Metrics:** Total reserves, adequacy status, risk indicators

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Reserve Health Scorecard (Single Value Cards)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Total IBNR Reserve
# MAGIC -- Visualization: Counter/Single Value
# MAGIC SELECT 
# MAGIC     CONCAT('$', FORMAT_NUMBER(total_ibnr, 0)) as total_ibnr_formatted,
# MAGIC     total_ibnr
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Reserve Adequacy Percentage
# MAGIC -- Visualization: Counter with conditional formatting (Green >= 100%, Yellow >= 85%, Red < 85%)
# MAGIC SELECT 
# MAGIC     CONCAT(ROUND(reserve_adequacy * 100, 1), '%') as adequacy_formatted,
# MAGIC     reserve_adequacy,
# MAGIC     CASE 
# MAGIC         WHEN reserve_adequacy >= 1.0 THEN 'ADEQUATE'
# MAGIC         WHEN reserve_adequacy >= 0.85 THEN 'MARGINAL'
# MAGIC         ELSE 'DEFICIENT'
# MAGIC     END as status
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Total Ultimate Loss
# MAGIC -- Visualization: Counter/Single Value
# MAGIC SELECT 
# MAGIC     CONCAT('$', FORMAT_NUMBER(total_ultimate, 0)) as ultimate_formatted,
# MAGIC     total_ultimate
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: AI Risk Adjustment Factor
# MAGIC -- Visualization: Counter showing additional reserve needed due to AI-detected risks
# MAGIC SELECT 
# MAGIC     ROUND((ai_adjustment_factor - 1) * 100, 1) as additional_reserve_pct,
# MAGIC     CONCAT('+', ROUND((ai_adjustment_factor - 1) * 100, 1), '%') as formatted,
# MAGIC     ai_adjustment_factor
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Executive Summary Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Executive Summary Overview
# MAGIC -- Visualization: Table
# MAGIC SELECT 
# MAGIC     'Total Paid to Date' as metric,
# MAGIC     CONCAT('$', FORMAT_NUMBER(total_paid, 0)) as value
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Total IBNR Reserve' as metric,
# MAGIC     CONCAT('$', FORMAT_NUMBER(total_ibnr, 0)) as value
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Total Ultimate Loss' as metric,
# MAGIC     CONCAT('$', FORMAT_NUMBER(total_ultimate, 0)) as value
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Held Reserves' as metric,
# MAGIC     CONCAT('$', FORMAT_NUMBER(held_reserves, 0)) as value
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Reserve Margin' as metric,
# MAGIC     CONCAT('$', FORMAT_NUMBER(reserve_margin, 0)) as value
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Reserve Adequacy' as metric,
# MAGIC     CONCAT(ROUND(reserve_adequacy * 100, 1), '%') as value
# MAGIC FROM unified_reserves.gold.reserve_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📊 DASHBOARD 2: Reserve Adequacy Monitor
# MAGIC 
# MAGIC **Audience:** Chief Actuary, Reserve Committee
# MAGIC 
# MAGIC **Purpose:** Monitor reserve health and solvency risk

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Reserve Adequacy Gauge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Reserve Adequacy by Loss Type
# MAGIC -- Visualization: Gauge Chart or Bar Chart with thresholds
# MAGIC SELECT 
# MAGIC     loss_type,
# MAGIC     ROUND(reserve_adequacy * 100, 1) as adequacy_pct,
# MAGIC     adequacy_status,
# MAGIC     estimated_ultimate_loss,
# MAGIC     held_reserves,
# MAGIC     reserve_margin
# MAGIC FROM unified_reserves.gold.v_reserve_adequacy
# MAGIC ORDER BY reserve_adequacy ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 IBNR by Origin Year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: IBNR Reserves by Origin Year
# MAGIC -- Visualization: Bar Chart (X: origin_year, Y: estimated_ibnr)
# MAGIC SELECT 
# MAGIC     origin_year,
# MAGIC     paid_to_date,
# MAGIC     projected_ultimate,
# MAGIC     estimated_ibnr,
# MAGIC     ROUND(percent_developed, 1) as percent_developed
# MAGIC FROM unified_reserves.gold.v_ibnr_by_origin
# MAGIC ORDER BY origin_year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Stacked Bar - Paid vs IBNR by Origin Year
# MAGIC -- Visualization: Stacked Bar Chart
# MAGIC SELECT 
# MAGIC     origin_year,
# MAGIC     paid_to_date as `Paid to Date`,
# MAGIC     estimated_ibnr as `IBNR Reserve`
# MAGIC FROM unified_reserves.gold.v_ibnr_by_origin
# MAGIC ORDER BY origin_year

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Development Completion by Origin Year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Percent Developed by Origin Year
# MAGIC -- Visualization: Horizontal Bar Chart or Bullet Chart
# MAGIC SELECT 
# MAGIC     origin_year,
# MAGIC     ROUND(percent_developed, 1) as percent_developed,
# MAGIC     100 - ROUND(percent_developed, 1) as percent_remaining
# MAGIC FROM unified_reserves.gold.v_ibnr_by_origin
# MAGIC ORDER BY origin_year

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📐 DASHBOARD 3: Loss Development Analysis
# MAGIC 
# MAGIC **Audience:** Reserving Actuaries
# MAGIC 
# MAGIC **Purpose:** Analyze historical development patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Age-to-Age Development Factors

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Development Factors Table
# MAGIC -- Visualization: Table with conditional formatting
# MAGIC SELECT 
# MAGIC     development_period,
# MAGIC     ROUND(ata_factor, 4) as ata_factor,
# MAGIC     ROUND(cdf_to_ultimate, 4) as cdf_to_ultimate,
# MAGIC     ROUND(1/cdf_to_ultimate * 100, 1) as percent_developed
# MAGIC FROM unified_reserves.gold.development_factors
# MAGIC ORDER BY development_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: ATA Factor Trend
# MAGIC -- Visualization: Line Chart (X: development_period, Y: ata_factor)
# MAGIC SELECT 
# MAGIC     CONCAT('Period ', development_period, ' → ', development_period + 1) as development_stage,
# MAGIC     development_period,
# MAGIC     ata_factor
# MAGIC FROM unified_reserves.gold.development_factors
# MAGIC ORDER BY development_period

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Loss Triangle Heatmap

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Loss Triangle Data for Heatmap
# MAGIC -- Visualization: Pivot Table or Heatmap
# MAGIC SELECT 
# MAGIC     origin_period,
# MAGIC     development_period,
# MAGIC     cumulative_value
# MAGIC FROM unified_reserves.silver.loss_triangles
# MAGIC WHERE triangle_type = 'paid' 
# MAGIC   AND loss_type = 'ALL'
# MAGIC   AND cumulative_value IS NOT NULL
# MAGIC ORDER BY origin_period, development_period

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Loss Triangle Pivot View
# MAGIC -- Visualization: Table (Pivot format)
# MAGIC SELECT 
# MAGIC     origin_period,
# MAGIC     MAX(CASE WHEN development_period = 1 THEN cumulative_value END) as dev_1,
# MAGIC     MAX(CASE WHEN development_period = 2 THEN cumulative_value END) as dev_2,
# MAGIC     MAX(CASE WHEN development_period = 3 THEN cumulative_value END) as dev_3,
# MAGIC     MAX(CASE WHEN development_period = 4 THEN cumulative_value END) as dev_4,
# MAGIC     MAX(CASE WHEN development_period = 5 THEN cumulative_value END) as dev_5
# MAGIC FROM unified_reserves.silver.loss_triangles
# MAGIC WHERE triangle_type = 'paid' AND loss_type = 'ALL'
# MAGIC GROUP BY origin_period
# MAGIC ORDER BY origin_period

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Incremental Development Pattern

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Incremental Losses by Development Period
# MAGIC -- Visualization: Area Chart showing payment timing
# MAGIC WITH cumulative AS (
# MAGIC     SELECT 
# MAGIC         origin_period,
# MAGIC         development_period,
# MAGIC         cumulative_value,
# MAGIC         LAG(cumulative_value) OVER (PARTITION BY origin_period ORDER BY development_period) as prev_value
# MAGIC     FROM unified_reserves.silver.loss_triangles
# MAGIC     WHERE triangle_type = 'paid' AND loss_type = 'ALL'
# MAGIC )
# MAGIC SELECT 
# MAGIC     development_period,
# MAGIC     SUM(COALESCE(cumulative_value - prev_value, cumulative_value)) as incremental_loss
# MAGIC FROM cumulative
# MAGIC GROUP BY development_period
# MAGIC ORDER BY development_period

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # ⚠️ DASHBOARD 4: Risk Exposure Analysis
# MAGIC 
# MAGIC **Audience:** Chief Risk Officer, Actuarial Team
# MAGIC 
# MAGIC **Purpose:** Monitor AI-detected risks and high-exposure claims

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Risk Category Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Claims by Risk Category
# MAGIC -- Visualization: Pie Chart or Donut Chart
# MAGIC SELECT 
# MAGIC     risk_category,
# MAGIC     COUNT(*) as claim_count,
# MAGIC     SUM(incurred_amount) as total_exposure
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC GROUP BY risk_category
# MAGIC ORDER BY total_exposure DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 High Risk Exposure by Origin Year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: High Risk Claims Trend
# MAGIC -- Visualization: Combo Chart (Bars: high_risk_claims, Line: high_risk_percentage)
# MAGIC SELECT 
# MAGIC     origin_year,
# MAGIC     total_claims,
# MAGIC     high_risk_claims,
# MAGIC     ROUND(high_risk_percentage, 1) as high_risk_pct,
# MAGIC     high_risk_exposure,
# MAGIC     ROUND(high_risk_exposure_pct, 1) as high_risk_exposure_pct
# MAGIC FROM unified_reserves.gold.v_risk_exposure
# MAGIC ORDER BY origin_year

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Risk Score Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Risk Score Histogram
# MAGIC -- Visualization: Histogram
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN risk_score = 0 THEN '0 - No Risk'
# MAGIC         WHEN risk_score < 0.3 THEN '0.01-0.30 Low'
# MAGIC         WHEN risk_score < 0.5 THEN '0.30-0.50 Medium'
# MAGIC         WHEN risk_score < 0.7 THEN '0.50-0.70 High'
# MAGIC         ELSE '0.70+ Critical'
# MAGIC     END as risk_bucket,
# MAGIC     COUNT(*) as claim_count,
# MAGIC     SUM(incurred_amount) as total_exposure
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN risk_score = 0 THEN '0 - No Risk'
# MAGIC         WHEN risk_score < 0.3 THEN '0.01-0.30 Low'
# MAGIC         WHEN risk_score < 0.5 THEN '0.30-0.50 Medium'
# MAGIC         WHEN risk_score < 0.7 THEN '0.50-0.70 High'
# MAGIC         ELSE '0.70+ Critical'
# MAGIC     END
# MAGIC ORDER BY risk_bucket

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Top High-Risk Claims Watchlist

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: High Risk Claims Detail
# MAGIC -- Visualization: Table with drill-down
# MAGIC SELECT 
# MAGIC     claim_id,
# MAGIC     loss_date,
# MAGIC     loss_type,
# MAGIC     county,
# MAGIC     risk_category,
# MAGIC     ROUND(risk_score, 2) as risk_score,
# MAGIC     incurred_amount,
# MAGIC     adjuster_notes
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC WHERE high_risk_flag = TRUE
# MAGIC ORDER BY risk_score DESC, incurred_amount DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Risk Keywords Frequency

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Most Common Risk Keywords
# MAGIC -- Visualization: Bar Chart or Word Cloud
# MAGIC SELECT 
# MAGIC     keyword,
# MAGIC     COUNT(*) as occurrence_count
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC LATERAL VIEW EXPLODE(risk_keywords) t AS keyword
# MAGIC WHERE keyword IS NOT NULL
# MAGIC GROUP BY keyword
# MAGIC ORDER BY occurrence_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📋 DASHBOARD 5: Claims Operations Monitor
# MAGIC 
# MAGIC **Audience:** Claims Manager, Operations Team
# MAGIC 
# MAGIC **Purpose:** Operational metrics and claims inventory

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Claims Inventory by Status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Claims Count by Status
# MAGIC -- Visualization: Pie Chart
# MAGIC SELECT 
# MAGIC     claim_status,
# MAGIC     COUNT(*) as claim_count,
# MAGIC     SUM(incurred_amount) as total_incurred
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC GROUP BY claim_status
# MAGIC ORDER BY claim_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Claims by Loss Type

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Claims Distribution by Loss Type
# MAGIC -- Visualization: Bar Chart
# MAGIC SELECT 
# MAGIC     loss_type,
# MAGIC     COUNT(*) as claim_count,
# MAGIC     SUM(incurred_amount) as total_incurred,
# MAGIC     AVG(incurred_amount) as avg_incurred
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC GROUP BY loss_type
# MAGIC ORDER BY total_incurred DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Geographic Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Claims by County
# MAGIC -- Visualization: Map or Bar Chart
# MAGIC SELECT 
# MAGIC     county,
# MAGIC     state,
# MAGIC     COUNT(*) as claim_count,
# MAGIC     SUM(incurred_amount) as total_incurred,
# MAGIC     SUM(CASE WHEN high_risk_flag THEN 1 ELSE 0 END) as high_risk_count
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC WHERE county IS NOT NULL
# MAGIC GROUP BY county, state
# MAGIC ORDER BY total_incurred DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Claims Aging Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Open Claims by Age Bucket
# MAGIC -- Visualization: Bar Chart
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN DATEDIFF(CURRENT_DATE(), loss_date) <= 365 THEN '0-12 months'
# MAGIC         WHEN DATEDIFF(CURRENT_DATE(), loss_date) <= 730 THEN '13-24 months'
# MAGIC         WHEN DATEDIFF(CURRENT_DATE(), loss_date) <= 1095 THEN '25-36 months'
# MAGIC         WHEN DATEDIFF(CURRENT_DATE(), loss_date) <= 1825 THEN '37-60 months'
# MAGIC         ELSE '60+ months'
# MAGIC     END as age_bucket,
# MAGIC     COUNT(*) as claim_count,
# MAGIC     SUM(incurred_amount) as total_incurred
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC WHERE claim_status = 'Open'
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN DATEDIFF(CURRENT_DATE(), loss_date) <= 365 THEN '0-12 months'
# MAGIC         WHEN DATEDIFF(CURRENT_DATE(), loss_date) <= 730 THEN '13-24 months'
# MAGIC         WHEN DATEDIFF(CURRENT_DATE(), loss_date) <= 1095 THEN '25-36 months'
# MAGIC         WHEN DATEDIFF(CURRENT_DATE(), loss_date) <= 1825 THEN '37-60 months'
# MAGIC         ELSE '60+ months'
# MAGIC     END
# MAGIC ORDER BY 
# MAGIC     CASE age_bucket
# MAGIC         WHEN '0-12 months' THEN 1
# MAGIC         WHEN '13-24 months' THEN 2
# MAGIC         WHEN '25-36 months' THEN 3
# MAGIC         WHEN '37-60 months' THEN 4
# MAGIC         ELSE 5
# MAGIC     END

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Catastrophe Event Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Claims by Catastrophe Code
# MAGIC -- Visualization: Bar Chart
# MAGIC SELECT 
# MAGIC     COALESCE(catastrophe_code, 'Non-CAT') as catastrophe_code,
# MAGIC     COUNT(*) as claim_count,
# MAGIC     SUM(incurred_amount) as total_incurred,
# MAGIC     SUM(CASE WHEN high_risk_flag THEN incurred_amount ELSE 0 END) as high_risk_incurred
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC GROUP BY COALESCE(catastrophe_code, 'Non-CAT')
# MAGIC ORDER BY total_incurred DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📉 DASHBOARD 6: Trend Analysis
# MAGIC 
# MAGIC **Audience:** Pricing Actuaries, Management
# MAGIC 
# MAGIC **Purpose:** Identify trends and emerging patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Loss Development by Origin Year

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Cumulative Loss Development Curves
# MAGIC -- Visualization: Line Chart (Multiple series by origin_year)
# MAGIC SELECT 
# MAGIC     origin_period as origin_year,
# MAGIC     development_period,
# MAGIC     cumulative_value
# MAGIC FROM unified_reserves.silver.loss_triangles
# MAGIC WHERE triangle_type = 'paid' 
# MAGIC   AND loss_type = 'ALL'
# MAGIC   AND cumulative_value IS NOT NULL
# MAGIC ORDER BY origin_period, development_period

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Average Claim Size Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Average Incurred by Origin Year
# MAGIC -- Visualization: Line Chart
# MAGIC SELECT 
# MAGIC     origin_year,
# MAGIC     COUNT(*) as claim_count,
# MAGIC     AVG(incurred_amount) as avg_incurred,
# MAGIC     PERCENTILE(incurred_amount, 0.5) as median_incurred,
# MAGIC     MAX(incurred_amount) as max_incurred
# MAGIC FROM unified_reserves.silver.enriched_claims
# MAGIC GROUP BY origin_year
# MAGIC ORDER BY origin_year

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 High Risk Trend Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: High Risk Percentage Trend
# MAGIC -- Visualization: Line Chart with trend line
# MAGIC SELECT 
# MAGIC     origin_year,
# MAGIC     high_risk_percentage,
# MAGIC     high_risk_exposure_pct,
# MAGIC     avg_risk_score
# MAGIC FROM unified_reserves.gold.v_risk_exposure
# MAGIC ORDER BY origin_year

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 🎯 DASHBOARD 7: The Boardroom Dashboard
# MAGIC 
# MAGIC **Audience:** Board of Directors, C-Suite
# MAGIC 
# MAGIC **Purpose:** One-page executive view for board meetings

# COMMAND ----------

# MAGIC %sql
# MAGIC -- QUERY: Boardroom Summary - All Key Metrics
# MAGIC -- Visualization: Table or Cards
# MAGIC SELECT 
# MAGIC     'Total Reserves Held' as metric,
# MAGIC     CONCAT('$', FORMAT_NUMBER(held_reserves, 0)) as value,
# MAGIC     'Amount set aside for future claims' as description,
# MAGIC     CASE WHEN reserve_adequacy >= 1 THEN '🟢' WHEN reserve_adequacy >= 0.85 THEN '🟡' ELSE '🔴' END as status
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'Reserve Adequacy' as metric,
# MAGIC     CONCAT(ROUND(reserve_adequacy * 100, 0), '%') as value,
# MAGIC     'Held reserves vs estimated ultimate' as description,
# MAGIC     CASE WHEN reserve_adequacy >= 1 THEN '🟢' WHEN reserve_adequacy >= 0.85 THEN '🟡' ELSE '🔴' END as status
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'AI Risk Adjustment' as metric,
# MAGIC     CONCAT('+', ROUND((ai_adjustment_factor - 1) * 100, 1), '%') as value,
# MAGIC     'Additional reserve for hidden risks' as description,
# MAGIC     CASE WHEN ai_adjustment_factor <= 1.1 THEN '🟢' WHEN ai_adjustment_factor <= 1.25 THEN '🟡' ELSE '🔴' END as status
# MAGIC FROM unified_reserves.gold.reserve_summary
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'High Risk Claims' as metric,
# MAGIC     CAST(SUM(CASE WHEN high_risk_flag THEN 1 ELSE 0 END) as STRING) as value,
# MAGIC     'Claims flagged by AI for mold/litigation' as description,
# MAGIC     '⚠️' as status
# MAGIC FROM unified_reserves.silver.enriched_claims

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # 📝 Dashboard Building Instructions
# MAGIC 
# MAGIC ## Option 1: Create from SQL Editor
# MAGIC 1. Go to **SQL Editor** in Databricks
# MAGIC 2. Copy any query from above
# MAGIC 3. Run the query
# MAGIC 4. Click **+ Add visualization**
# MAGIC 5. Choose chart type and configure
# MAGIC 6. Click **Add to dashboard** → Create new or add to existing
# MAGIC 
# MAGIC ## Option 2: Create from this Notebook
# MAGIC 1. Run each query cell
# MAGIC 2. Click the chart icon below the results
# MAGIC 3. Configure the visualization
# MAGIC 4. Click **Pin to dashboard**
# MAGIC 
# MAGIC ## Recommended Dashboard Layout
# MAGIC 
# MAGIC | Dashboard | Key Visualizations |
# MAGIC |-----------|-------------------|
# MAGIC | **Executive Summary** | Counters, Gauge, Summary Table |
# MAGIC | **Reserve Adequacy** | Gauge, Stacked Bar, Progress Bars |
# MAGIC | **Loss Development** | Triangle Table, Line Charts, Heatmap |
# MAGIC | **Risk Monitor** | Pie Chart, Bar Charts, Watchlist Table |
# MAGIC | **Claims Operations** | Pie Charts, Geographic Map, Aging Bars |
# MAGIC | **Boardroom** | Single-page with all key metrics |
# MAGIC 
# MAGIC ## Refresh Schedule
# MAGIC - Set dashboards to refresh daily or after each pipeline run
# MAGIC - Use **Schedule** button in dashboard settings







