-- Databricks Delta Live Tables (DLT) Pipeline
-- Layer: Silver
-- Purpose: Loss Triangle Construction
-- Author: Marcus (Data Engineer)

-- =============================================================================
-- SILVER LAYER: Loss Triangle Construction
-- Transform linear payment data into actuarial triangle format
-- Uses batch bronze tables (with embedded sample data)
--
-- ACTUARIAL METHODOLOGY (per Loss Data Analytics Chapter 11):
-- Reference: https://openacttexts.github.io/Loss-Data-Analytics/ChapLossReserves.html
--
-- Notation:
--   i = origin_year (accident/occurrence year)
--   j = development_period (0 = year of occurrence, 1 = 1 year after, etc.)
--   X_ij = incremental payments in development period j for origin year i
--   C_ij = cumulative payments up to period j = X_i0 + X_i1 + ... + X_ij
--
-- Observable Triangle: cells where i + j <= current_year
-- Lower Triangle: cells where i + j > current_year (to be predicted)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Silver Table: Triangle Base Data
-- Joins claims with payments and calculates development periods
-- Per actuarial convention: development_period 0 = year of occurrence
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_triangle_base
COMMENT "Base data for triangle construction with development periods - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
    p.payment_id,
    p.claim_id,
    c.loss_date,
    p.payment_date,
    c.loss_type,
    c.state,
    c.county,
    c.catastrophe_code,
    
    -- Origin period (i): accident/occurrence year
    YEAR(c.loss_date) AS origin_year,
    
    -- Development period (j): 0-indexed per actuarial convention
    -- j = 0 means payment in year of occurrence
    -- j = 1 means payment 1 year after occurrence, etc.
    CAST(FLOOR(MONTHS_BETWEEN(p.payment_date, c.loss_date) / 12) AS INT) AS development_period,
    
    -- Payment amounts
    p.payment_amount,  -- Incremental payment (X_ij contribution)
    p.cumulative_paid, -- Per-claim running total (for reference only)
    p.reserve_amount
    
FROM LIVE.bronze_raw_payments_batch p
INNER JOIN LIVE.bronze_raw_claims_batch c ON p.claim_id = c.claim_id
WHERE p.payment_date >= c.loss_date;  -- Sanity check


-- -----------------------------------------------------------------------------
-- Silver Table: Loss Triangle Cells (Aggregated)
-- Aggregate payments by origin year and development period
-- 
-- CRITICAL: Per Loss Data Analytics Ch 11:
--   X_ij (incremental) = SUM of all payment_amount in cell (i,j)
--   C_ij (cumulative) = Running sum of X_i0 + X_i1 + ... + X_ij
-- 
-- We first calculate incremental, then use window function for cumulative.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_triangle_cells
COMMENT "Aggregated triangle cells with incremental (X_ij) and cumulative (C_ij) values - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
WITH incremental_cells AS (
    -- Step 1: Calculate X_ij (incremental payments per cell)
    SELECT 
        origin_year AS origin_period,
        development_period,
        'paid' AS triangle_type,
        'ALL' AS loss_type,
        SUM(payment_amount) AS incremental_value,  -- X_ij
        COUNT(DISTINCT claim_id) AS claim_count,
        'year' AS period_type
    FROM LIVE.silver_triangle_base
    WHERE development_period >= 0 AND development_period <= 9
    GROUP BY origin_year, development_period
)
-- Step 2: Calculate C_ij as running sum of X_ij within each origin year
SELECT 
    origin_period,
    development_period,
    triangle_type,
    loss_type,
    incremental_value,  -- X_ij
    -- C_ij = cumulative sum of incremental values up to this development period
    SUM(incremental_value) OVER (
        PARTITION BY origin_period 
        ORDER BY development_period 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_value,
    claim_count,
    period_type,
    CURRENT_DATE() AS as_of_date,
    CURRENT_TIMESTAMP() AS _created_at
FROM incremental_cells
ORDER BY origin_period, development_period;


-- -----------------------------------------------------------------------------
-- Silver Table: Loss Triangle by Loss Type
-- Separate triangles for each loss type with proper incremental/cumulative calc
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_triangle_by_loss_type
COMMENT "Triangle cells segmented by loss type with X_ij and C_ij - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
WITH incremental_by_type AS (
    -- Step 1: Calculate X_ij per loss type
    SELECT 
        origin_year AS origin_period,
        development_period,
        'paid' AS triangle_type,
        loss_type,
        SUM(payment_amount) AS incremental_value,  -- X_ij
        COUNT(DISTINCT claim_id) AS claim_count,
        'year' AS period_type
    FROM LIVE.silver_triangle_base
    WHERE development_period >= 0 AND development_period <= 9
    GROUP BY origin_year, development_period, loss_type
)
-- Step 2: Calculate C_ij within each (origin_period, loss_type) combination
SELECT 
    origin_period,
    development_period,
    triangle_type,
    loss_type,
    incremental_value,  -- X_ij
    SUM(incremental_value) OVER (
        PARTITION BY origin_period, loss_type 
        ORDER BY development_period 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_value,  -- C_ij
    claim_count,
    period_type,
    CURRENT_DATE() AS as_of_date,
    CURRENT_TIMESTAMP() AS _created_at
FROM incremental_by_type
ORDER BY loss_type, origin_period, development_period;


-- -----------------------------------------------------------------------------
-- Silver Table: Loss Triangle Wide Format (Pivot)
-- Triangle in traditional actuarial format (rows=origin, cols=development)
-- Per textbook Figure 11.7: rows are occurrence years, columns are dev periods
-- Development periods are 0-indexed (0, 1, 2, ..., 9)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_triangle_pivot
COMMENT "Loss triangle in pivot/wide format (C_ij values) - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
    origin_period,
    -- 0-indexed development periods matching actuarial convention
    MAX(CASE WHEN development_period = 0 THEN cumulative_value END) AS dev_0,
    MAX(CASE WHEN development_period = 1 THEN cumulative_value END) AS dev_1,
    MAX(CASE WHEN development_period = 2 THEN cumulative_value END) AS dev_2,
    MAX(CASE WHEN development_period = 3 THEN cumulative_value END) AS dev_3,
    MAX(CASE WHEN development_period = 4 THEN cumulative_value END) AS dev_4,
    MAX(CASE WHEN development_period = 5 THEN cumulative_value END) AS dev_5,
    MAX(CASE WHEN development_period = 6 THEN cumulative_value END) AS dev_6,
    MAX(CASE WHEN development_period = 7 THEN cumulative_value END) AS dev_7,
    MAX(CASE WHEN development_period = 8 THEN cumulative_value END) AS dev_8,
    MAX(CASE WHEN development_period = 9 THEN cumulative_value END) AS dev_9
FROM LIVE.silver_triangle_cells
WHERE triangle_type = 'paid' AND loss_type = 'ALL'
GROUP BY origin_period
ORDER BY origin_period;


-- -----------------------------------------------------------------------------
-- Silver Table: Risk Summary by Origin Year
-- Aggregate risk metrics for reporting
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_risk_summary
COMMENT "Risk summary aggregated by origin year and loss type - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
    origin_year,
    loss_type,
    COUNT(*) AS total_claims,
    SUM(CASE WHEN high_risk_flag_sql THEN 1 ELSE 0 END) AS high_risk_claims,
    AVG(CASE WHEN high_risk_flag_sql THEN 1.0 ELSE 0.0 END) AS avg_risk_indicator,
    SUM(incurred_amount) AS total_incurred,
    SUM(CASE WHEN high_risk_flag_sql THEN incurred_amount ELSE 0 END) AS high_risk_incurred,
    
    -- Percentages
    ROUND(SUM(CASE WHEN high_risk_flag_sql THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS high_risk_pct,
    ROUND(SUM(CASE WHEN high_risk_flag_sql THEN incurred_amount ELSE 0 END) * 100.0 / 
          NULLIF(SUM(incurred_amount), 0), 2) AS high_risk_incurred_pct,
    
    CURRENT_TIMESTAMP() AS _created_at
    
FROM LIVE.silver_claims_basic_risk
GROUP BY origin_year, loss_type
ORDER BY origin_year, loss_type;
