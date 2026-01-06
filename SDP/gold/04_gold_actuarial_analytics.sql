-- Databricks Delta Live Tables (DLT) Pipeline
-- Layer: Gold
-- Purpose: Advanced Actuarial Analytics inspired by chainladder-python gallery
-- Author: Sarah (Actuarial Data Scientist)
-- Reference: https://chainladder-python.readthedocs.io/en/latest/gallery/index.html

-- =============================================================================
-- GOLD LAYER: Actuarial Analytics Dashboard Data
-- Creates tables for Databricks dashboard visualizations
-- Inspired by chainladder-python example gallery
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. LOSS DEVELOPMENT CURVES (Gallery: "Loss Development Patterns")
-- Visualization: Multi-line chart showing cumulative development by origin year
-- Each line = one origin year, X = development period, Y = cumulative value
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_loss_development_curves
COMMENT "Loss development curves by origin year for line chart visualization"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
    origin_period AS origin_year,
    development_period,
    cumulative_value,
    -- Normalize to first development period for comparison
    cumulative_value / FIRST_VALUE(cumulative_value) OVER (
        PARTITION BY origin_period 
        ORDER BY development_period
    ) AS development_index,
    -- Calculate incremental
    cumulative_value - COALESCE(
        LAG(cumulative_value) OVER (PARTITION BY origin_period ORDER BY development_period),
        0
    ) AS incremental_value,
    triangle_type,
    loss_type
FROM LIVE.silver_triangle_cells
WHERE triangle_type = 'paid' 
  AND loss_type = 'ALL'
  AND cumulative_value IS NOT NULL
ORDER BY origin_period, development_period;


-- -----------------------------------------------------------------------------
-- 2. ACTUAL VS EXPECTED (Gallery: "Actual vs Expected")
-- Compare actual emergence to what Chain Ladder predicted
-- Visualization: Scatter plot with 45-degree reference line
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_actual_vs_expected
COMMENT "Actual vs Expected development for diagnostic visualization"
TBLPROPERTIES ("quality" = "gold")
AS
WITH expected_values AS (
    -- Calculate expected values using prior diagonal and ATA factors
    SELECT 
        t.origin_period,
        t.development_period,
        t.cumulative_value AS actual_value,
        LAG(t.cumulative_value) OVER (
            PARTITION BY t.origin_period 
            ORDER BY t.development_period
        ) AS prior_value,
        f.ata_factor,
        LAG(t.cumulative_value) OVER (
            PARTITION BY t.origin_period 
            ORDER BY t.development_period
        ) * f.ata_factor AS expected_value
    FROM LIVE.silver_triangle_cells t
    LEFT JOIN LIVE.gold_ata_factors f 
        ON t.development_period - 1 = f.development_period
    WHERE t.triangle_type = 'paid' 
      AND t.loss_type = 'ALL'
      AND t.cumulative_value IS NOT NULL
)
SELECT 
    origin_period,
    development_period,
    actual_value,
    expected_value,
    actual_value - expected_value AS residual,
    CASE 
        WHEN expected_value > 0 
        THEN (actual_value - expected_value) / expected_value * 100 
        ELSE 0 
    END AS pct_deviation,
    -- Flag unusual development
    CASE 
        WHEN ABS((actual_value - expected_value) / NULLIF(expected_value, 0)) > 0.15 
        THEN 'UNUSUAL'
        ELSE 'NORMAL'
    END AS development_flag,
    CURRENT_TIMESTAMP() AS _calculated_at
FROM expected_values
WHERE expected_value IS NOT NULL
ORDER BY origin_period, development_period;


-- -----------------------------------------------------------------------------
-- 3. IBNR RUNOFF PROJECTION (Gallery: "IBNR Runoff")
-- Shows how IBNR is expected to emerge over future development periods
-- Visualization: Stacked area chart showing IBNR runoff by origin year
-- DISABLED: Requires chain_ladder_job.py to create gold_reserve_estimates first
-- -----------------------------------------------------------------------------
-- CREATE OR REFRESH LIVE TABLE gold_ibnr_runoff_projection
-- COMMENT "Projected IBNR runoff by future development period"
-- TBLPROPERTIES ("quality" = "gold")
-- AS
-- WITH reserve_base AS (
--     SELECT 
--         origin_period,
--         latest_cumulative,
--         ultimate_loss,
--         ibnr,
--         percent_reported
--     FROM LIVE.gold_reserve_estimates
--     WHERE ibnr > 0
-- ),
-- development_pattern AS (
--     SELECT 
--         development_period,
--         ata_factor,
--         cdf_to_ultimate,
--         1.0 / cdf_to_ultimate AS percent_emerged,
--         LAG(1.0 / cdf_to_ultimate, 1, 0) OVER (ORDER BY development_period) AS prev_percent,
--         1.0 / cdf_to_ultimate - LAG(1.0 / cdf_to_ultimate, 1, 0) OVER (ORDER BY development_period) AS incremental_emergence_pct
--     FROM LIVE.gold_cdf_factors
-- ),
-- max_dev AS (
--     SELECT MAX(development_period) AS current_max_dev 
--     FROM LIVE.silver_triangle_cells 
--     WHERE cumulative_value IS NOT NULL
-- )
-- SELECT 
--     r.origin_period,
--     d.development_period AS future_development_period,
--     r.ibnr * d.incremental_emergence_pct AS projected_emergence,
--     r.ibnr,
--     r.ultimate_loss,
--     d.incremental_emergence_pct,
--     d.percent_emerged AS cumulative_emergence_pct,
--     CURRENT_TIMESTAMP() AS _calculated_at
-- FROM reserve_base r
-- CROSS JOIN development_pattern d
-- CROSS JOIN max_dev m
-- WHERE d.development_period > (m.current_max_dev - (2024 - r.origin_period))
--   AND d.incremental_emergence_pct > 0
-- ORDER BY r.origin_period, d.development_period;


-- -----------------------------------------------------------------------------
-- 4. ATA FACTOR DIAGNOSTICS (Gallery: "Tuning Development Patterns")
-- Shows spread of individual ATA factors with min/max/selected
-- Visualization: Box plot or range chart
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_ata_factor_diagnostics
COMMENT "ATA factor spread and diagnostics for selection analysis"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
    development_period,
    ata_factor AS selected_ata,
    ata_simple_avg AS simple_average,
    ata_min,
    ata_max,
    ata_stddev,
    n_observations,
    -- Coefficient of variation
    ata_stddev / NULLIF(ata_factor, 0) AS cv,
    -- Range as % of selected
    (ata_max - ata_min) / NULLIF(ata_factor, 0) * 100 AS range_pct,
    -- Flag high variability
    CASE 
        WHEN ata_stddev / NULLIF(ata_factor, 0) > 0.1 THEN 'HIGH_VARIABILITY'
        WHEN n_observations < 3 THEN 'LIMITED_DATA'
        ELSE 'STABLE'
    END AS stability_flag,
    CURRENT_TIMESTAMP() AS _calculated_at
FROM LIVE.gold_ata_factors
ORDER BY development_period;


-- -----------------------------------------------------------------------------
-- 5. CLARK GROWTH CURVE FIT (Gallery: "Clark Growth Curves")
-- Fit Loglogistic/Weibull curves to development patterns
-- Visualization: Line chart comparing actual vs fitted curve
-- DISABLED: SQL syntax error - window function inside aggregate not allowed
-- TODO: Rewrite using subqueries to pre-calculate window values
-- -----------------------------------------------------------------------------
-- CREATE OR REFRESH LIVE TABLE gold_growth_curve_fit
-- COMMENT "Fitted growth curve parameters for development pattern"
-- TBLPROPERTIES ("quality" = "gold")
-- AS
-- WITH pattern_data AS (
--     SELECT 
--         development_period,
--         1.0 / cdf_to_ultimate AS percent_developed,
--         LN(development_period) AS ln_age,
--         LN(1.0 / cdf_to_ultimate / (1 - 1.0 / cdf_to_ultimate + 0.001)) AS logit_pct
--     FROM LIVE.gold_cdf_factors
--     WHERE cdf_to_ultimate > 1.001
-- ),
-- regression AS (
--     SELECT 
--         AVG(ln_age) AS avg_x,
--         AVG(logit_pct) AS avg_y,
--         -- Cannot use window function inside aggregate
--         COUNT(*) AS n
--     FROM pattern_data
-- )
-- SELECT 
--     p.development_period,
--     p.percent_developed AS actual_pct_developed,
--     'loglogistic' AS curve_type,
--     CURRENT_TIMESTAMP() AS _calculated_at
-- FROM pattern_data p
-- ORDER BY p.development_period;


-- -----------------------------------------------------------------------------
-- 6. BORNHUETTER-FERGUSON VS CHAINLADDER (Gallery: "BornhutterFerguson vs Chainladder")
-- Compare CL ultimate with BF ultimate using a priori
-- Visualization: Bar chart comparing methods side by side
-- DISABLED: Requires chain_ladder_job.py to create gold_reserve_estimates first
-- -----------------------------------------------------------------------------
-- CREATE OR REFRESH LIVE TABLE gold_method_comparison
-- COMMENT "Comparison of Chain Ladder vs Bornhuetter-Ferguson estimates"
-- TBLPROPERTIES ("quality" = "gold")
-- AS
-- WITH apriori AS (
--     SELECT AVG(ultimate_loss) AS avg_ultimate
--     FROM LIVE.gold_reserve_estimates
--     WHERE percent_reported > 0.9
-- ),
-- estimates AS (
--     SELECT 
--         r.origin_period,
--         r.latest_cumulative,
--         r.ultimate_loss AS cl_ultimate,
--         r.ibnr AS cl_ibnr,
--         r.percent_reported,
--         a.avg_ultimate AS apriori_ultimate,
--         r.latest_cumulative + (1 - r.percent_reported) * a.avg_ultimate AS bf_ultimate,
--         (1 - r.percent_reported) * a.avg_ultimate AS bf_ibnr
--     FROM LIVE.gold_reserve_estimates r
--     CROSS JOIN apriori a
-- )
-- SELECT 
--     origin_period,
--     latest_cumulative AS paid_to_date,
--     percent_reported,
--     apriori_ultimate,
--     cl_ultimate,
--     cl_ibnr,
--     bf_ultimate,
--     bf_ibnr,
--     cl_ultimate - bf_ultimate AS cl_bf_diff,
--     cl_ibnr - bf_ibnr AS ibnr_diff,
--     cl_ultimate * percent_reported + bf_ultimate * (1 - percent_reported) AS benktander_ultimate,
--     CURRENT_TIMESTAMP() AS _calculated_at
-- FROM estimates
-- ORDER BY origin_period;


-- -----------------------------------------------------------------------------
-- 7. RESERVE VARIABILITY / VALUE AT RISK (Gallery: "Value at Risk")
-- Estimate reserve range using coefficient of variation
-- Visualization: Tornado chart or error bars
-- DISABLED: Requires chain_ladder_job.py to create gold_reserve_estimates first
-- -----------------------------------------------------------------------------
-- CREATE OR REFRESH LIVE TABLE gold_reserve_variability
-- COMMENT "Reserve variability estimates for risk analysis"
-- TBLPROPERTIES ("quality" = "gold")
-- AS
-- WITH cv_estimate AS (
--     SELECT 
--         SQRT(AVG(POWER(ata_stddev / NULLIF(ata_factor, 0), 2))) AS avg_cv
--     FROM LIVE.gold_ata_factors
--     WHERE ata_stddev IS NOT NULL
-- )
-- SELECT 
--     r.origin_period,
--     r.ibnr AS point_estimate,
--     c.avg_cv AS estimated_cv,
--     r.ibnr * c.avg_cv AS standard_error,
--     r.ibnr * EXP(-1.96 * c.avg_cv) AS ibnr_low_95,
--     r.ibnr AS ibnr_mid,
--     r.ibnr * EXP(1.96 * c.avg_cv) AS ibnr_high_95,
--     r.ibnr * EXP(0.675 * c.avg_cv) AS ibnr_75th_pct,
--     r.ibnr * (EXP(1.96 * c.avg_cv) - EXP(-1.96 * c.avg_cv)) AS range_95,
--     CURRENT_TIMESTAMP() AS _calculated_at
-- FROM LIVE.gold_reserve_estimates r
-- CROSS JOIN cv_estimate c
-- WHERE r.ibnr > 0
-- ORDER BY r.origin_period;


-- -----------------------------------------------------------------------------
-- 8. CALENDAR YEAR DEVELOPMENT (Gallery: "Triangle Slicing")
-- Aggregate by calendar year instead of accident year
-- Visualization: Line chart showing calendar year emergence
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_calendar_year_development
COMMENT "Development aggregated by calendar year"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
    origin_period + development_period - 1 AS calendar_year,
    SUM(incremental_value) AS total_incremental,
    COUNT(DISTINCT origin_period) AS n_origin_years,
    AVG(incremental_value) AS avg_incremental,
    CURRENT_TIMESTAMP() AS _calculated_at
FROM LIVE.gold_loss_development_curves
WHERE incremental_value IS NOT NULL
GROUP BY origin_period + development_period - 1
ORDER BY calendar_year;


-- -----------------------------------------------------------------------------
-- 9. TAIL FACTOR SENSITIVITY (Gallery: "Bondy Tail Sensitivity" / "TailCurve")
-- Show impact of different tail factors on reserves
-- Visualization: Sensitivity table or tornado chart
-- DISABLED: Requires chain_ladder_job.py to create gold_reserve_estimates first
-- -----------------------------------------------------------------------------
-- CREATE OR REFRESH LIVE TABLE gold_tail_sensitivity
-- COMMENT "Sensitivity of reserves to tail factor assumptions"
-- TBLPROPERTIES ("quality" = "gold")
-- AS
-- WITH base_reserves AS (
--     SELECT SUM(ibnr) AS total_ibnr_base
--     FROM LIVE.gold_reserve_estimates
-- ),
-- tail_scenarios AS (
--     SELECT 1.00 AS tail_factor, 'No Tail' AS scenario UNION ALL
--     SELECT 1.02, 'Light Tail (2%)' UNION ALL
--     SELECT 1.05, 'Moderate Tail (5%)' UNION ALL
--     SELECT 1.10, 'Heavy Tail (10%)' UNION ALL
--     SELECT 1.15, 'Very Heavy Tail (15%)'
-- )
-- SELECT 
--     t.scenario,
--     t.tail_factor,
--     b.total_ibnr_base,
--     b.total_ibnr_base * t.tail_factor AS adjusted_ibnr,
--     b.total_ibnr_base * (t.tail_factor - 1) AS tail_ibnr_addition,
--     (t.tail_factor - 1) * 100 AS pct_increase,
--     CURRENT_TIMESTAMP() AS _calculated_at
-- FROM tail_scenarios t
-- CROSS JOIN base_reserves b
-- ORDER BY t.tail_factor;


-- -----------------------------------------------------------------------------
-- 10. DEVELOPMENT FACTOR EXHIBIT (Gallery: "MackChainladder Example")
-- Complete exhibit showing all development factors with diagnostics
-- Visualization: Formatted table for actuarial reports
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_development_exhibit
COMMENT "Complete development factor exhibit for actuarial reporting"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
    f.development_period,
    CONCAT(f.development_period, ' to ', f.development_period + 1) AS development_age,
    
    -- Volume-weighted ATA
    ROUND(f.ata_factor, 4) AS ata_selected,
    
    -- Alternatives
    ROUND(f.ata_simple_avg, 4) AS ata_simple_avg,
    ROUND(f.ata_min, 4) AS ata_min,
    ROUND(f.ata_max, 4) AS ata_max,
    
    -- CDF
    ROUND(c.cdf_to_ultimate, 4) AS cdf_to_ultimate,
    ROUND(c.percent_developed, 1) AS pct_developed,
    
    -- Diagnostics
    f.n_observations,
    ROUND(f.ata_stddev, 4) AS std_dev,
    ROUND(f.ata_stddev / NULLIF(f.ata_factor, 0) * 100, 1) AS cv_pct,
    
    -- Used for selection
    CASE 
        WHEN f.n_observations >= 5 THEN 'Volume Weighted'
        WHEN f.n_observations >= 3 THEN 'Simple Average'
        ELSE 'Judgmental'
    END AS selection_method,
    
    CURRENT_TIMESTAMP() AS _calculated_at
    
FROM LIVE.gold_ata_factors f
LEFT JOIN LIVE.gold_cdf_factors c ON f.development_period = c.development_period
ORDER BY f.development_period;
