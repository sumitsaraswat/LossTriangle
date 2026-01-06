-- Databricks Delta Live Tables (DLT) Pipeline
-- Layer: Gold
-- Purpose: Semantic Views for AI/BI Genie
-- Author: David (Analytics Lead)

-- =============================================================================
-- GOLD LAYER: Semantic Views for Reporting & Genie
-- All views created in SQL for better performance
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Gold View: Reserve Adequacy
-- Key metric for solvency monitoring
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_v_reserve_adequacy
COMMENT "Reserve adequacy metrics for executive reporting - Gold Layer"
TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
    as_of_date,
    loss_type,
    total_ultimate AS estimated_ultimate_loss,
    held_reserves,
    reserve_adequacy,
    CASE 
        WHEN reserve_adequacy >= 1.0 THEN 'ADEQUATE'
        WHEN reserve_adequacy >= 0.85 THEN 'MARGINAL'
        ELSE 'DEFICIENT'
    END AS adequacy_status,
    total_ibnr,
    reserve_margin,
    ai_adjustment_factor
FROM LIVE.gold_reserve_summary;


-- -----------------------------------------------------------------------------
-- Gold View: IBNR by Origin Year
-- Detailed reserve breakdown
-- NOTE: gold_reserve_estimates is created by chain_ladder_job.py (external to DLT)
-- DISABLED: Requires chain_ladder_job.py to run first to create the source table
-- Re-enable after running chain_ladder_job.py
-- -----------------------------------------------------------------------------
-- CREATE OR REFRESH LIVE TABLE gold_v_ibnr_by_origin
-- COMMENT "IBNR reserves by origin year - Gold Layer"
-- TBLPROPERTIES ("quality" = "gold")
-- AS 
-- SELECT 
--     origin_period AS origin_year,
--     loss_type,
--     triangle_type,
--     latest_cumulative AS paid_to_date,
--     ultimate_loss AS projected_ultimate,
--     ibnr AS estimated_ibnr,
--     ROUND(percent_reported * 100, 1) AS percent_developed,
--     _calculated_at AS calculation_date
-- FROM unified_reserves.losstriangle.gold_reserve_estimates;


-- -----------------------------------------------------------------------------
-- Gold View: Risk Exposure
-- AI-detected risk metrics
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_v_risk_exposure
COMMENT "Risk exposure metrics from NLP analysis - Gold Layer"
TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
    origin_year,
    loss_type,
    total_claims,
    high_risk_claims,
    ROUND(high_risk_pct, 1) AS high_risk_percentage,
    total_incurred,
    high_risk_incurred AS high_risk_exposure,
    ROUND(high_risk_incurred_pct, 1) AS high_risk_exposure_pct,
    ROUND(avg_risk_score, 3) AS avg_risk_score
FROM LIVE.silver_risk_summary_nlp;


-- -----------------------------------------------------------------------------
-- Gold View: Executive Dashboard
-- Combined view for boardroom reporting
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_v_executive_dashboard
COMMENT "Executive dashboard metrics - Gold Layer"
TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
    rs.as_of_date,
    rs.loss_type,
    rs.total_paid,
    rs.total_ibnr,
    rs.total_ultimate,
    rs.held_reserves,
    rs.reserve_adequacy,
    rs.reserve_margin,
    rs.ai_adjustment_factor,
    rs.total_ibnr * rs.ai_adjustment_factor AS risk_adjusted_ibnr,
    CASE 
        WHEN rs.reserve_adequacy >= 1.0 THEN 'GREEN'
        WHEN rs.reserve_adequacy >= 0.85 THEN 'YELLOW'
        ELSE 'RED'
    END AS status_indicator
FROM LIVE.gold_reserve_summary rs;


-- -----------------------------------------------------------------------------
-- Gold View: Development Factor Summary
-- For actuarial review
-- NOTE: gold_development_factors_python is created by chain_ladder_job.py (external to DLT)
-- DISABLED: Requires chain_ladder_job.py to run first to create the source table
-- Re-enable after running chain_ladder_job.py
-- -----------------------------------------------------------------------------
-- CREATE OR REFRESH LIVE TABLE gold_v_development_factors
-- COMMENT "Development factor summary for actuarial analysis - Gold Layer"
-- TBLPROPERTIES ("quality" = "gold")
-- AS 
-- SELECT 
--     development_period,
--     ROUND(ata_factor, 4) AS ata_factor,
--     ROUND(cdf_to_ultimate, 4) AS cdf_to_ultimate,
--     ROUND(percent_developed, 1) AS percent_developed,
--     triangle_type,
--     loss_type,
--     _calculated_at
-- FROM unified_reserves.losstriangle.gold_development_factors_python
-- ORDER BY development_period;


-- -----------------------------------------------------------------------------
-- Gold View: Claims Watchlist
-- High-risk claims for monitoring
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_v_claims_watchlist
COMMENT "High-risk claims watchlist - Gold Layer"
TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
    claim_id,
    loss_date,
    origin_year,
    loss_type,
    county,
    state,
    claim_status,
    risk_category,
    ROUND(risk_score, 2) AS risk_score,
    risk_keywords,
    total_paid,
    current_reserve,
    incurred_amount,
    adjuster_notes
FROM LIVE.silver_enriched_claims_nlp
WHERE high_risk_flag = TRUE
ORDER BY risk_score DESC, incurred_amount DESC;






