-- Databricks Delta Live Tables (DLT) Pipeline
-- Layer: Silver
-- Purpose: Enriched claims with payment aggregations
-- Author: Marcus (Data Engineer)

-- =============================================================================
-- SILVER LAYER: Enriched Claims
-- NOTE: Change table references based on which bronze you use:
--   - Streaming: bronze_raw_claims, bronze_raw_payments
--   - Batch:     bronze_raw_claims_batch, bronze_raw_payments_batch
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Silver Table: Claims with Latest Payment Info
-- Joins claims with aggregated payment data
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_claims_with_payments
COMMENT "Claims enriched with payment aggregations - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
WITH latest_payments AS (
    SELECT 
        claim_id,
        MAX(payment_date) AS latest_payment_date,
        SUM(payment_amount) AS total_paid,
        MAX(reserve_amount) AS current_reserve
    -- Use bronze_raw_payments for streaming, bronze_raw_payments_batch for batch
    FROM LIVE.bronze_raw_payments_batch
    GROUP BY claim_id
)
SELECT 
    c.claim_id,
    c.policy_id,
    c.loss_date,
    c.report_date,
    c.claimant_name,
    c.loss_type,
    c.loss_description,
    c.adjuster_notes,
    c.claim_status,
    c.county,
    c.state,
    c.zip_code,
    c.catastrophe_code,
    
    -- Origin period calculations
    YEAR(c.loss_date) AS origin_year,
    YEAR(c.loss_date) AS origin_period,
    
    -- Payment aggregations
    p.latest_payment_date,
    COALESCE(p.total_paid, 0) AS total_paid,
    COALESCE(p.current_reserve, 0) AS current_reserve,
    COALESCE(p.total_paid, 0) + COALESCE(p.current_reserve, 0) AS incurred_amount,
    
    -- Metadata
    c._ingested_at,
    current_timestamp() AS _processed_at
    
-- Use bronze_raw_claims for streaming, bronze_raw_claims_batch for batch
FROM LIVE.bronze_raw_claims_batch c
LEFT JOIN latest_payments p ON c.claim_id = p.claim_id;


-- -----------------------------------------------------------------------------
-- Silver Table: Claims with Basic Risk Flags (SQL-based)
-- Basic risk detection using SQL pattern matching
-- Note: Advanced NLP risk detection done in Python pipeline
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_claims_basic_risk
COMMENT "Claims with SQL-based risk flags - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
    *,
    
    -- Water/Mold risk detection (SQL pattern matching)
    CASE 
        WHEN LOWER(adjuster_notes) LIKE '%mold%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%seepage%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%moisture%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%damp%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%black mold%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%toxic mold%' THEN TRUE
        ELSE FALSE
    END AS has_water_mold_risk,
    
    -- Litigation risk detection
    CASE 
        WHEN LOWER(adjuster_notes) LIKE '%attorney%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%lawyer%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%lawsuit%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%litigation%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%legal counsel%' THEN TRUE
        ELSE FALSE
    END AS has_litigation_risk,
    
    -- Structural risk detection
    CASE 
        WHEN LOWER(adjuster_notes) LIKE '%structural%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%foundation%' THEN TRUE
        WHEN LOWER(adjuster_notes) LIKE '%collapse%' THEN TRUE
        ELSE FALSE
    END AS has_structural_risk,
    
    -- Combined risk category (SQL version)
    CASE 
        WHEN LOWER(adjuster_notes) LIKE '%attorney%' OR LOWER(adjuster_notes) LIKE '%lawsuit%' THEN 'litigation'
        WHEN LOWER(adjuster_notes) LIKE '%mold%' OR LOWER(adjuster_notes) LIKE '%seepage%' THEN 'water_damage'
        WHEN LOWER(adjuster_notes) LIKE '%structural%' OR LOWER(adjuster_notes) LIKE '%foundation%' THEN 'structural'
        ELSE 'low_risk'
    END AS risk_category_sql,
    
    -- High risk flag (any risk detected)
    CASE 
        WHEN LOWER(adjuster_notes) LIKE '%mold%' 
          OR LOWER(adjuster_notes) LIKE '%attorney%' 
          OR LOWER(adjuster_notes) LIKE '%lawsuit%'
          OR LOWER(adjuster_notes) LIKE '%structural%'
          OR LOWER(adjuster_notes) LIKE '%seepage%'
        THEN TRUE
        ELSE FALSE
    END AS high_risk_flag_sql
    
FROM LIVE.silver_claims_with_payments;
