-- Databricks Delta Live Tables (DLT) Pipeline
-- Layer: Silver
-- Purpose: AI-Powered NLP Risk Detection using Databricks AI Functions
-- Author: Anya (AI Engineer)
-- Reference: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin#ai-functions

-- =============================================================================
-- SILVER LAYER: NLP Risk Detection with AI Functions
-- Leverages Databricks built-in AI functions for intelligent risk classification
-- 
-- AI Functions Used:
--   - ai_classify: Classifies claims into risk categories
--   - ai_extract: Extracts specific risk entities from text
--   - ai_analyze_sentiment: Detects sentiment indicating claim severity
--
-- Requirements:
--   - Databricks SQL Pro or Serverless compute
--   - Foundation Model APIs enabled
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Silver Table: Claims with AI-Powered Risk Classification
-- Uses ai_classify to intelligently categorize claims into risk levels
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_enriched_claims_nlp
COMMENT "Claims enriched with AI-powered risk analysis - Silver Layer"
TBLPROPERTIES (
    "quality" = "silver",
    "pipelines.autoOptimize.managed" = "true"
)
AS 
WITH claims_with_text AS (
    -- Prepare combined text for AI analysis (truncate to stay within token limits)
    SELECT 
        *,
        LEFT(
            CONCAT_WS(
                ' | ',
                COALESCE(adjuster_notes, ''),
                COALESCE(loss_description, '')
            ),
            3500
        ) AS _analysis_text
    FROM LIVE.silver_claims_with_payments
    WHERE adjuster_notes IS NOT NULL OR loss_description IS NOT NULL
),
ai_analysis AS (
    -- Apply AI functions to classify and extract risk information
    SELECT 
        *,
        
        -- AI Classification: Classify into risk categories
        -- ai_classify uses foundation models to understand context
        ai_classify(
            _analysis_text,
            ARRAY('high_risk', 'medium_risk', 'low_risk')
        ) AS risk_category,
        
        -- AI Entity Extraction: Extract specific risk indicators
        -- Returns a struct with each entity type and extracted values
        ai_extract(
            _analysis_text,
            ARRAY(
                'mold_damage',
                'water_damage', 
                'litigation_risk',
                'structural_damage',
                'bodily_injury',
                'fraud_indicator',
                'attorney_involvement'
            )
        ) AS extracted_risks,
        
        -- Sentiment Analysis: Negative sentiment often indicates problematic claims
        ai_analyze_sentiment(_analysis_text) AS claim_sentiment
        
    FROM claims_with_text
)
SELECT 
    -- Original columns
    claim_id,
    policy_id,
    loss_date,
    report_date,
    claimant_name,
    loss_type,
    loss_description,
    adjuster_notes,
    claim_status,
    county,
    state,
    zip_code,
    catastrophe_code,
    origin_year,
    origin_period,
    latest_payment_date,
    total_paid,
    current_reserve,
    incurred_amount,
    _ingested_at,
    _processed_at,
    
    -- AI-derived risk fields
    risk_category,
    extracted_risks,
    claim_sentiment,
    
    -- Risk score derived from AI classification + sentiment
    -- high_risk=1.0, medium_risk=0.5, low_risk=0.1
    -- Boost by 0.2 if sentiment is negative
    LEAST(1.0,
        CASE 
            WHEN risk_category = 'high_risk' THEN 1.0
            WHEN risk_category = 'medium_risk' THEN 0.5
            ELSE 0.1
        END
        + CASE WHEN claim_sentiment = 'negative' THEN 0.2 ELSE 0.0 END
    ) AS risk_score,
    
    -- High risk flag
    CASE 
        WHEN risk_category = 'high_risk' THEN TRUE
        WHEN risk_category = 'medium_risk' AND claim_sentiment = 'negative' THEN TRUE
        ELSE FALSE
    END AS high_risk_flag,
    
    -- Extract risk keywords array from the extracted_risks struct
    FILTER(
        ARRAY(
            CASE WHEN extracted_risks.mold_damage IS NOT NULL THEN 'mold_damage' END,
            CASE WHEN extracted_risks.water_damage IS NOT NULL THEN 'water_damage' END,
            CASE WHEN extracted_risks.litigation_risk IS NOT NULL THEN 'litigation_risk' END,
            CASE WHEN extracted_risks.structural_damage IS NOT NULL THEN 'structural_damage' END,
            CASE WHEN extracted_risks.bodily_injury IS NOT NULL THEN 'bodily_injury' END,
            CASE WHEN extracted_risks.fraud_indicator IS NOT NULL THEN 'fraud_indicator' END,
            CASE WHEN extracted_risks.attorney_involvement IS NOT NULL THEN 'attorney_involvement' END
        ),
        x -> x IS NOT NULL
    ) AS risk_keywords,
    
    -- Metadata
    'ai_functions' AS _analysis_method,
    current_timestamp() AS _risk_analyzed_at

FROM ai_analysis;


-- -----------------------------------------------------------------------------
-- Silver Table: AI-Powered Risk Summary
-- Aggregates risk metrics from AI analysis
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_risk_summary_nlp
COMMENT "Risk summary with AI-derived metrics - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
    origin_year,
    loss_type,
    
    -- Claim counts
    COUNT(*) AS total_claims,
    SUM(CASE WHEN high_risk_flag THEN 1 ELSE 0 END) AS high_risk_claims,
    SUM(CASE WHEN risk_category = 'medium_risk' THEN 1 ELSE 0 END) AS medium_risk_claims,
    SUM(CASE WHEN risk_category = 'low_risk' THEN 1 ELSE 0 END) AS low_risk_claims,
    
    -- Risk metrics
    ROUND(AVG(risk_score), 4) AS avg_risk_score,
    
    -- Financial exposure
    SUM(incurred_amount) AS total_incurred,
    SUM(CASE WHEN high_risk_flag THEN incurred_amount ELSE 0 END) AS high_risk_incurred,
    
    -- Sentiment breakdown
    SUM(CASE WHEN claim_sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_sentiment_count,
    SUM(CASE WHEN claim_sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_sentiment_count,
    SUM(CASE WHEN claim_sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_sentiment_count,
    
    -- Percentages
    ROUND(SUM(CASE WHEN high_risk_flag THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS high_risk_pct,
    ROUND(
        SUM(CASE WHEN high_risk_flag THEN incurred_amount ELSE 0 END) * 100.0 / 
        NULLIF(SUM(incurred_amount), 0), 
        2
    ) AS high_risk_incurred_pct,
    
    -- Metadata
    'ai_functions' AS _analysis_method,
    current_timestamp() AS _created_at
    
FROM LIVE.silver_enriched_claims_nlp
GROUP BY origin_year, loss_type
ORDER BY origin_year, loss_type;


-- -----------------------------------------------------------------------------
-- Silver Table: Risk Entity Breakdown
-- Detailed analysis of extracted risk entities
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_risk_entity_breakdown
COMMENT "Breakdown of AI-extracted risk entities by category - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
WITH exploded_risks AS (
    SELECT 
        claim_id,
        origin_year,
        loss_type,
        incurred_amount,
        risk_entity
    FROM LIVE.silver_enriched_claims_nlp
    LATERAL VIEW EXPLODE(risk_keywords) AS risk_entity
    WHERE SIZE(risk_keywords) > 0
)
SELECT 
    risk_entity,
    loss_type,
    
    -- Counts
    COUNT(*) AS occurrence_count,
    COUNT(DISTINCT claim_id) AS affected_claims,
    
    -- Financial exposure
    SUM(incurred_amount) AS total_exposure,
    ROUND(AVG(incurred_amount), 2) AS avg_claim_amount,
    
    -- By origin year breakdown
    COUNT(DISTINCT CASE WHEN origin_year = YEAR(CURRENT_DATE()) THEN claim_id END) AS current_year_claims,
    COUNT(DISTINCT CASE WHEN origin_year = YEAR(CURRENT_DATE()) - 1 THEN claim_id END) AS prior_year_claims,
    
    current_timestamp() AS _created_at
    
FROM exploded_risks
GROUP BY risk_entity, loss_type
ORDER BY total_exposure DESC;


-- -----------------------------------------------------------------------------
-- Silver Table: High Risk Claims Detail
-- Detailed view of high-risk claims for actuarial review
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_high_risk_claims_detail
COMMENT "Detailed view of high-risk claims identified by AI - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
    claim_id,
    policy_id,
    loss_date,
    report_date,
    loss_type,
    loss_description,
    adjuster_notes,
    claim_status,
    state,
    county,
    
    -- Financial
    total_paid,
    current_reserve,
    incurred_amount,
    
    -- Risk analysis results
    risk_category,
    risk_score,
    claim_sentiment,
    risk_keywords,
    
    -- Priority ranking for review
    CASE 
        WHEN risk_score >= 0.9 THEN 'CRITICAL'
        WHEN risk_score >= 0.7 THEN 'HIGH'
        WHEN risk_score >= 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS review_priority,
    
    -- Extracted risk details (for investigation)
    extracted_risks,
    
    -- Days since loss (older high-risk claims = higher concern)
    DATEDIFF(CURRENT_DATE(), loss_date) AS days_since_loss,
    
    _risk_analyzed_at
    
FROM LIVE.silver_enriched_claims_nlp
WHERE high_risk_flag = TRUE
ORDER BY risk_score DESC, incurred_amount DESC;


-- -----------------------------------------------------------------------------
-- Silver Table: Litigation Risk Watch List
-- Claims with attorney involvement or litigation indicators
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE silver_litigation_watch_list
COMMENT "Claims with litigation risk indicators for legal review - Silver Layer"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
    claim_id,
    policy_id,
    claimant_name,
    loss_date,
    loss_type,
    state,
    county,
    
    -- Financial exposure
    total_paid,
    current_reserve,
    incurred_amount,
    
    -- Risk details
    risk_score,
    claim_sentiment,
    risk_keywords,
    
    -- Litigation-specific fields from extracted_risks
    extracted_risks.litigation_risk AS litigation_details,
    extracted_risks.attorney_involvement AS attorney_details,
    
    -- Adjuster notes for review
    adjuster_notes,
    
    current_timestamp() AS _flagged_at
    
FROM LIVE.silver_enriched_claims_nlp
WHERE 
    ARRAY_CONTAINS(risk_keywords, 'litigation_risk')
    OR ARRAY_CONTAINS(risk_keywords, 'attorney_involvement')
ORDER BY incurred_amount DESC;
