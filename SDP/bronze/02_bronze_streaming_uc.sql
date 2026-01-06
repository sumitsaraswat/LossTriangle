-- Databricks Delta Live Tables (DLT) Pipeline
-- Layer: Bronze (Streaming - Unity Catalog Compatible)
-- Purpose: Raw data ingestion from cloud files with Auto Loader
-- Author: Marcus (Data Engineer)
-- 
-- NOTE: Uses _metadata.file_path instead of input_file_name() for Unity Catalog

-- =============================================================================
-- BRONZE LAYER: Streaming Data Ingestion (Unity Catalog Compatible)
-- Requires data files in Volumes: /Volumes/unified_reserves/bronze/raw/
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Bronze Table: Raw Claims (Streaming)
-- Ingests raw claims data with quality expectations
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE bronze_raw_claims (
    -- Data Quality Expectations (inline)
    CONSTRAINT valid_claim_id EXPECT (claim_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_loss_date EXPECT (loss_date IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_loss_type EXPECT (loss_type IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Raw claims data from source systems - Bronze Layer (Streaming)"
TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.autoOptimize.zOrderCols" = "claim_id"
)
AS SELECT
    claim_id,
    policy_id,
    CAST(loss_date AS DATE) AS loss_date,
    CAST(report_date AS DATE) AS report_date,
    claimant_name,
    loss_type,
    loss_description,
    adjuster_notes,
    claim_status,
    county,
    state,
    zip_code,
    catastrophe_code,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    current_timestamp() AS _ingested_at,
    _metadata.file_path AS _source_file  -- Unity Catalog compatible
FROM cloud_files(
    "/Volumes/unified_reserves/bronze/raw/claims/",
    "csv",
    map(
        "header", "true",
        "inferSchema", "false",
        "cloudFiles.inferColumnTypes", "false"
    )
);


-- -----------------------------------------------------------------------------
-- Bronze Table: Raw Payments (Streaming)
-- Ingests raw payment transactions with quality expectations
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING LIVE TABLE bronze_raw_payments (
    -- Data Quality Expectations (inline)
    CONSTRAINT valid_payment_id EXPECT (payment_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_claim_id EXPECT (claim_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_payment_amount EXPECT (payment_amount >= 0) ON VIOLATION DROP ROW
)
COMMENT "Raw payment transactions - Bronze Layer (Streaming)"
TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.autoOptimize.zOrderCols" = "claim_id,payment_date"
)
AS SELECT
    payment_id,
    claim_id,
    CAST(payment_date AS DATE) AS payment_date,
    payment_type,
    CAST(payment_amount AS DOUBLE) AS payment_amount,
    CAST(cumulative_paid AS DOUBLE) AS cumulative_paid,
    CAST(reserve_amount AS DOUBLE) AS reserve_amount,
    payment_status,
    check_number,
    payee_name,
    expense_type,
    CAST(created_at AS TIMESTAMP) AS created_at,
    current_timestamp() AS _ingested_at,
    _metadata.file_path AS _source_file  -- Unity Catalog compatible
FROM cloud_files(
    "/Volumes/unified_reserves/bronze/raw/payments/",
    "csv",
    map(
        "header", "true",
        "inferSchema", "false"
    )
);







