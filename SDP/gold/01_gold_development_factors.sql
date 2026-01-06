-- Databricks Delta Live Tables (DLT) Pipeline
-- Layer: Gold
-- Purpose: Development Factor Calculations (SQL portion)
-- Author: Sarah (Actuarial Data Scientist)

-- =============================================================================
-- GOLD LAYER: Development Factor Analysis
-- Age-to-Age (ATA) factors calculated using SQL
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Gold Table: Age-to-Age Factor Base
-- Calculate individual link ratios for each origin-development pair
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_ata_factor_base
COMMENT "Individual Age-to-Age ratios by origin period - Gold Layer"
TBLPROPERTIES ("quality" = "gold")
AS 
WITH triangle_with_next AS (
    SELECT 
        t1.origin_period,
        t1.development_period,
        t1.cumulative_value AS current_value,
        t2.cumulative_value AS next_value,
        CASE 
            WHEN t1.cumulative_value > 0 
            THEN t2.cumulative_value / t1.cumulative_value 
            ELSE NULL 
        END AS individual_ata
    FROM LIVE.silver_triangle_cells t1
    LEFT JOIN LIVE.silver_triangle_cells t2 
        ON t1.origin_period = t2.origin_period 
        AND t1.development_period = t2.development_period - 1
        AND t1.triangle_type = t2.triangle_type
        AND t1.loss_type = t2.loss_type
    WHERE t1.triangle_type = 'paid' 
      AND t1.loss_type = 'ALL'
      AND t1.cumulative_value IS NOT NULL
)
SELECT * FROM triangle_with_next
ORDER BY origin_period, development_period;


-- -----------------------------------------------------------------------------
-- Gold Table: Volume-Weighted ATA Factors
-- Standard Chain Ladder ATA factor calculation
-- f_j = Σ C_{i,j+1} / Σ C_{i,j}
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_ata_factors
COMMENT "Volume-weighted Age-to-Age development factors - Gold Layer"
TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
    development_period,
    
    -- Volume-weighted ATA factor (Chain Ladder method)
    SUM(next_value) / SUM(current_value) AS ata_factor,
    
    -- Simple average (for comparison)
    AVG(individual_ata) AS ata_simple_avg,
    
    -- Statistics for diagnostics
    COUNT(*) AS n_observations,
    MIN(individual_ata) AS ata_min,
    MAX(individual_ata) AS ata_max,
    STDDEV(individual_ata) AS ata_stddev,
    
    -- Metadata
    'paid' AS triangle_type,
    'ALL' AS loss_type,
    CURRENT_TIMESTAMP() AS _calculated_at
    
FROM LIVE.gold_ata_factor_base
WHERE individual_ata IS NOT NULL
  AND current_value > 0
GROUP BY development_period
ORDER BY development_period;


-- -----------------------------------------------------------------------------
-- Gold Table: Cumulative Development Factors
-- CDF = product of all remaining ATA factors
-- -----------------------------------------------------------------------------
CREATE OR REFRESH LIVE TABLE gold_cdf_factors
COMMENT "Cumulative Development Factors to ultimate - Gold Layer"
TBLPROPERTIES ("quality" = "gold")
AS
WITH ata_ordered AS (
    SELECT 
        development_period,
        ata_factor,
        ROW_NUMBER() OVER (ORDER BY development_period DESC) as reverse_order
    FROM LIVE.gold_ata_factors
),
-- Calculate cumulative product from the end
cdf_calc AS (
    SELECT 
        development_period,
        ata_factor,
        -- For SQL, we use EXP(SUM(LOG(x))) to calculate product
        EXP(SUM(LOG(ata_factor)) OVER (
            ORDER BY development_period ASC 
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )) AS cdf_to_ultimate
    FROM ata_ordered
)
SELECT 
    development_period,
    ata_factor,
    cdf_to_ultimate,
    ROUND(1.0 / cdf_to_ultimate * 100, 2) AS percent_developed,
    'paid' AS triangle_type,
    'ALL' AS loss_type,
    CURRENT_TIMESTAMP() AS _calculated_at
FROM cdf_calc
ORDER BY development_period;







