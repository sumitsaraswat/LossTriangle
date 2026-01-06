"""
Metric Definitions
==================

Defines business metrics for AI/BI Genie semantic layer.
These definitions enable natural language queries like:
"What is the reserve adequacy for the 2018 storm?"

Owner: David (Analytics Lead)
"""

from typing import Optional
from pyspark.sql import SparkSession
from loguru import logger


class MetricDefinitions:
    """
    Defines and registers metrics for Genie.
    
    Metrics are defined using Unity Catalog's metric views
    which Genie uses to understand business concepts.
    """
    
    # Core metric definitions
    METRICS = {
        "reserve_adequacy": {
            "name": "Reserve Adequacy",
            "description": "Ratio of held reserves to estimated ultimate loss. Values >= 1.0 indicate adequate reserves.",
            "formula": "held_reserves / estimated_ultimate_loss",
            "unit": "ratio",
            "thresholds": {
                "adequate": 1.0,
                "marginal": 0.85,
                "deficient": 0.0,
            },
        },
        "ibnr": {
            "name": "IBNR (Incurred But Not Reported)",
            "description": "Estimated future payments for claims that have occurred but not yet been reported or fully developed.",
            "formula": "projected_ultimate_loss - cumulative_paid_loss",
            "unit": "currency",
        },
        "loss_ratio": {
            "name": "Loss Ratio",
            "description": "Ratio of incurred losses to earned premium. Lower is better for profitability.",
            "formula": "incurred_loss / earned_premium",
            "unit": "percentage",
            "thresholds": {
                "excellent": 0.5,
                "good": 0.65,
                "poor": 0.8,
            },
        },
        "development_factor": {
            "name": "Development Factor (ATA)",
            "description": "Age-to-Age factor showing how losses develop from one period to the next.",
            "formula": "loss_at_age_t1 / loss_at_age_t",
            "unit": "ratio",
        },
        "reserve_margin": {
            "name": "Reserve Margin",
            "description": "Difference between held reserves and estimated IBNR. Positive means over-reserved.",
            "formula": "held_reserves - estimated_ibnr",
            "unit": "currency",
        },
        "high_risk_exposure": {
            "name": "High Risk Exposure",
            "description": "Total incurred amount for claims flagged as high risk by AI model.",
            "formula": "SUM(incurred_amount) WHERE high_risk_flag = TRUE",
            "unit": "currency",
        },
        "risk_adjusted_ibnr": {
            "name": "Risk-Adjusted IBNR",
            "description": "IBNR estimate adjusted for AI-detected risks. Higher than standard IBNR indicates hidden exposure.",
            "formula": "standard_ibnr * ai_adjustment_factor",
            "unit": "currency",
        },
    }
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "unified_reserves",
        gold_schema: str = "gold",
    ):
        """
        Initialize metric definitions.
        
        Args:
            spark: Active SparkSession
            catalog: Unity Catalog name
            gold_schema: Gold layer schema
        """
        self.spark = spark
        self.catalog = catalog
        self.gold_schema = gold_schema
    
    def create_metric_views(self) -> None:
        """
        Create metric views in Unity Catalog.
        
        These views enable Genie to understand and calculate
        business metrics from natural language queries.
        """
        logger.info("Creating metric views for Genie")
        
        # Reserve Adequacy View
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self.catalog}.{self.gold_schema}.v_reserve_adequacy AS
            SELECT 
                as_of_date,
                loss_type,
                total_ultimate AS estimated_ultimate_loss,
                held_reserves,
                CASE 
                    WHEN total_ultimate > 0 THEN held_reserves / total_ultimate
                    ELSE 1.0
                END AS reserve_adequacy,
                CASE 
                    WHEN held_reserves >= total_ultimate THEN 'ADEQUATE'
                    WHEN held_reserves >= total_ultimate * 0.85 THEN 'MARGINAL'
                    ELSE 'DEFICIENT'
                END AS adequacy_status,
                total_ibnr,
                held_reserves - total_ibnr AS reserve_margin
            FROM {self.catalog}.{self.gold_schema}.reserve_summary
        """)
        
        # IBNR by Origin Year View
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self.catalog}.{self.gold_schema}.v_ibnr_by_origin AS
            SELECT 
                origin_period AS origin_year,
                loss_type,
                triangle_type,
                latest_cumulative AS paid_to_date,
                ultimate_loss AS projected_ultimate,
                ibnr AS estimated_ibnr,
                percent_reported * 100 AS percent_developed,
                _calculated_at AS calculation_date
            FROM {self.catalog}.{self.gold_schema}.reserve_estimates
        """)
        
        # Risk Exposure View
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self.catalog}.{self.gold_schema}.v_risk_exposure AS
            SELECT 
                rs.origin_year,
                rs.loss_type,
                rs.total_claims,
                rs.high_risk_claims,
                rs.high_risk_pct AS high_risk_percentage,
                rs.total_incurred,
                rs.high_risk_incurred AS high_risk_exposure,
                rs.high_risk_incurred_pct AS high_risk_exposure_pct,
                rs.avg_risk_score
            FROM {self.catalog}.silver.risk_summary rs
        """)
        
        # Executive Dashboard View (combines all key metrics)
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW {self.catalog}.{self.gold_schema}.v_executive_dashboard AS
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
                rs.total_ibnr * COALESCE(rs.ai_adjustment_factor, 1.0) AS risk_adjusted_ibnr,
                CASE 
                    WHEN rs.reserve_adequacy >= 1.0 THEN 'GREEN'
                    WHEN rs.reserve_adequacy >= 0.85 THEN 'YELLOW'
                    ELSE 'RED'
                END AS status_indicator
            FROM {self.catalog}.{self.gold_schema}.reserve_summary rs
        """)
        
        logger.info("Metric views created successfully")
    
    def get_metric_definition(self, metric_name: str) -> Optional[dict]:
        """
        Get definition for a specific metric.
        
        Args:
            metric_name: Name of the metric
            
        Returns:
            Metric definition dictionary or None
        """
        return self.METRICS.get(metric_name)
    
    def list_metrics(self) -> list[str]:
        """List all available metrics."""
        return list(self.METRICS.keys())
    
    def generate_genie_instructions(self) -> str:
        """
        Generate Genie instruction text for metric understanding.
        
        This text can be added to Genie space configuration
        to help it understand domain-specific terminology.
        
        Returns:
            Instruction text for Genie
        """
        instructions = """
# Insurance Loss Reserving Domain Guide

## Key Concepts

### Loss Triangle
A loss triangle is a 2D matrix showing how insurance claims develop over time.
- Rows represent "Origin Years" (when claims occurred)
- Columns represent "Development Periods" (how old the claims are)
- Values show cumulative paid or incurred losses

### IBNR (Incurred But Not Reported)
IBNR represents claims that have happened but aren't yet fully reported or paid.
Formula: IBNR = Ultimate Loss - Current Paid Loss

### Reserve Adequacy
Measures if the company has set aside enough money for future claims.
Formula: Reserve Adequacy = Held Reserves / Estimated Ultimate Loss
- >= 1.0: Adequate (enough reserves)
- 0.85-1.0: Marginal (may need attention)
- < 0.85: Deficient (potential solvency risk)

### Chain Ladder Method
An actuarial technique that uses historical patterns to project future claim development.

## Available Metrics

"""
        for name, defn in self.METRICS.items():
            instructions += f"### {defn['name']}\n"
            instructions += f"{defn['description']}\n"
            instructions += f"Formula: `{defn['formula']}`\n\n"
        
        instructions += """
## Common Questions

1. "What is our reserve adequacy?" - Check v_reserve_adequacy view
2. "How much IBNR do we have for 2018?" - Check v_ibnr_by_origin with origin_year filter
3. "What is our exposure for water damage claims?" - Filter by loss_type
4. "Which claims are high risk?" - Check high_risk_flag in enriched_claims
5. "Do we have enough cash for the 2018 storm?" - Compare held_reserves to total_ultimate

## Tables Reference

- `gold.reserve_summary` - Summary by loss type
- `gold.reserve_estimates` - Detailed by origin year
- `silver.enriched_claims` - Claim-level with risk flags
- `silver.risk_summary` - AI risk analysis summary
"""
        return instructions

