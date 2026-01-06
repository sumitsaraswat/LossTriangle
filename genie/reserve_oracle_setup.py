# Databricks notebook source
# MAGIC %md
# MAGIC # 🔮 Reserve Oracle - Genie Space Setup
# MAGIC 
# MAGIC **Your AI-Powered Actuarial Guide to Loss Triangle Insights**
# MAGIC 
# MAGIC This notebook creates and configures the "Reserve Oracle" Genie space
# MAGIC for natural language queries on insurance loss reserves.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration

# COMMAND ----------

# Configuration
WAREHOUSE_ID = "148ccb90800933a1"
WAREHOUSE_PATH = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
CATALOG = "unified_reserves"
SCHEMA = "losstriangle"

GENIE_SPACE_NAME = "Reserve Oracle"
GENIE_SPACE_DESCRIPTION = """🔮 Your AI-Powered Actuarial Guide

Ask me anything about:
• Loss reserves and IBNR projections
• Reserve adequacy and solvency status
• High-risk claims and AI-detected risks
• Development patterns and factors
• Loss triangles by type and origin year

I understand insurance terminology like "mold claims", "litigation risk", 
"the 2018 storm", and "do we have enough reserves?"
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Table Definitions for Genie

# COMMAND ----------

# Tables to include in the Genie space with business-friendly descriptions
GENIE_TABLES = [
    {
        "table": f"{CATALOG}.{SCHEMA}.gold_reserve_estimates",
        "description": """
        📊 **Reserve Estimates by Origin Year**
        
        Contains IBNR (Incurred But Not Reported) reserve calculations for each 
        accident year and loss type using the Chain Ladder method.
        
        Key columns:
        - origin_period: The year when losses occurred (accident year)
        - loss_type: Type of loss (Fire, Water Damage, Wind, Theft, Liability, ALL)
        - latest_cumulative: Amount paid to date
        - ultimate_loss: Projected total cost when fully developed
        - ibnr: Estimated reserves still needed (ultimate - paid)
        - percent_reported: How much has emerged (100% = fully developed)
        """
    },
    {
        "table": f"{CATALOG}.{SCHEMA}.gold_reserve_summary",
        "description": """
        📈 **Executive Reserve Summary**
        
        High-level reserve metrics for the entire portfolio. Perfect for 
        board-level questions and executive reporting.
        
        Key columns:
        - total_paid: Total amount paid on all claims
        - total_ibnr: Total IBNR reserves needed
        - total_ultimate: Total projected ultimate loss
        - held_reserves: Reserves currently held
        - reserve_adequacy: Ratio showing if we have enough (>1.0 = adequate)
        - ai_adjustment_factor: Additional reserve adjustment from AI risk analysis
        """
    },
    {
        "table": f"{CATALOG}.{SCHEMA}.gold_v_reserve_adequacy",
        "description": """
        🎯 **Reserve Adequacy Status**
        
        Shows whether we're holding enough reserves for each loss type.
        
        Status meanings:
        - ADEQUATE (Green): Reserve adequacy >= 100%
        - MARGINAL (Yellow): Reserve adequacy 85-99%
        - DEFICIENT (Red): Reserve adequacy < 85%
        
        Key columns:
        - loss_type: Type of loss
        - reserve_adequacy: Ratio (1.0 = 100% adequate)
        - adequacy_status: ADEQUATE, MARGINAL, or DEFICIENT
        - reserve_margin: Extra reserves above estimate (can be negative)
        """
    },
    {
        "table": f"{CATALOG}.{SCHEMA}.gold_v_risk_exposure",
        "description": """
        ⚠️ **AI-Detected Risk Exposure**
        
        Shows claims flagged as high-risk by AI analysis of adjuster notes.
        AI looks for hidden risks like mold, litigation, and structural damage.
        
        Key columns:
        - origin_year: Accident year
        - loss_type: Type of loss
        - total_claims: Total number of claims
        - high_risk_claims: Claims flagged as high risk
        - high_risk_percentage: % of claims that are high risk
        - high_risk_exposure: Dollar exposure from high-risk claims
        - avg_risk_score: Average AI risk score (0-1, higher = riskier)
        """
    },
    {
        "table": f"{CATALOG}.{SCHEMA}.gold_v_claims_watchlist",
        "description": """
        🔍 **High-Risk Claims Watchlist**
        
        Individual claims that need attention based on AI risk analysis.
        Perfect for claims managers and risk teams.
        
        Key columns:
        - claim_id: Unique claim identifier
        - loss_type: Type of loss
        - risk_score: AI risk score (0-1)
        - risk_category: Why it's flagged (mold, litigation, etc.)
        - incurred_amount: Total incurred on the claim
        - county, state: Location
        - adjuster_notes: Notes that triggered the risk flag
        """
    },
    {
        "table": f"{CATALOG}.{SCHEMA}.gold_v_executive_dashboard",
        "description": """
        🏢 **Executive Dashboard Metrics**
        
        Boardroom-ready metrics with traffic light status indicators.
        
        Key columns:
        - total_paid, total_ibnr, total_ultimate: Key financial metrics
        - reserve_adequacy: Are we adequately reserved?
        - status_indicator: GREEN (good), YELLOW (watch), RED (action needed)
        - risk_adjusted_ibnr: IBNR with AI risk adjustment applied
        """
    },
    {
        "table": f"{CATALOG}.{SCHEMA}.gold_development_factors_python",
        "description": """
        📐 **Development Factors**
        
        Age-to-Age (ATA) and Cumulative Development Factors (CDF) used in 
        Chain Ladder calculations.
        
        Key columns:
        - development_period: Years since accident
        - ata_factor: How much losses grow period-to-period
        - cdf_to_ultimate: Cumulative factor to project ultimate
        - percent_developed: How much has emerged at this age
        - loss_type: Factors vary by loss type
        """
    },
    {
        "table": f"{CATALOG}.{SCHEMA}.silver_triangle_cells",
        "description": """
        📊 **Loss Triangle Data**
        
        The actual loss triangle showing paid losses by origin year and 
        development period.
        
        Key columns:
        - origin_period: Accident year
        - development_period: Years since accident
        - cumulative_value: Total paid up to this point
        - incremental_value: Paid in this specific period
        """
    },
    {
        "table": f"{CATALOG}.{SCHEMA}.silver_enriched_claims_nlp",
        "description": """
        📋 **Enriched Claims with AI Analysis**
        
        All claims with NLP-derived risk scores and categories.
        
        Key columns:
        - claim_id: Unique identifier
        - loss_type, loss_date, county, state: Claim details
        - risk_score: AI-calculated risk (0-1)
        - risk_category: water_damage, litigation, structural, etc.
        - high_risk_flag: TRUE if needs attention
        - adjuster_notes: Original notes analyzed by AI
        """
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: System Instructions

# COMMAND ----------

SYSTEM_INSTRUCTIONS = """
# 🔮 Reserve Oracle - System Instructions

You are **Reserve Oracle**, an AI-powered actuarial assistant specializing in insurance loss reserve analysis. You help actuaries, executives, and claims professionals understand their reserve positions through natural language.

## Your Expertise

You are an expert in:
1. **Loss Reserving** - IBNR, Chain Ladder, Bornhuetter-Ferguson methods
2. **Reserve Adequacy** - Assessing if held reserves are sufficient
3. **Risk Analysis** - Understanding AI-flagged high-risk claims
4. **Loss Triangles** - Development patterns and factors

## Key Terminology

When users say... | They mean...
---|---
"IBNR" or "reserves needed" | `ibnr` or `total_ibnr` column
"ultimate" or "projected total" | `ultimate_loss` or `total_ultimate`
"paid" or "incurred to date" | `latest_cumulative` or `total_paid`
"adequacy" or "enough reserves" | `reserve_adequacy` (1.0 = 100% adequate)
"high risk" or "flagged claims" | `high_risk_flag = TRUE` or `risk_score > 0.5`
"mold claims" | `risk_category = 'water_damage'` or notes contain 'mold'
"litigation" or "lawyer claims" | `risk_category = 'litigation'`
"the 2018 storm" or "hurricane" | `origin_period = 2018` with `catastrophe_code IS NOT NULL`
"origin year" or "accident year" | `origin_period` column
"development period" | Years since loss occurred

## Status Thresholds

- **ADEQUATE** (🟢 GREEN): `reserve_adequacy >= 1.0` (100%+)
- **MARGINAL** (🟡 YELLOW): `reserve_adequacy >= 0.85` (85-99%)
- **DEFICIENT** (🔴 RED): `reserve_adequacy < 0.85` (<85%)

## Loss Types Available

- **ALL** - Grand total across all loss types
- **Fire** - Fire and smoke damage claims
- **Water Damage** - Water damage, flooding, mold-related
- **Wind** - Wind, storm, hurricane damage
- **Theft** - Theft and burglary claims
- **Liability** - Liability claims (slip & fall, lawsuits)

## Best Practices

1. **When asked about totals**, use the `loss_type = 'ALL'` row or aggregate individual types
2. **For trend analysis**, order by `origin_period` or `development_period`
3. **For risk questions**, join with claims watchlist or risk exposure views
4. **For executive questions**, use the executive dashboard or reserve summary tables
5. **Always format currency** with $ and commas (e.g., $1,234,567)
6. **Round percentages** to 1 decimal place

## Sample Query Patterns

### Reserve Questions
- Total IBNR: `SELECT total_ibnr FROM gold_reserve_summary WHERE loss_type = 'ALL'`
- By loss type: `GROUP BY loss_type ORDER BY ibnr DESC`

### Risk Questions  
- High risk claims: `WHERE high_risk_flag = TRUE ORDER BY risk_score DESC`
- Risk exposure: `SELECT loss_type, high_risk_exposure FROM gold_v_risk_exposure`

### Adequacy Questions
- Status: `SELECT loss_type, adequacy_status FROM gold_v_reserve_adequacy`
- Deficient types: `WHERE adequacy_status = 'DEFICIENT'`

## Response Style

- Be concise but thorough
- Use tables for multi-row results
- Highlight concerning metrics (deficient reserves, high risk %)
- Provide context (e.g., "This is 15% higher than last year")
- Suggest follow-up questions when relevant
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Sample Questions

# COMMAND ----------

SAMPLE_QUESTIONS = [
    # Executive Questions
    "What is our total IBNR?",
    "Are we adequately reserved?",
    "Give me the executive summary",
    "What's our reserve status by loss type?",
    "Show me the board dashboard",
    
    # Reserve Analysis
    "What is our total ultimate loss?",
    "Show me IBNR by origin year",
    "Which loss type has the highest reserves?",
    "Compare paid vs ultimate for all loss types",
    "What's our reserve margin?",
    
    # Risk Analysis
    "Show me high risk claims",
    "What percentage of claims are high risk?",
    "Which origin year has the most risk exposure?",
    "List claims with mold risk",
    "Show me litigation claims",
    "What's our high risk exposure by county?",
    
    # Loss Type Analysis
    "How much IBNR for Water Damage?",
    "Show me Fire claims by origin year",
    "Which loss type is most concerning?",
    "Compare Liability vs Theft reserves",
    
    # Development Questions
    "What are the development factors?",
    "How developed is the 2022 year?",
    "Show me the loss triangle",
    "What's the ATA factor for period 1 to 2?",
    
    # Geographic Questions
    "Show me claims by county",
    "Which state has highest exposure?",
    "Miami-Dade high risk claims",
    
    # Trend Analysis
    "How has IBNR changed over time?",
    "2018 hurricane claims exposure",
    "Compare 2020 vs 2021 adequacy"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Genie Space (Manual Steps)
# MAGIC 
# MAGIC Since Genie Spaces are created via UI, follow these steps:
# MAGIC 
# MAGIC ### 1. Navigate to Genie
# MAGIC - Go to **SQL** → **Genie Spaces** in the left sidebar
# MAGIC - Click **Create Genie Space**
# MAGIC 
# MAGIC ### 2. Configure Basic Settings
# MAGIC - **Name**: `Reserve Oracle`
# MAGIC - **Description**: (paste from GENIE_SPACE_DESCRIPTION above)
# MAGIC - **SQL Warehouse**: Select `/sql/1.0/warehouses/148ccb90800933a1`
# MAGIC 
# MAGIC ### 3. Add Tables
# MAGIC Add these tables from `unified_reserves.losstriangle`:
# MAGIC - `gold_reserve_estimates`
# MAGIC - `gold_reserve_summary`
# MAGIC - `gold_v_reserve_adequacy`
# MAGIC - `gold_v_risk_exposure`
# MAGIC - `gold_v_claims_watchlist`
# MAGIC - `gold_v_executive_dashboard`
# MAGIC - `gold_development_factors_python`
# MAGIC - `silver_triangle_cells`
# MAGIC - `silver_enriched_claims_nlp`
# MAGIC 
# MAGIC ### 4. Add System Instructions
# MAGIC - Click on **Instructions** tab
# MAGIC - Paste the SYSTEM_INSTRUCTIONS content above
# MAGIC 
# MAGIC ### 5. Add Sample Questions
# MAGIC - Click **Sample Questions**
# MAGIC - Add questions from SAMPLE_QUESTIONS list

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Print Configuration for Easy Copy/Paste

# COMMAND ----------

print("=" * 80)
print("🔮 RESERVE ORACLE - GENIE SPACE CONFIGURATION")
print("=" * 80)

print("\n📋 GENIE SPACE NAME:")
print("-" * 40)
print(GENIE_SPACE_NAME)

print("\n📝 DESCRIPTION:")
print("-" * 40)
print(GENIE_SPACE_DESCRIPTION)

print("\n🔗 SQL WAREHOUSE:")
print("-" * 40)
print(WAREHOUSE_PATH)

print("\n📊 TABLES TO ADD:")
print("-" * 40)
for t in GENIE_TABLES:
    print(f"  • {t['table']}")

print("\n❓ SAMPLE QUESTIONS:")
print("-" * 40)
for i, q in enumerate(SAMPLE_QUESTIONS[:10], 1):
    print(f"  {i}. {q}")
print(f"  ... and {len(SAMPLE_QUESTIONS) - 10} more")

print("\n" + "=" * 80)
print("Copy the SYSTEM_INSTRUCTIONS from the cell above for the Instructions tab")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎉 Setup Complete!
# MAGIC 
# MAGIC Your **Reserve Oracle** Genie space is ready to answer questions like:
# MAGIC 
# MAGIC - *"What is our total IBNR?"*
# MAGIC - *"Are we adequately reserved for Fire claims?"*
# MAGIC - *"Show me high-risk mold claims in Miami-Dade"*
# MAGIC - *"Which origin year has the most development remaining?"*
# MAGIC - *"Give me the executive summary"*
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC **Pro Tips:**
# MAGIC 1. Start with broad questions, then drill down
# MAGIC 2. Use loss type filters for specific analysis
# MAGIC 3. Ask about "status" to get adequacy indicators
# MAGIC 4. Reference specific years like "2018 storm" or "2022 claims"

