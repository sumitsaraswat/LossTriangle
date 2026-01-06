# Databricks notebook source
# MAGIC %md
# MAGIC # 🧞 AI/BI Genie Setup
# MAGIC 
# MAGIC **Owner: David (Analytics Lead)**
# MAGIC 
# MAGIC This notebook configures views for AI/BI Genie natural language queries.
# MAGIC 
# MAGIC **Goal:** Enable executives to ask questions like:
# MAGIC > *"What is our total exposure for Water Damage claims in Miami-Dade county older than 24 months?"*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "unified_reserves"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND ----------

# Set catalog
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✅ Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Metric Views for Genie

# COMMAND ----------

# Reserve Adequacy View
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.v_reserve_adequacy AS
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
    FROM {CATALOG}.{GOLD_SCHEMA}.reserve_summary
""")
print("✅ Created v_reserve_adequacy view")

# COMMAND ----------

# IBNR by Origin Year View
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.v_ibnr_by_origin AS
    SELECT 
        origin_period AS origin_year,
        loss_type,
        triangle_type,
        latest_cumulative AS paid_to_date,
        ultimate_loss AS projected_ultimate,
        ibnr AS estimated_ibnr,
        percent_reported * 100 AS percent_developed,
        _calculated_at AS calculation_date
    FROM {CATALOG}.{GOLD_SCHEMA}.reserve_estimates
""")
print("✅ Created v_ibnr_by_origin view")

# COMMAND ----------

# Risk Exposure View
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.v_risk_exposure AS
    SELECT 
        origin_year,
        loss_type,
        total_claims,
        high_risk_claims,
        high_risk_pct AS high_risk_percentage,
        total_incurred,
        high_risk_incurred AS high_risk_exposure,
        high_risk_incurred_pct AS high_risk_exposure_pct,
        avg_risk_score
    FROM {CATALOG}.{SILVER_SCHEMA}.risk_summary
""")
print("✅ Created v_risk_exposure view")

# COMMAND ----------

# Executive Dashboard View
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOLD_SCHEMA}.v_executive_dashboard AS
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
    FROM {CATALOG}.{GOLD_SCHEMA}.reserve_summary rs
""")
print("✅ Created v_executive_dashboard view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Views

# COMMAND ----------

print("📊 Reserve Adequacy View:")
display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.v_reserve_adequacy"))

# COMMAND ----------

print("📊 IBNR by Origin Year View:")
display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.v_ibnr_by_origin"))

# COMMAND ----------

print("📊 Risk Exposure View:")
display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.v_risk_exposure"))

# COMMAND ----------

print("📊 Executive Dashboard View:")
display(spark.sql(f"SELECT * FROM {CATALOG}.{GOLD_SCHEMA}.v_executive_dashboard"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Genie Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: "What is our total IBNR?"

# COMMAND ----------

display(spark.sql(f"""
    SELECT SUM(total_ibnr) as total_ibnr
    FROM {CATALOG}.{GOLD_SCHEMA}.reserve_summary
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: "Show me reserve adequacy by loss type"

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        loss_type,
        reserve_adequacy,
        adequacy_status,
        held_reserves,
        estimated_ultimate_loss
    FROM {CATALOG}.{GOLD_SCHEMA}.v_reserve_adequacy
    ORDER BY reserve_adequacy ASC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: "What percentage of claims are high risk?"

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        origin_year,
        loss_type,
        total_claims,
        high_risk_claims,
        high_risk_percentage
    FROM {CATALOG}.{GOLD_SCHEMA}.v_risk_exposure
    ORDER BY high_risk_percentage DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4: "Show high risk claims from Miami-Dade"

# COMMAND ----------

display(spark.sql(f"""
    SELECT 
        claim_id,
        loss_date,
        loss_type,
        county,
        adjuster_notes,
        risk_category,
        risk_score,
        incurred_amount
    FROM {CATALOG}.{SILVER_SCHEMA}.enriched_claims
    WHERE county = 'Miami-Dade' AND high_risk_flag = TRUE
    ORDER BY risk_score DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space Configuration Instructions
# MAGIC 
# MAGIC To complete Genie setup:
# MAGIC 
# MAGIC 1. Go to **Databricks Workspace > AI/BI > Genie**
# MAGIC 2. Create a new Genie Space called "Insurance Reserving Assistant"
# MAGIC 3. Add these tables:
# MAGIC    - `unified_reserves.gold.reserve_summary`
# MAGIC    - `unified_reserves.gold.reserve_estimates`
# MAGIC    - `unified_reserves.gold.v_reserve_adequacy`
# MAGIC    - `unified_reserves.gold.v_ibnr_by_origin`
# MAGIC    - `unified_reserves.gold.v_risk_exposure`
# MAGIC    - `unified_reserves.gold.v_executive_dashboard`
# MAGIC    - `unified_reserves.silver.enriched_claims`
# MAGIC    - `unified_reserves.silver.risk_summary`

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Boardroom Question Demo
# MAGIC 
# MAGIC > **CFO asks:** *"What is our total exposure for Water Damage claims in Miami-Dade county older than 24 months? Do we have enough cash?"*

# COMMAND ----------

from pyspark.sql.functions import months_between, current_date, col, sum as spark_sum

# Answer the CFO's question
result = spark.sql(f"""
    SELECT 
        'Water Damage - Miami-Dade (24+ months)' as segment,
        COUNT(*) as claim_count,
        SUM(incurred_amount) as total_exposure,
        SUM(CASE WHEN high_risk_flag THEN incurred_amount ELSE 0 END) as high_risk_exposure,
        AVG(risk_score) as avg_risk_score
    FROM {CATALOG}.{SILVER_SCHEMA}.enriched_claims
    WHERE loss_type = 'Water Damage'
      AND county = 'Miami-Dade'
      AND months_between(current_date(), loss_date) > 24
""")

display(result)

# COMMAND ----------

# Get reserve adequacy
adequacy = spark.sql(f"""
    SELECT reserve_adequacy, adequacy_status, held_reserves, total_ultimate
    FROM {CATALOG}.{GOLD_SCHEMA}.v_reserve_adequacy
    LIMIT 1
""").collect()[0]

print(f"""
📊 ANSWER TO CFO:
================

Reserve Adequacy: {adequacy['reserve_adequacy']:.1%}
Status: {adequacy['adequacy_status']}
Held Reserves: ${adequacy['held_reserves']:,.0f}
Estimated Ultimate: ${adequacy['total_ultimate']:,.0f}

{"✅ We have adequate reserves." if adequacy['adequacy_status'] == 'ADEQUATE' else "⚠️ Reserves may need attention."}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Genie Setup Complete!
# MAGIC 
# MAGIC The semantic layer is configured. Genie can now answer questions about:
# MAGIC - Reserve adequacy
# MAGIC - IBNR by origin year
# MAGIC - High-risk claims
# MAGIC - Loss development patterns
# MAGIC 
# MAGIC **The CFO can now ask questions in plain English and get instant answers!**
