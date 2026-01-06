# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Silver Layer: AI Risk Detection
# MAGIC 
# MAGIC **Owner: Anya (AI Engineer)**
# MAGIC 
# MAGIC This notebook runs NLP analysis on adjuster notes to detect hidden risks.
# MAGIC The "Early Warning System" flags claims with potential long-tail exposure.
# MAGIC 
# MAGIC **Key Risk Categories:**
# MAGIC - 💧 Water Damage / Mold (highest long-tail risk)
# MAGIC - ⚖️ Litigation (expensive attorney involvement)
# MAGIC - 🏗️ Structural Issues
# MAGIC - 🚨 Fraud Indicators

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "unified_reserves"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------

# Set catalog
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✅ Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Risk Keywords Definition

# COMMAND ----------

import re
from pyspark.sql.functions import (
    col, udf, lower, when, lit, 
    sum as spark_sum, count, avg,
    coalesce, year, current_timestamp
)
from pyspark.sql.types import ArrayType, StringType, DoubleType

# Risk keyword categories with severity weights
RISK_KEYWORDS = {
    # Water/Mold risks (HIGH - often lead to long-tail claims)
    "mold": {"category": "water_damage", "severity": 0.9},
    "black mold": {"category": "water_damage", "severity": 1.0},
    "toxic mold": {"category": "water_damage", "severity": 1.0},
    "seepage": {"category": "water_damage", "severity": 0.7},
    "leak": {"category": "water_damage", "severity": 0.5},
    "water damage": {"category": "water_damage", "severity": 0.6},
    "flooding": {"category": "water_damage", "severity": 0.6},
    "moisture": {"category": "water_damage", "severity": 0.4},
    "damp": {"category": "water_damage", "severity": 0.4},
    
    # Legal/Litigation risks (HIGH - expensive)
    "attorney": {"category": "litigation", "severity": 0.9},
    "lawyer": {"category": "litigation", "severity": 0.9},
    "lawsuit": {"category": "litigation", "severity": 1.0},
    "litigation": {"category": "litigation", "severity": 1.0},
    "legal": {"category": "litigation", "severity": 0.7},
    "sue": {"category": "litigation", "severity": 0.8},
    
    # Structural risks (MEDIUM-HIGH)
    "structural": {"category": "structural", "severity": 0.7},
    "foundation": {"category": "structural", "severity": 0.8},
    "collapse": {"category": "structural", "severity": 0.9},
    "crack": {"category": "structural", "severity": 0.5},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Risk Detection Functions

# COMMAND ----------

def extract_keywords(text):
    """Extract risk keywords from text."""
    if not text:
        return []
    text_lower = text.lower()
    matches = []
    for keyword in RISK_KEYWORDS.keys():
        pattern = re.compile(rf'\b{re.escape(keyword)}\b', re.IGNORECASE)
        if pattern.search(text_lower):
            matches.append(keyword)
    return matches

def calculate_risk_score(text):
    """Calculate risk score from text."""
    if not text:
        return 0.0
    keywords = extract_keywords(text)
    if not keywords:
        return 0.0
    total_severity = sum(RISK_KEYWORDS[kw]["severity"] for kw in keywords)
    return min(1.0, total_severity / 3.0)

def get_risk_category(text):
    """Get primary risk category from text."""
    if not text:
        return "low_risk"
    keywords = extract_keywords(text)
    if not keywords:
        return "low_risk"
    category_scores = {}
    for kw in keywords:
        cat = RISK_KEYWORDS[kw]["category"]
        sev = RISK_KEYWORDS[kw]["severity"]
        category_scores[cat] = max(category_scores.get(cat, 0), sev)
    if not category_scores:
        return "low_risk"
    return max(category_scores.items(), key=lambda x: x[1])[0]

# Register UDFs
extract_keywords_udf = udf(extract_keywords, ArrayType(StringType()))
risk_score_udf = udf(calculate_risk_score, DoubleType())
risk_category_udf = udf(get_risk_category, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Enrich Claims Data

# COMMAND ----------

# Load claims from Bronze
claims_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_claims")
payments_df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_payments")

# Get latest payment per claim
from pyspark.sql.functions import max as spark_max

latest_payments = payments_df.groupBy("claim_id").agg(
    spark_max("payment_date").alias("latest_payment_date"),
    spark_sum("payment_amount").alias("total_paid"),
    spark_max("reserve_amount").alias("current_reserve"),
)

# Join
enriched_claims = claims_df.join(latest_payments, on="claim_id", how="left")

# Add calculated fields
enriched_claims = enriched_claims \
    .withColumn("origin_year", year(col("loss_date"))) \
    .withColumn("origin_period", year(col("loss_date"))) \
    .withColumn(
        "incurred_amount",
        coalesce(col("total_paid"), lit(0)) + coalesce(col("current_reserve"), lit(0))
    )

print(f"Loaded {enriched_claims.count()} claims for analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply Risk Detection

# COMMAND ----------

# Combine text fields for analysis
from pyspark.sql.functions import concat_ws

analyzed_claims = enriched_claims.withColumn(
    "_combined_text",
    concat_ws(" ", col("adjuster_notes"), col("loss_description"))
)

# Apply risk detection
analyzed_claims = analyzed_claims \
    .withColumn("risk_keywords", extract_keywords_udf(col("_combined_text"))) \
    .withColumn("risk_score", risk_score_udf(col("_combined_text"))) \
    .withColumn("risk_category", risk_category_udf(col("_combined_text"))) \
    .withColumn("high_risk_flag", when(col("risk_score") >= 0.5, True).otherwise(False)) \
    .drop("_combined_text")

display(analyzed_claims.select(
    "claim_id", "loss_type", "adjuster_notes", 
    "risk_keywords", "risk_score", "risk_category", "high_risk_flag"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyze Risk Distribution

# COMMAND ----------

# Risk by category
print("🔍 Risk Distribution by Category:")
risk_by_category = analyzed_claims \
    .filter(col("risk_category") != "low_risk") \
    .groupBy("risk_category") \
    .agg(
        count("*").alias("claim_count"),
        spark_sum("incurred_amount").alias("total_exposure"),
    ) \
    .orderBy(col("total_exposure").desc())

display(risk_by_category)

# COMMAND ----------

# High risk claims detail
print("🚨 High Risk Claims:")
high_risk = analyzed_claims.filter(col("high_risk_flag") == True)
display(high_risk.select(
    "claim_id", "loss_date", "loss_type", "county",
    "adjuster_notes", "risk_category", "risk_score", "incurred_amount"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Risk Summary

# COMMAND ----------

# Risk summary by origin year and loss type
risk_summary = analyzed_claims.groupBy("origin_year", "loss_type").agg(
    count("*").alias("total_claims"),
    spark_sum(when(col("high_risk_flag"), 1).otherwise(0)).alias("high_risk_claims"),
    avg("risk_score").alias("avg_risk_score"),
    spark_sum("incurred_amount").alias("total_incurred"),
    spark_sum(when(col("high_risk_flag"), col("incurred_amount")).otherwise(0)).alias("high_risk_incurred"),
)

# Calculate percentages
risk_summary = risk_summary \
    .withColumn("high_risk_pct", col("high_risk_claims") / col("total_claims") * 100) \
    .withColumn("high_risk_incurred_pct", col("high_risk_incurred") / col("total_incurred") * 100)

print("📊 Risk Summary by Origin Year:")
display(risk_summary.orderBy("origin_year", "loss_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to Silver Layer

# COMMAND ----------

# Save enriched claims with risk flags
analyzed_claims_to_save = analyzed_claims.withColumn("_processed_at", current_timestamp())

analyzed_claims_to_save.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.enriched_claims")

print(f"✅ Saved enriched claims to {CATALOG}.{SILVER_SCHEMA}.enriched_claims")

# COMMAND ----------

# Save risk summary
risk_summary_to_save = risk_summary.withColumn("_created_at", current_timestamp())

risk_summary_to_save.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.risk_summary")

print(f"✅ Saved risk summary to {CATALOG}.{SILVER_SCHEMA}.risk_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Findings Summary

# COMMAND ----------

# Summary statistics
total_claims = analyzed_claims.count()
high_risk_count = analyzed_claims.filter(col("high_risk_flag")).count()
high_risk_pct = high_risk_count / total_claims * 100 if total_claims > 0 else 0

total_incurred = analyzed_claims.agg(spark_sum("incurred_amount")).collect()[0][0] or 0
high_risk_incurred = analyzed_claims.filter(col("high_risk_flag")).agg(spark_sum("incurred_amount")).collect()[0][0] or 0

print(f"""
📊 RISK ANALYSIS SUMMARY
========================

Total Claims Analyzed: {total_claims}
High Risk Claims: {high_risk_count} ({high_risk_pct:.1f}%)

Total Incurred: ${total_incurred:,.0f}
High Risk Incurred: ${high_risk_incurred:,.0f} ({high_risk_incurred/total_incurred*100 if total_incurred > 0 else 0:.1f}%)

⚠️  KEY INSIGHT: {high_risk_pct:.1f}% of claims represent potentially 
    {high_risk_incurred/total_incurred*100 if total_incurred > 0 else 0:.1f}% of ultimate exposure due to
    mold, litigation, or structural risk indicators.
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Risk Detection Complete!
# MAGIC 
# MAGIC Claims have been analyzed and flagged for risk. Proceed to:
# MAGIC - **03_gold/04_chain_ladder** - Calculate reserves with AI adjustment
