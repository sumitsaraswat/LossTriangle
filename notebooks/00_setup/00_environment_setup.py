# Databricks notebook source
# MAGIC %md
# MAGIC # 🌀 The Long-Tail Storm - Environment Setup
# MAGIC 
# MAGIC This notebook sets up the Unity Catalog schemas and tables required
# MAGIC for the Loss Triangle Reserving system.
# MAGIC 
# MAGIC **Run this notebook first before executing other notebooks.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration parameters
CATALOG = "unified_reserves"
ENVIRONMENT = "dev"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schemas

# COMMAND ----------

# Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

print(f"✅ Using catalog: {CATALOG}")

# COMMAND ----------

# Create schemas for each layer
schemas = ["bronze", "silver", "gold"]

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"✅ Created schema: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Layer Tables

# COMMAND ----------

# Raw claims table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_claims (
        claim_id STRING NOT NULL COMMENT 'Unique claim identifier',
        policy_id STRING NOT NULL COMMENT 'Policy identifier',
        loss_date DATE NOT NULL COMMENT 'Date of loss occurrence',
        report_date DATE NOT NULL COMMENT 'Date claim was reported',
        claimant_name STRING COMMENT 'Name of claimant',
        loss_type STRING NOT NULL COMMENT 'Type of loss (Water Damage, Wind, Fire, etc.)',
        loss_description STRING COMMENT 'Detailed loss description',
        adjuster_notes STRING COMMENT 'Notes from claims adjuster',
        claim_status STRING NOT NULL COMMENT 'Current claim status',
        county STRING COMMENT 'County of loss location',
        state STRING COMMENT 'State of loss location',
        zip_code STRING COMMENT 'ZIP code of loss location',
        catastrophe_code STRING COMMENT 'CAT code if applicable',
        created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
        updated_at TIMESTAMP NOT NULL COMMENT 'Record update timestamp',
        _ingested_at TIMESTAMP COMMENT 'Ingestion timestamp',
        _source_file STRING COMMENT 'Source file path'
    )
    USING DELTA
    COMMENT 'Raw claims data from source systems - Bronze Layer'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'bronze'
    )
""")

print(f"✅ Created table: {CATALOG}.bronze.raw_claims")

# COMMAND ----------

# Raw payments table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_payments (
        payment_id STRING NOT NULL COMMENT 'Unique payment identifier',
        claim_id STRING NOT NULL COMMENT 'Associated claim ID',
        payment_date DATE NOT NULL COMMENT 'Payment date',
        payment_type STRING NOT NULL COMMENT 'Payment type (Indemnity, Expense)',
        payment_amount DOUBLE NOT NULL COMMENT 'Payment amount',
        cumulative_paid DOUBLE COMMENT 'Cumulative paid to date',
        reserve_amount DOUBLE COMMENT 'Current reserve amount',
        payment_status STRING NOT NULL COMMENT 'Payment status',
        check_number STRING COMMENT 'Check number',
        payee_name STRING COMMENT 'Payee name',
        expense_type STRING COMMENT 'Expense category',
        created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
        _ingested_at TIMESTAMP COMMENT 'Ingestion timestamp',
        _source_file STRING COMMENT 'Source file path'
    )
    USING DELTA
    COMMENT 'Raw payment transactions - Bronze Layer'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'bronze'
    )
""")

print(f"✅ Created table: {CATALOG}.bronze.raw_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Layer Tables

# COMMAND ----------

# Enriched claims with risk flags
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.silver.enriched_claims (
        claim_id STRING NOT NULL,
        policy_id STRING NOT NULL,
        loss_date DATE NOT NULL,
        report_date DATE NOT NULL,
        origin_year INT NOT NULL COMMENT 'Year of loss occurrence',
        origin_period INT NOT NULL COMMENT 'Origin period for triangle',
        loss_type STRING NOT NULL,
        loss_description STRING,
        adjuster_notes STRING,
        claim_status STRING NOT NULL,
        county STRING,
        state STRING,
        catastrophe_code STRING,
        total_paid DOUBLE COMMENT 'Total paid to date',
        current_reserve DOUBLE COMMENT 'Current reserve amount',
        incurred_amount DOUBLE COMMENT 'Total incurred (paid + reserve)',
        risk_category STRING COMMENT 'AI-detected risk category',
        risk_score DOUBLE COMMENT 'AI risk score (0-1)',
        risk_keywords ARRAY<STRING> COMMENT 'Detected risk keywords',
        high_risk_flag BOOLEAN COMMENT 'High risk indicator',
        _processed_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Enriched claims with AI risk analysis - Silver Layer'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'silver'
    )
""")

print(f"✅ Created table: {CATALOG}.silver.enriched_claims")

# COMMAND ----------

# Loss triangles table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.silver.loss_triangles (
        triangle_id STRING NOT NULL COMMENT 'Unique triangle identifier',
        triangle_type STRING NOT NULL COMMENT 'Triangle type (paid, incurred)',
        loss_type STRING COMMENT 'Loss type filter',
        state STRING COMMENT 'State filter',
        county STRING COMMENT 'County filter',
        origin_period INT NOT NULL COMMENT 'Origin period (year)',
        development_period INT NOT NULL COMMENT 'Development period',
        cumulative_value DOUBLE COMMENT 'Cumulative loss value',
        incremental_value DOUBLE COMMENT 'Incremental loss value',
        claim_count INT COMMENT 'Number of claims',
        period_type STRING NOT NULL COMMENT 'Period granularity',
        as_of_date DATE NOT NULL COMMENT 'Evaluation date',
        _created_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (triangle_type, loss_type)
    COMMENT 'Loss triangles in long format - Silver Layer'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'silver'
    )
""")

print(f"✅ Created table: {CATALOG}.silver.loss_triangles")

# COMMAND ----------

# Risk summary table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.silver.risk_summary (
        origin_year INT NOT NULL,
        loss_type STRING NOT NULL,
        total_claims BIGINT,
        high_risk_claims BIGINT,
        avg_risk_score DOUBLE,
        total_incurred DOUBLE,
        high_risk_incurred DOUBLE,
        high_risk_pct DOUBLE,
        high_risk_incurred_pct DOUBLE,
        _created_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Risk analysis summary by origin year - Silver Layer'
    TBLPROPERTIES (
        'quality' = 'silver'
    )
""")

print(f"✅ Created table: {CATALOG}.silver.risk_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Layer Tables

# COMMAND ----------

# Reserve estimates table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.gold.reserve_estimates (
        origin_period INT NOT NULL COMMENT 'Origin period',
        latest_cumulative DOUBLE COMMENT 'Latest cumulative loss',
        ultimate_loss DOUBLE COMMENT 'Projected ultimate loss',
        ibnr DOUBLE COMMENT 'IBNR reserve',
        percent_reported DOUBLE COMMENT 'Percent of ultimate reported',
        triangle_type STRING NOT NULL COMMENT 'Triangle type',
        loss_type STRING COMMENT 'Loss type',
        segment STRING COMMENT 'Additional segmentation',
        tail_factor DOUBLE COMMENT 'Tail factor used',
        _calculated_at TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (triangle_type, loss_type)
    COMMENT 'Chain Ladder reserve estimates - Gold Layer'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'gold'
    )
""")

print(f"✅ Created table: {CATALOG}.gold.reserve_estimates")

# COMMAND ----------

# Reserve summary table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.gold.reserve_summary (
        as_of_date DATE NOT NULL COMMENT 'Evaluation date',
        triangle_type STRING NOT NULL COMMENT 'Triangle type',
        loss_type STRING COMMENT 'Loss type',
        total_paid DOUBLE COMMENT 'Total paid losses',
        total_ibnr DOUBLE COMMENT 'Total IBNR',
        total_ultimate DOUBLE COMMENT 'Total ultimate loss',
        reserve_adequacy DOUBLE COMMENT 'Reserve adequacy ratio',
        held_reserves DOUBLE COMMENT 'Currently held reserves',
        reserve_margin DOUBLE COMMENT 'Margin over IBNR',
        ai_adjustment_factor DOUBLE COMMENT 'AI risk adjustment',
        _calculated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Reserve summary for executive reporting - Gold Layer'
    TBLPROPERTIES (
        'quality' = 'gold'
    )
""")

print(f"✅ Created table: {CATALOG}.gold.reserve_summary")

# COMMAND ----------

# Development factors table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.gold.development_factors (
        triangle_type STRING NOT NULL,
        loss_type STRING,
        development_period INT NOT NULL,
        ata_factor DOUBLE COMMENT 'Age-to-Age factor',
        cdf_to_ultimate DOUBLE COMMENT 'CDF to ultimate',
        _calculated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Development factors by triangle - Gold Layer'
    TBLPROPERTIES (
        'quality' = 'gold'
    )
""")

print(f"✅ Created table: {CATALOG}.gold.development_factors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# List all tables
print("\n📋 Tables created in each schema:\n")

for schema in schemas:
    print(f"\n{schema.upper()} Layer:")
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    for table in tables:
        print(f"  ✓ {table.tableName}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC 
# MAGIC All tables have been created. You can now proceed to:
# MAGIC 1. **01_bronze/01_data_ingestion** - Load sample data
# MAGIC 2. **02_silver/02_triangle_builder** - Build loss triangles
# MAGIC 3. **02_silver/03_risk_detection** - Run NLP risk analysis
# MAGIC 4. **03_gold/04_chain_ladder** - Calculate reserves
# MAGIC 5. **04_analytics/05_genie_setup** - Configure Genie

