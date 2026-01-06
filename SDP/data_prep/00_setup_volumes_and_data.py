# Databricks notebook source
# MAGIC %md
# MAGIC # Data Preparation: Volume Setup & Sample Data Upload
# MAGIC 
# MAGIC **Purpose**: Creates Unity Catalog volumes and uploads sample data for the Loss Triangle DLT pipeline.
# MAGIC 
# MAGIC **Run this notebook ONCE before running the DLT pipeline.**
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configuration

# COMMAND ----------

# Configuration
CATALOG = "unified_reserves"

# Schema names
SCHEMAS = ["bronze", "silver", "gold"]

# Volume definitions: (schema, volume_name, description)
VOLUMES = [
    ("bronze", "raw", "Raw source files for ingestion"),
    ("bronze", "checkpoints", "Streaming checkpoints"),
    ("silver", "processed", "Processed intermediate data"),
    ("gold", "curated", "Curated business-ready data"),
    ("gold", "models", "ML models and artifacts"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schemas (if not exist)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog if not exists (requires admin privileges)
# MAGIC -- CREATE CATALOG IF NOT EXISTS unified_reserves;

# MAGIC -- Use the catalog
# MAGIC USE CATALOG unified_reserves;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS unified_reserves.bronze
# MAGIC COMMENT 'Bronze layer - raw data ingestion';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS unified_reserves.silver
# MAGIC COMMENT 'Silver layer - cleansed and enriched data';
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS unified_reserves.gold
# MAGIC COMMENT 'Gold layer - business-ready aggregates and analytics';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Volumes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze Volumes
# MAGIC CREATE VOLUME IF NOT EXISTS unified_reserves.bronze.raw
# MAGIC COMMENT 'Raw source files for ingestion';
# MAGIC 
# MAGIC CREATE VOLUME IF NOT EXISTS unified_reserves.bronze.checkpoints
# MAGIC COMMENT 'Streaming checkpoints for Auto Loader';
# MAGIC 
# MAGIC -- Silver Volumes
# MAGIC CREATE VOLUME IF NOT EXISTS unified_reserves.silver.processed
# MAGIC COMMENT 'Processed intermediate data files';
# MAGIC 
# MAGIC -- Gold Volumes
# MAGIC CREATE VOLUME IF NOT EXISTS unified_reserves.gold.curated
# MAGIC COMMENT 'Curated business-ready data exports';
# MAGIC 
# MAGIC CREATE VOLUME IF NOT EXISTS unified_reserves.gold.models
# MAGIC COMMENT 'ML models and artifacts';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Directory Structure in Volumes

# COMMAND ----------

import os

# Create subdirectories in the raw volume
raw_volume_path = f"/Volumes/{CATALOG}/bronze/raw"

subdirs = ["claims", "payments", "policies", "catastrophes"]

for subdir in subdirs:
    path = f"{raw_volume_path}/{subdir}"
    dbutils.fs.mkdirs(path)
    print(f"✓ Created directory: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Upload Sample Data
# MAGIC 
# MAGIC Embedding sample data directly to avoid file dependencies.

# COMMAND ----------

# Sample Claims Data
sample_claims_csv = """claim_id,policy_id,loss_date,report_date,claimant_name,loss_type,loss_description,adjuster_notes,claim_status,county,state,zip_code,catastrophe_code,created_at,updated_at
CLM-2020-001,POL-1001,2020-01-15,2020-01-18,John Smith,Wind,Roof damage from windstorm,Initial inspection complete. Shingles missing.,Open,Miami-Dade,FL,33101,CAT-2020-001,2020-01-18 09:00:00,2020-03-15 14:30:00
CLM-2020-002,POL-1002,2020-02-20,2020-02-22,Jane Doe,Flood,Basement flooding,Water damage to foundation. Potential structural issues.,Open,Harris,TX,77001,CAT-2020-002,2020-02-22 10:15:00,2020-04-10 11:00:00
CLM-2020-003,POL-1003,2020-03-10,2020-03-12,Bob Wilson,Fire,Kitchen fire,Significant smoke damage. Appliances destroyed.,Closed,Los Angeles,CA,90001,,2020-03-12 08:30:00,2020-06-01 16:45:00
CLM-2020-004,POL-1004,2020-04-05,2020-04-08,Alice Brown,Hail,Vehicle hail damage,Multiple dents on hood and roof. Windshield cracked.,Closed,Denver,CO,80201,CAT-2020-003,2020-04-08 13:00:00,2020-05-20 09:30:00
CLM-2020-005,POL-1005,2020-05-18,2020-05-20,Charlie Davis,Wind,Fence destroyed,Complete fence replacement needed. Neighbor dispute possible.,Open,Maricopa,AZ,85001,,2020-05-20 11:45:00,2020-07-15 10:00:00
CLM-2021-001,POL-1006,2021-01-10,2021-01-12,David Lee,Flood,Ground floor flooding,Extensive water damage. Mold risk high. Attorney involved.,Open,Orleans,LA,70112,CAT-2021-001,2021-01-12 09:30:00,2021-04-01 15:00:00
CLM-2021-002,POL-1007,2021-02-25,2021-02-27,Emma White,Fire,Garage fire,Total loss of structure. Investigation ongoing for fraud.,Closed,Cook,IL,60601,,2021-02-27 14:00:00,2021-05-15 11:30:00
CLM-2021-003,POL-1008,2021-03-15,2021-03-18,Frank Miller,Wind,Siding damage,Partial siding replacement. Pre-existing damage noted.,Closed,King,WA,98101,CAT-2021-002,2021-03-18 10:00:00,2021-04-30 16:00:00
CLM-2021-004,POL-1009,2021-04-20,2021-04-22,Grace Taylor,Hail,Roof and gutter damage,Comprehensive roof replacement recommended. Litigation pending.,Open,Dallas,TX,75201,CAT-2021-003,2021-04-22 08:45:00,2021-07-10 14:15:00
CLM-2021-005,POL-1010,2021-05-30,2021-06-02,Henry Anderson,Flood,Sewage backup,Health hazard. Professional remediation required.,Closed,Hillsborough,FL,33601,,2021-06-02 12:30:00,2021-08-01 10:45:00
CLM-2022-001,POL-1011,2022-01-05,2022-01-08,Ivy Martinez,Wind,Tree fell on house,Major structural damage. Temporary housing needed.,Open,Fulton,GA,30301,CAT-2022-001,2022-01-08 09:15:00,2022-03-20 13:00:00
CLM-2022-002,POL-1012,2022-02-14,2022-02-16,Jack Robinson,Fire,Electrical fire,Origin in breaker panel. Subrogation potential.,Closed,Clark,NV,89101,,2022-02-16 11:00:00,2022-05-01 15:30:00
CLM-2022-003,POL-1013,2022-03-22,2022-03-25,Karen Thompson,Flood,River overflow,First floor completely flooded. Contents total loss.,Open,Hamilton,OH,45201,CAT-2022-002,2022-03-25 10:30:00,2022-06-15 09:00:00
CLM-2022-004,POL-1014,2022-04-18,2022-04-20,Leo Garcia,Hail,Skylight damage,Multiple skylights shattered. Interior water damage.,Closed,Arapahoe,CO,80112,CAT-2022-003,2022-04-20 14:45:00,2022-06-01 11:15:00
CLM-2022-005,POL-1015,2022-05-25,2022-05-28,Mia Hernandez,Wind,Pool enclosure destroyed,Complete rebuild required. Permit issues.,Open,Broward,FL,33301,,2022-05-28 08:00:00,2022-08-10 16:30:00
CLM-2023-001,POL-1016,2023-01-12,2023-01-15,Noah Clark,Flood,Flash flood damage,Vehicle and garage contents destroyed. Bad faith claim possible.,Open,Bernalillo,NM,87101,CAT-2023-001,2023-01-15 09:45:00,2023-04-01 14:00:00
CLM-2023-002,POL-1017,2023-02-28,2023-03-02,Olivia Lewis,Fire,Chimney fire,Smoke damage throughout. HVAC system compromised.,Closed,Suffolk,MA,02101,,2023-03-02 13:15:00,2023-05-20 10:30:00
CLM-2023-003,POL-1018,2023-03-18,2023-03-20,Paul Walker,Wind,Tornado damage,Total loss. Catastrophe designation pending.,Open,Moore,OK,73160,CAT-2023-002,2023-03-20 07:30:00,2023-06-01 15:45:00
CLM-2023-004,POL-1019,2023-04-10,2023-04-12,Quinn Hall,Hail,Solar panel damage,Renewable energy system destroyed. Special assessment needed.,Closed,Travis,TX,78701,CAT-2023-003,2023-04-12 11:30:00,2023-06-15 09:00:00
CLM-2023-005,POL-1020,2023-05-05,2023-05-08,Rachel Young,Flood,Storm surge,Coastal property. Complete ground floor renovation. Attorney representation.,Open,Charleston,SC,29401,CAT-2023-004,2023-05-08 10:00:00,2023-08-01 14:30:00"""

# Write claims CSV to volume
claims_path = f"/Volumes/{CATALOG}/bronze/raw/claims/sample_claims.csv"
dbutils.fs.put(claims_path, sample_claims_csv, overwrite=True)
print(f"✓ Uploaded sample claims data to: {claims_path}")

# COMMAND ----------

# Sample Payments Data
sample_payments_csv = """payment_id,claim_id,payment_date,payment_type,payment_amount,cumulative_paid,reserve_amount,payment_status,check_number,payee_name,expense_type,created_at
PAY-2020-001,CLM-2020-001,2020-02-15,Partial,5000.00,5000.00,25000.00,Cleared,CHK-10001,ABC Roofing Co,Indemnity,2020-02-15 10:00:00
PAY-2020-002,CLM-2020-001,2020-03-01,Partial,8000.00,13000.00,17000.00,Cleared,CHK-10002,ABC Roofing Co,Indemnity,2020-03-01 11:30:00
PAY-2020-003,CLM-2020-002,2020-03-10,Partial,15000.00,15000.00,85000.00,Cleared,CHK-10003,FloodFix Inc,Indemnity,2020-03-10 09:15:00
PAY-2020-004,CLM-2020-002,2020-04-01,Expense,2500.00,17500.00,82500.00,Cleared,CHK-10004,Smith Engineering,LAE,2020-04-01 14:00:00
PAY-2020-005,CLM-2020-003,2020-04-15,Partial,20000.00,20000.00,30000.00,Cleared,CHK-10005,FireRestore LLC,Indemnity,2020-04-15 10:45:00
PAY-2020-006,CLM-2020-003,2020-05-20,Final,28500.00,48500.00,0.00,Cleared,CHK-10006,FireRestore LLC,Indemnity,2020-05-20 15:30:00
PAY-2020-007,CLM-2020-004,2020-05-01,Partial,3500.00,3500.00,4500.00,Cleared,CHK-10007,AutoBody Plus,Indemnity,2020-05-01 11:00:00
PAY-2020-008,CLM-2020-004,2020-05-15,Final,4200.00,7700.00,0.00,Cleared,CHK-10008,AutoBody Plus,Indemnity,2020-05-15 09:30:00
PAY-2020-009,CLM-2020-005,2020-06-10,Partial,2000.00,2000.00,6000.00,Cleared,CHK-10009,Fence Masters,Indemnity,2020-06-10 13:15:00
PAY-2021-001,CLM-2021-001,2021-02-20,Partial,25000.00,25000.00,175000.00,Cleared,CHK-10010,WaterDamage Pro,Indemnity,2021-02-20 10:00:00
PAY-2021-002,CLM-2021-001,2021-03-15,Expense,5000.00,30000.00,170000.00,Cleared,CHK-10011,Legal Associates,Defense,2021-03-15 14:30:00
PAY-2021-003,CLM-2021-002,2021-04-01,Partial,45000.00,45000.00,55000.00,Cleared,CHK-10012,BuildRight Construction,Indemnity,2021-04-01 09:00:00
PAY-2021-004,CLM-2021-002,2021-05-01,Final,52000.00,97000.00,0.00,Cleared,CHK-10013,BuildRight Construction,Indemnity,2021-05-01 11:45:00
PAY-2021-005,CLM-2021-003,2021-04-10,Final,12000.00,12000.00,0.00,Cleared,CHK-10014,SidingPros Inc,Indemnity,2021-04-10 10:30:00
PAY-2021-006,CLM-2021-004,2021-06-01,Partial,18000.00,18000.00,82000.00,Cleared,CHK-10015,RoofMasters TX,Indemnity,2021-06-01 15:00:00
PAY-2021-007,CLM-2021-004,2021-06-20,Expense,8000.00,26000.00,74000.00,Cleared,CHK-10016,Johnson & Associates,Defense,2021-06-20 09:30:00
PAY-2021-008,CLM-2021-005,2021-07-15,Final,35000.00,35000.00,0.00,Cleared,CHK-10017,CleanTech Services,Indemnity,2021-07-15 14:00:00
PAY-2022-001,CLM-2022-001,2022-02-10,Partial,40000.00,40000.00,160000.00,Cleared,CHK-10018,HomeRestore Atlanta,Indemnity,2022-02-10 10:15:00
PAY-2022-002,CLM-2022-001,2022-03-01,Expense,3500.00,43500.00,156500.00,Cleared,CHK-10019,Temp Housing Inc,LAE,2022-03-01 11:00:00
PAY-2022-003,CLM-2022-002,2022-03-20,Partial,28000.00,28000.00,22000.00,Cleared,CHK-10020,ElectriFix Nevada,Indemnity,2022-03-20 09:45:00
PAY-2022-004,CLM-2022-002,2022-04-15,Final,21500.00,49500.00,0.00,Cleared,CHK-10021,ElectriFix Nevada,Indemnity,2022-04-15 14:30:00
PAY-2022-005,CLM-2022-003,2022-05-01,Partial,50000.00,50000.00,200000.00,Cleared,CHK-10022,FloodCare Ohio,Indemnity,2022-05-01 10:00:00
PAY-2022-006,CLM-2022-003,2022-06-01,Partial,45000.00,95000.00,155000.00,Cleared,CHK-10023,FloodCare Ohio,Indemnity,2022-06-01 11:30:00
PAY-2022-007,CLM-2022-004,2022-05-15,Final,15000.00,15000.00,0.00,Cleared,CHK-10024,SkyLight Specialists,Indemnity,2022-05-15 13:00:00
PAY-2022-008,CLM-2022-005,2022-07-01,Partial,22000.00,22000.00,58000.00,Cleared,CHK-10025,PoolPro Florida,Indemnity,2022-07-01 09:15:00
PAY-2023-001,CLM-2023-001,2023-02-15,Partial,18000.00,18000.00,82000.00,Cleared,CHK-10026,FloodFix NM,Indemnity,2023-02-15 10:30:00
PAY-2023-002,CLM-2023-001,2023-03-20,Expense,4000.00,22000.00,78000.00,Cleared,CHK-10027,Claims Consultants,LAE,2023-03-20 14:45:00
PAY-2023-003,CLM-2023-002,2023-04-10,Partial,25000.00,25000.00,35000.00,Cleared,CHK-10028,HVAC Solutions Boston,Indemnity,2023-04-10 11:00:00
PAY-2023-004,CLM-2023-002,2023-05-15,Final,33000.00,58000.00,0.00,Cleared,CHK-10029,HVAC Solutions Boston,Indemnity,2023-05-15 09:30:00
PAY-2023-005,CLM-2023-003,2023-05-01,Partial,75000.00,75000.00,425000.00,Cleared,CHK-10030,DisasterRecovery OK,Indemnity,2023-05-01 08:00:00
PAY-2023-006,CLM-2023-003,2023-05-20,Expense,12000.00,87000.00,413000.00,Cleared,CHK-10031,Engineering Experts,LAE,2023-05-20 15:15:00
PAY-2023-007,CLM-2023-004,2023-05-25,Final,42000.00,42000.00,0.00,Cleared,CHK-10032,SolarFix Texas,Indemnity,2023-05-25 10:45:00
PAY-2023-008,CLM-2023-005,2023-06-15,Partial,60000.00,60000.00,240000.00,Cleared,CHK-10033,CoastalRestore SC,Indemnity,2023-06-15 11:30:00
PAY-2023-009,CLM-2023-005,2023-07-01,Expense,15000.00,75000.00,225000.00,Cleared,CHK-10034,Maritime Law Group,Defense,2023-07-01 14:00:00"""

# Write payments CSV to volume
payments_path = f"/Volumes/{CATALOG}/bronze/raw/payments/sample_payments.csv"
dbutils.fs.put(payments_path, sample_payments_csv, overwrite=True)
print(f"✓ Uploaded sample payments data to: {payments_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Verify Setup

# COMMAND ----------

# List all volumes created
print("=" * 60)
print("VOLUMES CREATED:")
print("=" * 60)

for schema in SCHEMAS:
    print(f"\n📁 Schema: {CATALOG}.{schema}")
    try:
        volumes = spark.sql(f"SHOW VOLUMES IN {CATALOG}.{schema}").collect()
        for vol in volumes:
            print(f"   └── Volume: {vol.volume_name}")
    except Exception as e:
        print(f"   └── No volumes or error: {e}")

# COMMAND ----------

# Verify files uploaded
print("=" * 60)
print("FILES IN VOLUMES:")
print("=" * 60)

raw_volume = f"/Volumes/{CATALOG}/bronze/raw"
for subdir in ["claims", "payments"]:
    path = f"{raw_volume}/{subdir}"
    print(f"\n📂 {path}:")
    try:
        files = dbutils.fs.ls(path)
        for f in files:
            print(f"   └── {f.name} ({f.size:,} bytes)")
    except Exception as e:
        print(f"   └── Empty or error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete!
# MAGIC 
# MAGIC You can now run the DLT pipeline. The following resources have been created:
# MAGIC 
# MAGIC | Resource Type | Name | Purpose |
# MAGIC |--------------|------|---------|
# MAGIC | Schema | `unified_reserves.bronze` | Raw data ingestion layer |
# MAGIC | Schema | `unified_reserves.silver` | Cleansed and enriched data |
# MAGIC | Schema | `unified_reserves.gold` | Business-ready aggregates |
# MAGIC | Volume | `bronze.raw` | Source files (claims, payments) |
# MAGIC | Volume | `bronze.checkpoints` | Streaming checkpoints |
# MAGIC | Volume | `silver.processed` | Intermediate processed files |
# MAGIC | Volume | `gold.curated` | Business-ready exports |
# MAGIC | Volume | `gold.models` | ML models and artifacts |
# MAGIC 
# MAGIC ### Next Steps:
# MAGIC 1. Create and run the DLT pipeline using the SQL/Python files in `SDP/bronze`, `SDP/silver`, `SDP/gold`
# MAGIC 2. Configure the pipeline in Databricks Workflows → Delta Live Tables







