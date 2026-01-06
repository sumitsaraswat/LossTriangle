# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Bronze Layer: Data Ingestion
# MAGIC 
# MAGIC **Owner: Marcus (Data Engineer)**
# MAGIC 
# MAGIC This notebook ingests raw claims and payment data into the Bronze layer.
# MAGIC Supports both batch ingestion and streaming via Auto Loader.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "unified_reserves"
BRONZE_SCHEMA = "bronze"

# COMMAND ----------

# Set catalog
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✅ Using catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Sample Data
# MAGIC 
# MAGIC For development, we'll create sample data directly in the notebook.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp, lit, to_date, to_timestamp

# COMMAND ----------

# Sample Claims Data
claims_data = [
    ("CLM-2018-001", "POL-12345", "2018-09-15", "2018-09-16", "John Smith", "Water Damage", 
     "Hurricane caused roof damage and flooding", 
     "Initial inspection complete. Roof damage confirmed. Water intrusion in multiple rooms.",
     "Open", "Miami-Dade", "FL", "33101", "CAT-2018-MICHAEL", "2018-09-16 10:30:00", "2024-01-15 14:20:00"),
    ("CLM-2018-002", "POL-12346", "2018-09-15", "2018-09-18", "Maria Garcia", "Wind",
     "Severe wind damage to exterior",
     "Wind damage to siding and windows. Multiple areas affected.",
     "Closed", "Miami-Dade", "FL", "33102", "CAT-2018-MICHAEL", "2018-09-18 09:15:00", "2019-03-20 11:30:00"),
    ("CLM-2018-003", "POL-12347", "2018-09-15", "2018-10-01", "Robert Johnson", "Water Damage",
     "Flooding from storm surge",
     "Significant water damage. Signs of mold developing. Recommend immediate remediation. Customer mentioned consulting attorney.",
     "Open", "Broward", "FL", "33301", "CAT-2018-MICHAEL", "2018-10-01 14:45:00", "2024-06-10 09:00:00"),
    ("CLM-2018-004", "POL-12348", "2018-09-16", "2018-09-20", "Sarah Williams", "Water Damage",
     "Roof leak causing interior damage",
     "Water seepage through damaged roof. Drywall damage in living room and bedroom. Early signs of moisture accumulation.",
     "Closed", "Palm Beach", "FL", "33401", "CAT-2018-MICHAEL", "2018-09-20 11:00:00", "2020-02-15 16:30:00"),
    ("CLM-2018-005", "POL-12349", "2018-09-15", "2018-11-15", "Michael Brown", "Water Damage",
     "Delayed water damage discovery",
     "Customer discovered black mold in walls two months after storm. Extensive remediation required. Attorney representation confirmed.",
     "Open", "Miami-Dade", "FL", "33103", "CAT-2018-MICHAEL", "2018-11-15 13:30:00", "2024-09-01 10:15:00"),
    ("CLM-2019-001", "POL-22345", "2019-06-10", "2019-06-12", "Jennifer Davis", "Fire",
     "Kitchen fire damage",
     "Fire started in kitchen. Smoke and fire damage to multiple rooms. No injuries reported.",
     "Closed", "Orange", "FL", "32801", None, "2019-06-12 08:00:00", "2019-12-01 15:00:00"),
    ("CLM-2019-002", "POL-22346", "2019-08-20", "2019-08-22", "David Wilson", "Theft",
     "Burglary with property loss",
     "Break-in through back door. Electronics and jewelry stolen. Police report filed.",
     "Closed", "Hillsborough", "FL", "33601", None, "2019-08-22 10:30:00", "2019-11-15 14:00:00"),
    ("CLM-2020-001", "POL-32345", "2020-03-15", "2020-03-18", "Lisa Anderson", "Water Damage",
     "Pipe burst during cold snap",
     "Frozen pipe burst causing flooding. Significant water damage to flooring and walls. Damp conditions noted.",
     "Open", "Duval", "FL", "32099", None, "2020-03-18 09:00:00", "2024-03-10 11:30:00"),
    ("CLM-2020-002", "POL-32346", "2020-06-01", "2020-06-05", "James Taylor", "Liability",
     "Slip and fall on property",
     "Visitor injured on property. Medical expenses claimed. Lawsuit filed by injured party. Legal counsel engaged.",
     "Open", "Miami-Dade", "FL", "33104", None, "2020-06-05 14:00:00", "2024-08-20 16:45:00"),
    ("CLM-2021-001", "POL-42345", "2021-09-01", "2021-09-03", "Amanda Martinez", "Wind",
     "Hurricane Ida damage",
     "Roof and siding damage from Hurricane Ida. Tree fell on garage. Structural assessment needed.",
     "Open", "Miami-Dade", "FL", "33105", "CAT-2021-IDA", "2021-09-03 11:00:00", "2024-05-15 13:00:00"),
]

claims_schema = StructType([
    StructField("claim_id", StringType(), False),
    StructField("policy_id", StringType(), False),
    StructField("loss_date", StringType(), False),
    StructField("report_date", StringType(), False),
    StructField("claimant_name", StringType(), True),
    StructField("loss_type", StringType(), False),
    StructField("loss_description", StringType(), True),
    StructField("adjuster_notes", StringType(), True),
    StructField("claim_status", StringType(), False),
    StructField("county", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("catastrophe_code", StringType(), True),
    StructField("created_at", StringType(), False),
    StructField("updated_at", StringType(), False),
])

claims_df = spark.createDataFrame(claims_data, claims_schema)

# Convert date/timestamp columns
claims_df = claims_df \
    .withColumn("loss_date", to_date("loss_date")) \
    .withColumn("report_date", to_date("report_date")) \
    .withColumn("created_at", to_timestamp("created_at")) \
    .withColumn("updated_at", to_timestamp("updated_at")) \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit("sample_data"))

display(claims_df)

# COMMAND ----------

# Sample Payments Data
payments_data = [
    ("PAY-001", "CLM-2018-001", "2018-10-15", "Indemnity", 15000.0, 15000.0, 50000.0, "Cleared", "CHK-10001", "John Smith", "Loss Payment", "2018-10-15 10:00:00"),
    ("PAY-002", "CLM-2018-001", "2018-12-01", "Indemnity", 20000.0, 35000.0, 35000.0, "Cleared", "CHK-10015", "John Smith", "Loss Payment", "2018-12-01 14:30:00"),
    ("PAY-003", "CLM-2018-001", "2019-06-15", "Indemnity", 10000.0, 45000.0, 25000.0, "Cleared", "CHK-10089", "John Smith", "Loss Payment", "2019-06-15 09:00:00"),
    ("PAY-004", "CLM-2018-002", "2018-10-20", "Indemnity", 25000.0, 25000.0, 15000.0, "Cleared", "CHK-10005", "Maria Garcia", "Loss Payment", "2018-10-20 11:00:00"),
    ("PAY-005", "CLM-2018-002", "2019-01-15", "Indemnity", 15000.0, 40000.0, 0.0, "Cleared", "CHK-10045", "Maria Garcia", "Loss Payment", "2019-01-15 15:00:00"),
    ("PAY-006", "CLM-2018-003", "2018-11-01", "Indemnity", 10000.0, 10000.0, 100000.0, "Cleared", "CHK-10012", "Robert Johnson", "Loss Payment", "2018-11-01 10:30:00"),
    ("PAY-007", "CLM-2018-003", "2019-03-01", "Indemnity", 15000.0, 25000.0, 125000.0, "Cleared", "CHK-10067", "Robert Johnson", "Loss Payment", "2019-03-01 14:00:00"),
    ("PAY-008", "CLM-2018-003", "2020-01-15", "Indemnity", 30000.0, 55000.0, 150000.0, "Cleared", "CHK-10234", "Robert Johnson", "Loss Payment", "2020-01-15 11:00:00"),
    ("PAY-009", "CLM-2018-003", "2021-06-01", "Expense", 25000.0, 80000.0, 175000.0, "Cleared", "CHK-10456", "Mold Remediation Inc", "Remediation", "2021-06-01 09:30:00"),
    ("PAY-010", "CLM-2018-004", "2018-11-15", "Indemnity", 18000.0, 18000.0, 12000.0, "Cleared", "CHK-10025", "Sarah Williams", "Loss Payment", "2018-11-15 10:00:00"),
    ("PAY-011", "CLM-2018-004", "2019-02-01", "Indemnity", 12000.0, 30000.0, 0.0, "Cleared", "CHK-10055", "Sarah Williams", "Loss Payment", "2019-02-01 14:00:00"),
    ("PAY-012", "CLM-2018-005", "2019-01-15", "Indemnity", 20000.0, 20000.0, 200000.0, "Cleared", "CHK-10048", "Michael Brown", "Loss Payment", "2019-01-15 11:30:00"),
    ("PAY-013", "CLM-2018-005", "2020-03-01", "Expense", 50000.0, 70000.0, 250000.0, "Cleared", "CHK-10298", "Toxic Mold Specialists", "Remediation", "2020-03-01 10:00:00"),
    ("PAY-014", "CLM-2018-005", "2021-09-15", "Expense", 35000.0, 105000.0, 200000.0, "Cleared", "CHK-10567", "Smith & Associates Law", "Legal Defense", "2021-09-15 14:00:00"),
    ("PAY-015", "CLM-2019-001", "2019-07-01", "Indemnity", 45000.0, 45000.0, 15000.0, "Cleared", "CHK-10112", "Jennifer Davis", "Loss Payment", "2019-07-01 09:00:00"),
    ("PAY-016", "CLM-2019-001", "2019-10-15", "Indemnity", 15000.0, 60000.0, 0.0, "Cleared", "CHK-10189", "Jennifer Davis", "Loss Payment", "2019-10-15 11:00:00"),
    ("PAY-017", "CLM-2019-002", "2019-09-15", "Indemnity", 8000.0, 8000.0, 0.0, "Cleared", "CHK-10156", "David Wilson", "Loss Payment", "2019-09-15 14:30:00"),
    ("PAY-018", "CLM-2020-001", "2020-04-15", "Indemnity", 12000.0, 12000.0, 25000.0, "Cleared", "CHK-10312", "Lisa Anderson", "Loss Payment", "2020-04-15 10:00:00"),
    ("PAY-019", "CLM-2020-002", "2020-08-01", "Expense", 15000.0, 15000.0, 150000.0, "Cleared", "CHK-10389", "Legal Defense Fund", "Legal Defense", "2020-08-01 11:00:00"),
    ("PAY-020", "CLM-2021-001", "2021-10-15", "Indemnity", 25000.0, 25000.0, 75000.0, "Cleared", "CHK-10612", "Amanda Martinez", "Loss Payment", "2021-10-15 09:30:00"),
]

payments_schema = StructType([
    StructField("payment_id", StringType(), False),
    StructField("claim_id", StringType(), False),
    StructField("payment_date", StringType(), False),
    StructField("payment_type", StringType(), False),
    StructField("payment_amount", DoubleType(), False),
    StructField("cumulative_paid", DoubleType(), True),
    StructField("reserve_amount", DoubleType(), True),
    StructField("payment_status", StringType(), False),
    StructField("check_number", StringType(), True),
    StructField("payee_name", StringType(), True),
    StructField("expense_type", StringType(), True),
    StructField("created_at", StringType(), False),
])

payments_df = spark.createDataFrame(payments_data, payments_schema)

# Convert date/timestamp columns
payments_df = payments_df \
    .withColumn("payment_date", to_date("payment_date")) \
    .withColumn("created_at", to_timestamp("created_at")) \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", lit("sample_data"))

display(payments_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Tables

# COMMAND ----------

# Write claims to Bronze layer
claims_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.raw_claims")

print(f"✅ Loaded {claims_df.count()} claims to {CATALOG}.{BRONZE_SCHEMA}.raw_claims")

# COMMAND ----------

# Write payments to Bronze layer
payments_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.raw_payments")

print(f"✅ Loaded {payments_df.count()} payments to {CATALOG}.{BRONZE_SCHEMA}.raw_payments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

# Check claims
print("📋 Raw Claims Summary:")
claims_check = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_claims")
print(f"Total records: {claims_check.count()}")
display(claims_check.groupBy("loss_type").count().orderBy("count", ascending=False))

# COMMAND ----------

# Check payments
print("📋 Raw Payments Summary:")
payments_check = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.raw_payments")
print(f"Total records: {payments_check.count()}")
display(payments_check.groupBy("payment_type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Bronze Layer Complete!
# MAGIC 
# MAGIC Data has been ingested into the Bronze layer. Proceed to:
# MAGIC - **02_silver/02_triangle_builder** - Build loss triangles
