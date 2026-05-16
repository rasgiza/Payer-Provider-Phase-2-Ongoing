# Fabric notebook source

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# # Silver Stage: Data Cleaning and Validation
# 
# This notebook transforms Bronze raw data into Silver Stage cleaned data.
# 
# ## ⚠️ IMPORTANT: Set Default Lakehouse
# **Before running, set `lh_silver_stage` as the default lakehouse in the Fabric UI.**
# 
# Also attach `lh_bronze_raw` as an additional lakehouse (not default).
# 
# ## Data Flow
# ```
# lh_bronze_raw.patients_raw      →  patients_clean (default)
# lh_bronze_raw.providers_raw     →  providers_clean (default)
# lh_bronze_raw.encounters_raw    →  encounters_clean (default)
# lh_bronze_raw.claims_raw        →  claims_clean (default)
# lh_bronze_raw.prescriptions_raw →  prescriptions_clean (default)
# 
# lh_bronze_raw.diagnoses_raw     →  diagnoses_clean (default)```

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# Configuration - use fully qualified table names for pipeline execution
# In Fabric, tables are in lakehouse.table format
BRONZE_LAKEHOUSE = "lh_bronze_raw"
BRONZE_SCHEMA = f"{BRONZE_LAKEHOUSE}"  # Tables schema

SILVER_LAKEHOUSE = "lh_silver_stage"
SILVER_SCHEMA = f"{SILVER_LAKEHOUSE}"  # Target schema

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
print(f"Spark version: {spark.version}")
print(f"Processing started at: {datetime.now()}")
print(f"\nSource: {BRONZE_SCHEMA}")
print(f"Target: {SILVER_SCHEMA}")

# Verify Bronze tables
print(f"\nBronze tables ({BRONZE_SCHEMA}):")
try:
    spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").show(truncate=False)
except Exception as e:
    print(f"Could not list tables: {e}")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 1. Clean Patients Data

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# Read from Bronze using fully qualified table name
patients_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.patients_raw")
print(f"Bronze patients count: {patients_raw.count():,}")

# Clean and transform
patients_clean = patients_raw \
    .withColumn("first_name", initcap(trim(col("first_name")))) \
    .withColumn("last_name", initcap(trim(col("last_name")))) \
    .withColumn("gender", 
        when(upper(col("gender")).isin("M", "MALE"), "Male")
        .when(upper(col("gender")).isin("F", "FEMALE"), "Female")
        .otherwise("Unknown")
    ) \
    .withColumn("state", upper(trim(col("state")))) \
    .withColumn("zip_code", regexp_replace(col("zip_code"), "[^0-9]", "")) \
    .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", "")) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("date_of_birth", to_date(col("date_of_birth"))) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["patient_id"])

# Write to Silver lakehouse using fully qualified name
patients_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.patients_clean")

print(f"✓ {SILVER_SCHEMA}.patients_clean: {patients_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 2. Clean Providers Data

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# Read from Bronze
providers_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.providers_raw")
print(f"Bronze providers count: {providers_raw.count():,}")

# Clean and transform
providers_clean = providers_raw \
    .withColumn("first_name", initcap(trim(col("first_name")))) \
    .withColumn("last_name", initcap(trim(col("last_name")))) \
    .withColumn("specialty", initcap(trim(col("specialty")))) \
    .withColumn("department", initcap(trim(col("department")))) \
    .withColumn("npi", regexp_replace(col("npi").cast("string"), "[^0-9]", "")) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("hire_date", to_date(col("hire_date"))) \
    .withColumn("is_active", when(col("status") == "Active", True).otherwise(False)) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["provider_id"])

# Write to Silver lakehouse using fully qualified name
providers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.providers_clean")

print(f"✓ {SILVER_SCHEMA}.providers_clean: {providers_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 3. Clean Encounters Data

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# Read from Bronze
encounters_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.encounters_raw")
print(f"Bronze encounters count: {encounters_raw.count():,}")
print("Columns:", encounters_raw.columns)

# Clean and transform (admit_date from Bronze → encounter_date alias for downstream)
encounters_clean = encounters_raw \
    .withColumn("encounter_type", initcap(trim(col("encounter_type")))) \
    .withColumn("admission_type", initcap(trim(col("admission_type")))) \
    .withColumn("discharge_disposition", initcap(trim(col("discharge_disposition")))) \
    .withColumn("encounter_date", to_date(col("admit_date"))) \
    .withColumn("discharge_date", to_date(col("discharge_date"))) \
    .withColumn("length_of_stay", 
        when(col("discharge_date").isNotNull() & col("encounter_date").isNotNull(),
             datediff(col("discharge_date"), col("encounter_date")))
        .otherwise(col("length_of_stay"))
    ) \
    .withColumn("total_charges", col("total_charges").cast("decimal(18,2)")) \
    .withColumn("readmission_risk", col("readmission_risk").cast("double")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["encounter_id"])

# Write to Silver lakehouse using fully qualified name
encounters_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.encounters_clean")

print(f"✓ {SILVER_SCHEMA}.encounters_clean: {encounters_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 4. Clean Claims Data

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# Read from Bronze
claims_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.claims_raw")
print(f"Bronze claims count: {claims_raw.count():,}")
print("Columns:", claims_raw.columns)

# Clean and transform (submit_date → claim_date, process_date → payment_date for downstream)
claims_clean = claims_raw \
    .withColumn("claim_type", initcap(trim(col("claim_type")))) \
    .withColumn("claim_status", initcap(trim(col("claim_status")))) \
    .withColumn("service_date", to_date(col("service_date"))) \
    .withColumn("claim_date", to_date(col("submit_date"))) \
    .withColumn("payment_date", to_date(col("process_date"))) \
    .withColumn("billed_amount", col("billed_amount").cast("decimal(18,2)")) \
    .withColumn("allowed_amount", col("allowed_amount").cast("decimal(18,2)")) \
    .withColumn("paid_amount", col("paid_amount").cast("decimal(18,2)")) \
    .withColumn("denial_flag", coalesce(col("denial_flag").cast("int"), lit(0))) \
    .withColumn("denial_risk_score", col("denial_risk_score").cast("double")) \
    .withColumn("denial_risk_category", initcap(trim(col("denial_risk_category")))) \
    .withColumn("days_to_payment", col("days_to_payment").cast("int")) \
    .withColumn("net_collection_rate", col("net_collection_rate").cast("double")) \
    .withColumn("appeal_outcome", initcap(trim(col("appeal_outcome")))) \
    .withColumn("appeal_amount_recovered", col("appeal_amount_recovered").cast("decimal(18,2)")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["claim_id"])

# DQ: claim_id is mandatory; claim_date <= payment_date when both populated
_dq_null_pk = claims_clean.filter(col("claim_id").isNull()).count()
_dq_bad_dates = claims_clean.filter(
    col("payment_date").isNotNull() & col("claim_date").isNotNull()
    & (col("payment_date") < col("claim_date"))
).count()
print(f"DQ claims: null_claim_id={_dq_null_pk}, payment_before_claim={_dq_bad_dates}")
assert _dq_null_pk == 0, "DQ FAIL: claim_id contains nulls"

# Write to Silver lakehouse using fully qualified name
claims_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.claims_clean")

print(f"✓ {SILVER_SCHEMA}.claims_clean: {claims_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 5. Clean Prescriptions Data

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# Read from Bronze
prescriptions_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.prescriptions_raw")
print(f"Bronze prescriptions count: {prescriptions_raw.count():,}")
print("Columns:", prescriptions_raw.columns)

# Clean and transform (only columns that exist in Bronze)
prescriptions_clean = prescriptions_raw \
    .withColumn("medication_name", initcap(trim(col("medication_name")))) \
    .withColumn("fill_date", to_date(col("fill_date"))) \
    .withColumn("days_supply", col("days_supply").cast("int")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("refill_number", col("refill_number").cast("int")) \
    .withColumn("is_generic", col("is_generic").cast("int")) \
    .withColumn("total_cost", col("total_cost").cast("decimal(18,2)")) \
    .withColumn("copay_amount", col("copay_amount").cast("decimal(18,2)")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["prescription_id"])

# Write to Silver lakehouse
prescriptions_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.prescriptions_clean")

print(f"✓ {SILVER_SCHEMA}.prescriptions_clean: {prescriptions_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6. Clean Diagnoses Data

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

# Read from Bronze
diagnoses_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.diagnoses_raw")
print(f"Bronze diagnoses count: {diagnoses_raw.count():,}")
print("Columns:", diagnoses_raw.columns)

# Clean and transform (sequence_number from Bronze, no created_date/modified_date)
diagnoses_clean = diagnoses_raw \
    .withColumn("icd_code", upper(trim(col("icd_code")))) \
    .withColumn("diagnosis_type", initcap(trim(col("diagnosis_type")))) \
    .withColumn("present_on_admission", upper(trim(col("present_on_admission")))) \
    .withColumn("sequence_number", col("sequence_number").cast("int")) \
    .withColumn("diagnosis_date", to_date(col("diagnosis_date"))) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["diagnosis_id"])

# Write to Silver lakehouse
diagnoses_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.diagnoses_clean")

print(f"✓ {SILVER_SCHEMA}.diagnoses_clean: {diagnoses_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6a. Clean Member Enrollment

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

member_enrollment_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.member_enrollment_raw")
print(f"Bronze member_enrollment count: {member_enrollment_raw.count():,}")

member_enrollment_clean = member_enrollment_raw \
    .withColumn("year_month", trim(col("year_month"))) \
    .withColumn("coverage_start", to_date(col("coverage_start"))) \
    .withColumn("coverage_end", to_date(col("coverage_end"))) \
    .withColumn("enrollment_status", initcap(trim(col("enrollment_status")))) \
    .withColumn("pmpm_premium", col("pmpm_premium").cast("decimal(18,2)")) \
    .withColumn("is_capitated", col("is_capitated").cast("boolean")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["enrollment_id"])

_dq = member_enrollment_clean.filter(col("enrollment_id").isNull() | col("member_id").isNull()).count()
assert _dq == 0, f"DQ FAIL: member_enrollment has {_dq} null PK/FK rows"
print(f"DQ member_enrollment: null_keys={_dq}")

member_enrollment_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.member_enrollment_clean")
print(f"✓ {SILVER_SCHEMA}.member_enrollment_clean: {member_enrollment_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6b. Clean Premiums

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

premiums_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.premiums_raw")
print(f"Bronze premiums count: {premiums_raw.count():,}")

premiums_clean = premiums_raw \
    .withColumn("year_month", trim(col("year_month"))) \
    .withColumn("premium_amount", col("premium_amount").cast("decimal(18,2)")) \
    .withColumn("subsidy_amount", col("subsidy_amount").cast("decimal(18,2)")) \
    .withColumn("member_paid", col("member_paid").cast("decimal(18,2)")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["premium_id"])

_dq = premiums_clean.filter(col("premium_amount") < 0).count()
assert _dq == 0, f"DQ FAIL: premiums has {_dq} negative amounts"
premiums_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.premiums_clean")
print(f"✓ {SILVER_SCHEMA}.premiums_clean: {premiums_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6c. Clean Authorizations

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

authorizations_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.authorizations_raw")
print(f"Bronze authorizations count: {authorizations_raw.count():,}")

authorizations_clean = authorizations_raw \
    .withColumn("submit_date", to_date(col("submit_date"))) \
    .withColumn("decision_date", to_date(col("decision_date"))) \
    .withColumn("decision_tat_hours", col("decision_tat_hours").cast("double")) \
    .withColumn("auth_outcome", initcap(trim(col("auth_outcome")))) \
    .withColumn("auth_units_requested", col("auth_units_requested").cast("int")) \
    .withColumn("auth_units_approved", col("auth_units_approved").cast("int")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["auth_id"])

authorizations_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.authorizations_clean")
print(f"✓ {SILVER_SCHEMA}.authorizations_clean: {authorizations_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6d. Clean Capitation

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

capitation_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.capitation_raw")
print(f"Bronze capitation count: {capitation_raw.count():,}")

capitation_clean = capitation_raw \
    .withColumn("year_month", trim(col("year_month"))) \
    .withColumn("capitation_pmpm", col("capitation_pmpm").cast("decimal(18,2)")) \
    .withColumn("withhold_pct", col("withhold_pct").cast("double")) \
    .withColumn("bonus_eligible", col("bonus_eligible").cast("boolean")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["capitation_id"])

capitation_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.capitation_clean")
print(f"✓ {SILVER_SCHEMA}.capitation_clean: {capitation_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6e. Clean Provider Contracts

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

provider_contracts_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.provider_contracts_raw")
print(f"Bronze provider_contracts count: {provider_contracts_raw.count():,}")

provider_contracts_clean = provider_contracts_raw \
    .withColumn("contract_type", initcap(trim(col("contract_type")))) \
    .withColumn("effective_date", to_date(col("effective_date"))) \
    .withColumn("termination_date", to_date(col("termination_date"))) \
    .withColumn("fee_schedule_pct_medicare", col("fee_schedule_pct_medicare").cast("double")) \
    .withColumn("withhold_pct", col("withhold_pct").cast("double")) \
    .withColumn("quality_bonus_pct", col("quality_bonus_pct").cast("double")) \
    .withColumn("is_in_network", col("is_in_network").cast("boolean")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["contract_id"])

provider_contracts_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.provider_contracts_clean")
print(f"✓ {SILVER_SCHEMA}.provider_contracts_clean: {provider_contracts_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6f. Clean HEDIS Compliance

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

hedis_compliance_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.hedis_compliance_raw")
print(f"Bronze hedis_compliance count: {hedis_compliance_raw.count():,}")

hedis_compliance_clean = hedis_compliance_raw \
    .withColumn("measurement_year", col("measurement_year").cast("int")) \
    .withColumn("denominator_eligible", col("denominator_eligible").cast("int")) \
    .withColumn("numerator_met", col("numerator_met").cast("int")) \
    .withColumn("compliance_rate", col("compliance_rate").cast("double")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["compliance_id"])

hedis_compliance_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.hedis_compliance_clean")
print(f"✓ {SILVER_SCHEMA}.hedis_compliance_clean: {hedis_compliance_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6g. Clean Star Ratings

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

star_ratings_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.star_ratings_raw")
print(f"Bronze star_ratings count: {star_ratings_raw.count():,}")

star_ratings_clean = star_ratings_raw \
    .withColumn("measurement_year", col("measurement_year").cast("int")) \
    .withColumn("weight", col("weight").cast("int")) \
    .withColumn("star_score", col("star_score").cast("double")) \
    .withColumn("weighted_score", col("weighted_score").cast("double")) \
    .withColumn("national_avg", col("national_avg").cast("double")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["star_id"])

star_ratings_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.star_ratings_clean")
print(f"✓ {SILVER_SCHEMA}.star_ratings_clean: {star_ratings_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6h. Clean Risk Adjustment (RAF)

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

risk_adjustment_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.risk_adjustment_raw")
print(f"Bronze risk_adjustment count: {risk_adjustment_raw.count():,}")

risk_adjustment_clean = risk_adjustment_raw \
    .withColumn("measurement_year", col("measurement_year").cast("int")) \
    .withColumn("hcc_count", col("hcc_count").cast("int")) \
    .withColumn("demographic_score", col("demographic_score").cast("double")) \
    .withColumn("disease_score", col("disease_score").cast("double")) \
    .withColumn("raf_score", col("raf_score").cast("double")) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["raf_id"])

risk_adjustment_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.risk_adjustment_clean")
print(f"✓ {SILVER_SCHEMA}.risk_adjustment_clean: {risk_adjustment_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 6i. Clean Claim Appeals

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

claim_appeals_raw = spark.sql(f"SELECT * FROM {BRONZE_SCHEMA}.claim_appeals_raw")
print(f"Bronze claim_appeals count: {claim_appeals_raw.count():,}")

claim_appeals_clean = claim_appeals_raw \
    .withColumn("appeal_level", trim(col("appeal_level"))) \
    .withColumn("appeal_level_num", col("appeal_level_num").cast("int")) \
    .withColumn("submit_date", to_date(col("submit_date"))) \
    .withColumn("decision_date", to_date(col("decision_date"))) \
    .withColumn("appeal_outcome", initcap(trim(col("appeal_outcome")))) \
    .withColumn("appeal_amount_recovered", col("appeal_amount_recovered").cast("decimal(18,2)")) \
    .withColumn("appeal_reason", trim(col("appeal_reason"))) \
    .withColumn("_silver_load_timestamp", current_timestamp()) \
    .dropDuplicates(["appeal_id"])

claim_appeals_clean.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.claim_appeals_clean")
print(f"✓ {SILVER_SCHEMA}.claim_appeals_clean: {claim_appeals_clean.count():,} rows")

# METADATA **{"language":"markdown"}**

# MARKDOWN **{"language":"markdown"}**

# ## 7. Verify Silver Tables

# METADATA **{"language":"python"}**

# CELL **{"language":"python"}**

print("="*60)
print("SILVER STAGE PROCESSING COMPLETE")
print("="*60)

# Show tables in Silver lakehouse using fully qualified names
print(f"\nSilver Stage Tables ({SILVER_SCHEMA}):")
try:
    spark.sql(f"SHOW TABLES IN {SILVER_SCHEMA}").show(truncate=False)
except Exception as e:
    print(f"Could not list tables: {e}")

# Row counts using fully qualified names
print("\nRow Counts:")
try:
    print(f"  patients_clean:      {spark.sql(f'SELECT COUNT(*) FROM {SILVER_SCHEMA}.patients_clean').collect()[0][0]:,}")
    print(f"  providers_clean:     {spark.sql(f'SELECT COUNT(*) FROM {SILVER_SCHEMA}.providers_clean').collect()[0][0]:,}")
    print(f"  encounters_clean:    {spark.sql(f'SELECT COUNT(*) FROM {SILVER_SCHEMA}.encounters_clean').collect()[0][0]:,}")
    print(f"  claims_clean:        {spark.sql(f'SELECT COUNT(*) FROM {SILVER_SCHEMA}.claims_clean').collect()[0][0]:,}")
    print(f"  prescriptions_clean: {spark.sql(f'SELECT COUNT(*) FROM {SILVER_SCHEMA}.prescriptions_clean').collect()[0][0]:,}")
    print(f"  diagnoses_clean:     {spark.sql(f'SELECT COUNT(*) FROM {SILVER_SCHEMA}.diagnoses_clean').collect()[0][0]:,}")
    for _t in ["member_enrollment_clean", "premiums_clean", "authorizations_clean",
               "capitation_clean", "provider_contracts_clean", "hedis_compliance_clean",
               "star_ratings_clean", "risk_adjustment_clean", "claim_appeals_clean"]:
        try:
            _c = spark.sql(f"SELECT COUNT(*) FROM {SILVER_SCHEMA}.{_t}").collect()[0][0]
            print(f"  {_t:<25} {_c:,}")
        except Exception as _e:
            print(f"  {_t:<25} ERROR: {_e}")
except Exception as e:
    print(f"Could not count tables: {e}")

print(f"\n✅ Processing completed at: {datetime.now()}")
