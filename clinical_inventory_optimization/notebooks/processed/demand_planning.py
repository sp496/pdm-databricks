# Databricks notebook source
import sys
import os

current_dir = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # Code/Clinical_Inventory
repo_root = os.path.dirname(project_root)  # Code
sys.path.extend([project_root, repo_root])

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import re
from pandas import json_normalize
from lib.processed import demand_planning_processor as dp
from delta.tables import DeltaTable

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print(f"Environment: {env}")

# COMMAND ----------

UNITY_CATALOG_TABLE = f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_demand_plan`"
treatment_group_mapping_table = f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_treatment_groups`"
subject_summary_table = f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_subject_summary`"


# COMMAND ----------


df_subjects = spark.sql(f"""
    SELECT * FROM {subject_summary_table} t
    WHERE t.extract_date = (
        SELECT MAX(t2.extract_date) FROM {subject_summary_table} t2
        WHERE t2.study_protocol = t.study_protocol
    )
""").toPandas()
df_mapping = spark.table(treatment_group_mapping_table).toPandas()
df_mapping = df_mapping[
    df_mapping[['visit_days', 'dispensing_quantity', 'dispensing_frequency_days']].notna().all(axis=1)]

# COMMAND ----------

# df_subjects = df_subjects[     (pd.to_datetime(df_subjects['extract_date']) - pd.to_datetime(df_subjects['last_study_visit_date'])).dt.days <= 60 ]

# COMMAND ----------

df_final = dp.run_demand_planning(df_subjects, df_mapping)

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
import pyspark.sql.functions as F

schema_mapping = {
    "study_name": StringType(),
    "parent_depot": IntegerType(),
    "site_id": IntegerType(),
    "subject_number": IntegerType(),
    "subject_status": StringType(),
    "subject_country": StringType(),
    "randomized_treatment": StringType(),
    "tpc": StringType(),
    "drug_dispensed": StringType(),
    "dispensing_quantity": IntegerType(),
    "predicted_study_visit": StringType(),
    "cycle": IntegerType(),
    "day": IntegerType(),
    "predicted_next_visit_date": DateType(),
    "extract_date": DateType(),
    "processed_timestamp": TimestampType()
}

spark_df = spark.createDataFrame(df_final)

for col_name, data_type in schema_mapping.items():
    if col_name in spark_df.columns:
        if isinstance(data_type, DateType):
            spark_df = spark_df.withColumn(col_name, F.to_date(F.col(col_name), "yyyy-MM-dd"))
        elif isinstance(data_type, TimestampType):
            spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
        else:
            spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(data_type))

# spark_df = spark_df.coalesce(4)

print(f"\nWriting records to Unity Catalog table: {UNITY_CATALOG_TABLE}...")

spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .saveAsTable(UNITY_CATALOG_TABLE)

print("Data successfully loaded and unified.")