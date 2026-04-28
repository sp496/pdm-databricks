# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import sys
import os

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # Code/Clinical_Inventory
repo_root    = os.path.dirname(project_root)                   # Code
sys.path.extend([project_root, repo_root])

# COMMAND ----------

import logging
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DateType, TimestampType, DoubleType
)
from common.config_loader import load_config

# COMMAND ----------

logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration Loading

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
logger.info(f"Environment: {env}")

config = load_config(os.path.join(project_root, "config/curated.json"))

resolved_env = "prod" if env == "prd" else env

mapping_bkt_mount_point = config["mapping_bkt_mount_point"]

treatment_group_mapping_file_path = config["treatment_group_mapping_file_path"].format(env=resolved_env)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Process and write treatment mapping

# COMMAND ----------

treatment_group_mapping_file_path = f"/dbfs{os.path.join(mapping_bkt_mount_point, treatment_group_mapping_file_path)}"
tgm_df = pd.read_excel(treatment_group_mapping_file_path, sheet_name='Treatment Group Mapping', dtype='str', engine='openpyxl')

# COMMAND ----------

# 🗺️ Column rename mapping (DataFrame → table)

column_mapping = {
    "Therapeutic Area": "therapeutic_area",
    "Study Protocol": "study_protocol",
    "Randomized Treatment": "randomized_treatment",
    "Subject Status": "subject_status",
    "TPC\nTreatment of Physician's Choice\nTopotecan OR Amrubicin Choice Of Drug\nIntended TPC": "tpc",
    "Study Drug Dispensed": "study_drug_dispensed",
    "Additional Study Drug Dispensed": "additional_study_drug_dispensed",
    "Additional Study Drug Prefix": "additional_study_drug_prefix",
    "Country": "country",
    "Visit Days": "visit_days",
    "Dispensing Quantity": "dispensing_quantity",
    "Dispensing Frequency (Days)": "dispensing_frequency_days",
    "Max Cycles": "max_cycles"
}

# 🧱 Expected schema for casting
schema_mapping = {
    "therapeutic_area": StringType(),
    "study_protocol": StringType(),
    "randomized_treatment": StringType(),
    "subject_status": StringType(),
    "tpc": StringType(),
    "study_drug_dispensed": StringType(),
    "additional_study_drug_dispensed": StringType(),
    "additional_study_drug_prefix": StringType(),
    "country": StringType(),
    "visit_days": StringType(),
    "dispensing_quantity": LongType(),
    "dispensing_frequency_days": LongType(),
    "max_cycles": DoubleType()
}

tgm_df = tgm_df[column_mapping.keys()]

# 🧩 Step 1: Rename columns
tgm_df_renamed = tgm_df.rename(columns=column_mapping)

# 🧹 Step 1b: Strip leading/trailing whitespace from all string columns
string_cols = tgm_df_renamed.select_dtypes(include='object').columns
tgm_df_renamed[string_cols] = tgm_df_renamed[string_cols].apply(lambda col: col.str.strip())

# tgm_df_cleaned = tgm_df_renamed.dropna(subset=['visit_days', 'dispensing_quantity', 'dispensing_frequency_days'])
tgm_df_cleaned = tgm_df_renamed.dropna(how='all')

# 🧱 Step 2: Convert pandas → Spark
spark_tgm_df = spark.createDataFrame(tgm_df_cleaned)

# 🧮 Step 3: Cast each column to correct type
for col_name, data_type in schema_mapping.items():
    if col_name in spark_tgm_df.columns:
        spark_tgm_df = spark_tgm_df.withColumn(col_name,  F.col(col_name).try_cast(data_type))

spark_tgm_df.display()

# 💾 Step 4: Truncate and load (overwrite entire table)
(
    spark_tgm_df.write
    .format("delta")
    .mode("overwrite")  # full overwrite (truncate + load)
    .option("overwriteSchema", "false")
    .saveAsTable(f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_treatment_groups`")
)

print("✅ Successfully loaded clinical_treatment_group_mapping table (truncate and load).")