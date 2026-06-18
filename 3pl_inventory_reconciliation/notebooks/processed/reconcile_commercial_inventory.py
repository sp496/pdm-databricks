# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Reconciliation Layer (COMMERCIAL)
# MAGIC Reconciles the commercial curated inventory against the SAP stock report and
# MAGIC writes `reconciled_3pl_commercial_inventory` (partitioned by Segment, Year,
# MAGIC Quarter), independent of the clinical run. The reconciliation logic lives in
# MAGIC `lib.processed.transformations.process_commercial`; this notebook owns the
# MAGIC load → process → finalise → write orchestration.
# MAGIC
# MAGIC Run the clinical counterpart (`reconcile_clinical_inventory`) separately.

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, DoubleType, BooleanType, IntegerType

from lib.processed.transformations import process_commercial
from common.config_loader import load_config

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resolve run context (env, config, quarter)

# COMMAND ----------

segment = "commercial"

env           = dbutils.widgets.get("DATAENV")
processed_cfg = load_config(os.path.join(project_root, "config/processed.json"))
run_config    = processed_cfg["run_config"]

curated_table        = processed_cfg["curated_table"].format(env=env)
sap_report_table     = processed_cfg["sap_report_table"].format(env=env)
header_mapping_table = processed_cfg["header_mapping_table"].format(env=env)
target_table         = processed_cfg["reconciled_commercial_table"].format(env=env)

# Resolve the target quarter — historical from config, else the latest present in
# the curated table for this segment.
if run_config["run_mode"] == "historical":
    year, quarter = run_config.get("year"), run_config.get("quarter")
    if not year or not quarter:
        raise ValueError("run_mode is 'historical' but 'year'/'quarter' not set in run_config")
else:
    latest = (
        spark.table(curated_table)
        .filter(col("Segment") == segment)
        .select("Year", "Quarter")
        .distinct()
        .orderBy(col("Year").desc(), col("Quarter").desc())
        .limit(1)
        .collect()
    )
    year, quarter = (latest[0]["Year"], latest[0]["Quarter"]) if latest else (None, None)

print(f"{segment}: env={env}  year={year}  quarter={quarter}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Finalise + write helper

# COMMAND ----------

def finalise_and_write(combined_df, year, quarter):
    """Stamp Processing_Timestamp/Segment, backfill Plant_Name for curated-only
    rows, cast columns, and write the commercial reconciled partition."""
    spark_df = spark.createDataFrame(combined_df)
    spark_df = spark_df.withColumn(
        "Processing_Timestamp",
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:00")),
    )
    spark_df = spark_df.withColumn("Segment", lit(segment))

    # Fill Plant_Name for curated-only rows using SAP-derived lookup
    plant_name_lookup = (
        spark_df
        .select("Plant_Number", "Plant_Name")
        .filter(col("Plant_Name").isNotNull())
        .distinct()
    )
    spark_df = (
        spark_df
        .drop("Plant_Name")
        .join(plant_name_lookup, on="Plant_Number", how="left")
    )

    spark_df = spark_df.select(
        col("Plant_Number").cast(StringType()),
        col("Plant_Name").cast(StringType()),
        col("External_Material_Group").cast(StringType()),
        col("Material_Number").cast(StringType()),
        col("Gilead_Material_Code").cast(StringType()),
        col("Material_Description").cast(StringType()),
        col("Cost_ea").cast(DoubleType()),
        col("Cost_ea_per_unit").cast(DoubleType()),
        col("Group_Valuation_Standard_Cost").cast(DoubleType()),
        col("Cost").cast(DoubleType()),
        col("Batch_Number").cast(StringType()),
        col("Stock_OH").cast(DoubleType()),
        col("UOM").cast(StringType()),
        col("3PL_Quantity").cast(DoubleType()),
        col("3PL_Converted_Quantity").cast(DoubleType()),
        col("3PL_UOM").cast(StringType()),
        col("3PL").cast(StringType()),
        col("3PL_Name").cast(StringType()),
        col("3PL_Material_Code").cast(StringType()),
        col("3PL_Material_Type").cast(StringType()),
        col("Line_item_variance_threshold_amount").cast(IntegerType()),
        col("File_Name").cast(StringType()),
        col("Date_Processed").cast(StringType()),
        col("Has_Error").cast(BooleanType()),
        col("Validation_Remark").cast(StringType()),
        col("Gilead_Batch_Number").cast(StringType()),
        col("3PL_Batch_Number").cast(StringType()),
        col("Plant_Classification").cast(StringType()),
        col("Effective_Material_Code").cast(StringType()),
        col("Effective_Batch_Number").cast(StringType()),
        col("Processing_Timestamp").cast(StringType()),
        col("Segment").cast(StringType()),
        col("Year").cast(StringType()),
        col("Quarter").cast(StringType()),
    )

    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"Segment = '{segment}' AND Year = '{year}' AND Quarter = '{quarter}'")
        .option("overwriteSchema", "false")
        .saveAsTable(target_table)
    )
    return spark_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load → reconcile → write

# COMMAND ----------

if not year or not quarter:
    print(f"No curated data found for {segment} in {curated_table} — nothing to reconcile")
else:
    f = (col("Segment") == segment) & (col("Year") == year) & (col("Quarter") == quarter)
    curated_df = spark.table(curated_table).filter(f).toPandas()
    sap_df     = spark.table(sap_report_table).filter(f).toPandas()
    header_df  = spark.table(header_mapping_table).filter(f).toPandas()
    print(f"  curated={curated_df.shape}  sap={sap_df.shape}  header_mapping={header_df.shape}")

    combined_df = process_commercial(
        curated_df        = curated_df,
        sap_df            = sap_df,
        header_mapping_df = header_df,
        year              = year,
        quarter           = quarter,
    )

    rows = finalise_and_write(combined_df, year, quarter)
    print(f"Done — wrote {rows} row(s) to {target_table}")
