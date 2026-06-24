# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Reconciliation Layer (CLINICAL)
# MAGIC Reconciles the clinical curated inventory against the staged EBS clinical
# MAGIC snapshot and writes `reconciled_3pl_clinical_inventory` (partitioned by
# MAGIC Segment, Year, Quarter), independent of the commercial run. The
# MAGIC reconciliation logic lives in `lib.processed.transformations.process_clinical`;
# MAGIC this notebook owns the load → process → finalise → write orchestration.
# MAGIC
# MAGIC Depends on `stage_clinical_inventory` (EBS snapshot) and the clinical
# MAGIC curated rows. Run the commercial counterpart
# MAGIC (`reconcile_commercial_inventory`) separately.

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
from pyspark.sql.types import StringType, DoubleType, BooleanType

from lib.processed.transformations import process_clinical
from lib.discovery import resolve_quarter_from_source
from common.config_loader import load_config

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resolve run context (env, config, quarter)

# COMMAND ----------

segment = "clinical"

env           = dbutils.widgets.get("DATAENV")
processed_cfg = load_config(os.path.join(project_root, "config/processed.json"))

curated_table                = processed_cfg["curated_table"].format(env=env)
ebs_clinical_inventory_table = processed_cfg["ebs_clinical_inventory_table"].format(env=env)
header_mapping_table         = processed_cfg["header_mapping_table"].format(env=env)
target_table                 = processed_cfg["reconciled_clinical_table"].format(env=env)

# Resolve the target quarter from the SOURCE landing — same logic as the upstream
# layers (historical | latest_completed | latest_available), so curate and
# reconcile always target the identical quarter for this segment.
resolved_env     = "prod" if env == "prd" else env
src_root         = f"{processed_cfg['src_bkt_mount_point']}/{processed_cfg['src_data_dir'].format(env=resolved_env)}"
segment_src_root = f"{src_root}/{segment}"
run_mode, year, quarter = resolve_quarter_from_source(processed_cfg, segment, dbutils, segment_src_root)

print(f"{segment}: env={env}  run_mode={run_mode}  year={year}  quarter={quarter}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Finalise + write helper

# COMMAND ----------

def finalise_and_write(combined_df, year, quarter):
    """Stamp Processing_Timestamp/Segment, backfill EBS context for curated-only
    rows, cast columns, and write the clinical reconciled partition."""
    spark_df = spark.createDataFrame(combined_df)
    spark_df = spark_df.withColumn(
        "Processing_Timestamp",
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:00")),
    )
    spark_df = spark_df.withColumn("Segment", lit(segment))

    # Backfill EBS context for curated-only rows by looking up rows with the
    # same Org_Code that have the EBS context populated.
    ebs_ctx = (
        spark_df
        .select("Org_Code", "Inventory_Org_Name", "Operating_Unit_Name", "Legal_Entity_Name")
        .filter(col("Inventory_Org_Name").isNotNull())
        .distinct()
    )
    spark_df = (
        spark_df
        .drop("Inventory_Org_Name", "Operating_Unit_Name", "Legal_Entity_Name")
        .join(ebs_ctx, on="Org_Code", how="left")
    )

    spark_df = spark_df.select(
        col("Org_Code").cast(StringType()),
        col("Inventory_Org_Name").cast(StringType()),
        col("Operating_Unit_Name").cast(StringType()),
        col("Legal_Entity_Name").cast(StringType()),
        col("Material_Group").cast(StringType()),
        col("Item_Number").cast(StringType()),
        col("Gilead_Material_Code").cast(StringType()),
        col("Item_Description").cast(StringType()),
        col("Material_Description").cast(StringType()),
        col("Lot_Number").cast(StringType()),
        col("Lot_Status").cast(StringType()),
        col("Lot_Expiry_Date").cast(StringType()),
        col("Lot_Retest_Date").cast(StringType()),
        col("Onhand_Quantity").cast(DoubleType()),
        col("Allocated_Quantity").cast(DoubleType()),
        col("Available_To_Reserve_Quantity").cast(DoubleType()),
        col("3PL_Quantity").cast(DoubleType()),
        col("3PL_Converted_Quantity").cast(DoubleType()),
        col("Primary_UOM").cast(StringType()),
        col("3PL_UOM").cast(StringType()),
        col("3PL").cast(StringType()),
        col("3PL_Name").cast(StringType()),
        col("3PL_Material_Code").cast(StringType()),
        col("3PL_Material_Type").cast(StringType()),
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
# MAGIC ebs_clinical_inventory is partitioned by (Year, Quarter) only and is
# MAGIC clinical-only, so its filter omits Segment.

# COMMAND ----------

if not year or not quarter:
    print(f"No curated data found for {segment} in {curated_table} — nothing to reconcile")
else:
    curated_f = (col("Segment") == segment) & (col("Year") == year) & (col("Quarter") == quarter)
    ebs_f     = (col("Year") == year) & (col("Quarter") == quarter)
    curated_df = spark.table(curated_table).filter(curated_f).toPandas()
    ebs_df     = spark.table(ebs_clinical_inventory_table).filter(ebs_f).toPandas()
    header_df  = spark.table(header_mapping_table).filter(curated_f).toPandas()
    print(f"  curated={curated_df.shape}  ebs={ebs_df.shape}  header_mapping={header_df.shape}")

    combined_df = process_clinical(
        curated_df        = curated_df,
        ebs_df            = ebs_df,
        header_mapping_df = header_df,
        year              = year,
        quarter           = quarter,
    )

    rows = finalise_and_write(combined_df, year, quarter)
    print(f"Done — wrote {rows} row(s) to {target_table}")
