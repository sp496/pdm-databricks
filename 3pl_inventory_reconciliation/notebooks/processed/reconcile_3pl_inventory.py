# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Reconciliation Layer
# MAGIC Loops over the configured segments (commercial, clinical) and runs the
# MAGIC segment-specific reconciliation pipeline against the curated + SAP Delta tables.
# MAGIC Output is written to reconciled_3pl_commercial_inventory / reconciled_3pl_clinical_inventory tables, both partitioned by (Segment, Year, Quarter).
# MAGIC
# MAGIC - **commercial** — full reconciliation pipeline implemented in `lib.processed.process_commercial`
# MAGIC - **clinical**   — placeholder; logic to be added later

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, DoubleType, BooleanType, IntegerType

from lib.processed.transformations import process_commercial, process_clinical
from common.config_loader import load_config
from common.dbfs_utils import dbfs_path


# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print(f"Environment : {env}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config and path resolution

# COMMAND ----------

processed_cfg = load_config(os.path.join(project_root, "config/processed.json"))

segments   = processed_cfg["segments"]
run_config = processed_cfg["run_config"]
run_mode   = run_config["run_mode"]

curated_table                = processed_cfg["curated_table"].format(env=env)
header_mapping_table         = processed_cfg["header_mapping_table"].format(env=env)
sap_report_table             = processed_cfg["sap_report_table"].format(env=env)
ebs_clinical_inventory_table = processed_cfg["ebs_clinical_inventory_table"].format(env=env)
reconciled_clinical_table    = processed_cfg["reconciled_clinical_table"].format(env=env)

print(f"Segments        : {segments}")
print(f"Run mode        : {run_mode}")
print(f"Curated table   : {curated_table}")
print(f"SAP table       : {sap_report_table}")

if run_mode == "historical":
    hist_year    = run_config.get("year")
    hist_quarter = run_config.get("quarter")
    if not hist_year or not hist_quarter:
        raise ValueError("run_mode is 'historical' but 'year' and/or 'quarter' not set in run_config")
    print(f"Historical load: year={hist_year}, quarter={hist_quarter}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Static reference files (segment-independent)

# COMMAND ----------

# All SAP enrichments (Material_Description, Batch_Number normalisation, numeric casts)
# are pre-applied in write_mapping_tables before writing to sap_report.
# Curated data carries Material_Description from the curated pipeline.
# No static reference files needed here.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Helpers

# COMMAND ----------

def resolve_quarter(segment: str):
    """Resolve year/quarter for a segment — historical from config, else latest in curated table."""
    if run_mode == "historical":
        return hist_year, hist_quarter

    latest = (
        spark.table(curated_table)
        .filter(col("Segment") == segment)
        .select("Year", "Quarter")
        .distinct()
        .orderBy(col("Year").desc(), col("Quarter").desc())
        .limit(1)
        .collect()
    )
    if not latest:
        return None, None
    return latest[0]["Year"], latest[0]["Quarter"]


def load_segment_data(segment: str, year: str, quarter: str):
    """Load curated, SAP, and header_mapping data for the given segment/quarter."""
    f = (col("Segment") == segment) & (col("Year") == year) & (col("Quarter") == quarter)
    curated_df  = spark.table(curated_table).filter(f).toPandas()
    sap_df      = spark.table(sap_report_table).filter(f).toPandas()
    header_df   = spark.table(header_mapping_table).filter(f).toPandas()
    return curated_df, sap_df, header_df


def load_clinical_segment_data(year: str, quarter: str):
    """Load curated (clinical), EBS staging, and header_mapping (clinical) for the quarter.

    Note: ebs_clinical_inventory is partitioned by (Year, Quarter) only, so
    the segment filter is not applied to it (the table is clinical-only).
    """
    curated_f = (col("Segment") == "clinical") & (col("Year") == year) & (col("Quarter") == quarter)
    ebs_f     = (col("Year") == year) & (col("Quarter") == quarter)
    curated_df = spark.table(curated_table).filter(curated_f).toPandas()
    ebs_df     = spark.table(ebs_clinical_inventory_table).filter(ebs_f).toPandas()
    header_df  = spark.table(header_mapping_table).filter(curated_f).toPandas()
    return curated_df, ebs_df, header_df


def finalise_and_write(combined_df: pd.DataFrame, target_table: str, segment: str, year: str, quarter: str) -> int:
    """Convert pandas DataFrame to Spark, enrich Plant_Name,
    cast columns, and write to the target Delta table."""
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


def finalise_and_write_clinical(combined_df: pd.DataFrame, target_table: str, year: str, quarter: str) -> int:
    """Stamp Processing_Timestamp and Segment, backfill EBS context for
    curated-only rows, cast columns, and write to the clinical reconciled
    Delta table."""
    spark_df = spark.createDataFrame(combined_df)
    spark_df = spark_df.withColumn(
        "Processing_Timestamp",
        lit(datetime.now().strftime("%Y-%m-%d %H:%M:00")),
    )
    spark_df = spark_df.withColumn("Segment", lit("clinical"))

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
        .option("replaceWhere", f"Segment = 'clinical' AND Year = '{year}' AND Quarter = '{quarter}'")
        .option("overwriteSchema", "false")
        .saveAsTable(target_table)
    )
    return spark_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process each segment

# COMMAND ----------

errors = []

for segment in segments:
    print(f"\n{'='*60}")
    print(f"Segment: {segment}")
    print(f"{'='*60}")

    year, quarter = resolve_quarter(segment)
    if not year or not quarter:
        print(f"  No data found for {segment} in {curated_table} — skipping")
        continue
    print(f"  Year={year}  Quarter={quarter}")

    # ------------------------------------------------------------------
    # Segment-specific branches
    # ------------------------------------------------------------------
    if segment == "commercial":
        try:
            curated_df, sap_df, header_df = load_segment_data(segment, year, quarter)
            print(f"  curated={curated_df.shape}  sap={sap_df.shape}  header_mapping={header_df.shape}")

            combined_df = process_commercial(
                curated_df        = curated_df,
                sap_df            = sap_df,
                header_mapping_df = header_df,
                year              = year,
                quarter           = quarter,
            )

            target_table = processed_cfg["reconciled_commercial_table"].format(env=env)
            rows = finalise_and_write(combined_df, target_table, segment, year, quarter)
            print(f"  Wrote {rows} rows to {target_table}")
        except Exception as e:
            print(f"  ERROR processing commercial: {e}")
            errors.append((segment, str(e)))

    elif segment == "clinical":
        try:
            curated_df, ebs_df, header_df = load_clinical_segment_data(year, quarter)
            print(f"  curated={curated_df.shape}  ebs={ebs_df.shape}  header_mapping={header_df.shape}")

            combined_df = process_clinical(
                curated_df        = curated_df,
                ebs_df            = ebs_df,
                header_mapping_df = header_df,
                year              = year,
                quarter           = quarter,
            )

            rows = finalise_and_write_clinical(combined_df, reconciled_clinical_table, year, quarter)
            print(f"  Wrote {rows} rows to {reconciled_clinical_table}")
        except Exception as e:
            print(f"  ERROR processing clinical: {e}")
            errors.append((segment, str(e)))

    else:
        print(f"  Unknown segment '{segment}' — no branch defined, skipping")
        continue

# COMMAND ----------

print(f"\n{'='*60}")
print(f"Processed layer complete")
print(f"  Segments processed : {len(segments)}")
print(f"  Errors             : {len(errors)}")
if errors:
    for seg, err in errors:
        print(f"    [{seg}] {err}")
