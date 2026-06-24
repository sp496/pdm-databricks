# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Stage Commercial SAP Report
# MAGIC Reads the SAP report CSV produced by the raw ingestion
# MAGIC (`raw/commercial/{year}/{quarter}/sap_report_files/`), applies the standard
# MAGIC pre-write preprocessing, filters to the relevant plants, and writes to the
# MAGIC `sap_report` Delta table (partition: Segment/Year/Quarter).
# MAGIC
# MAGIC Run order: after `ingest_3pl_inventory` for the target quarter, in
# MAGIC parallel with `write_mapping_tables`. Must complete before
# MAGIC `curate_commercial_inventory` and the processed-layer reconciliation.
# MAGIC
# MAGIC The clinical EBS snapshot is staged separately by the standalone
# MAGIC `stage_clinical_inventory` notebook (quarterly cadence).

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

import pandas as pd

from lib.curated.transformations import remove_decimal_if_all_zeros
from lib.curated.data_cache import load_sap_report_file
from lib.discovery import (
    resolve_quarter_from_source,
    discover_3pl_files,
    discover_sap_file,
)
from common.config_loader import load_config
from common.dbfs_utils import dbfs_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

env         = dbutils.widgets.get("DATAENV")
data_source = "spark" if env == "prd" else "starburst"

print(f"Environment : {env}")
print(f"Data source : {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config and path resolution

# COMMAND ----------

curated_cfg  = load_config(os.path.join(project_root, "config/curated.json"))
resolved_env = "prod" if env == "prd" else env

src_root = f"{curated_cfg['src_bkt_mount_point']}/{curated_cfg['src_data_dir'].format(env=resolved_env)}"
raw_root = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['raw_data_dir']}"

sap_report_table = curated_cfg["sap_report_table"].format(env=env)

print(f"Source root      : {src_root}")
print(f"Raw root         : {raw_root}")
print(f"SAP report table : {sap_report_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Delta write helper

# COMMAND ----------

def _write_delta(df: pd.DataFrame, table: str, label: str, segment: str, year: str, quarter: str) -> None:
    """Stamp Segment/Year/Quarter, convert to Spark, and overwrite the
    matching (Segment, Year, Quarter) partition of the Delta table."""
    df = df.copy()
    df["Segment"] = segment
    df["Year"]    = year
    df["Quarter"] = quarter
    spark_df = spark.createDataFrame(df)
    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"Segment = '{segment}' AND Year = '{year}' AND Quarter = '{quarter}'")
        .option("overwriteSchema", "false")
        .saveAsTable(table)
    )
    print(f"  {label}: wrote {len(df)} rows to {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stage commercial SAP report

# COMMAND ----------

segment_c             = "commercial"
segment_src_root_c    = f"{src_root}/{segment_c}"
segment_raw_root_c    = f"{raw_root}/{segment_c}"

print(f"\n{'='*60}")
print(f"Segment: {segment_c}")
print(f"{'='*60}")

# Resolve year/quarter per the segment's run_mode (historical | latest_completed | latest_available)
run_mode, year_c, quarter_c = resolve_quarter_from_source(curated_cfg, segment_c, dbutils, segment_src_root_c)
print(f"  Run mode: {run_mode}")
if run_mode != "historical" and (not year_c or not quarter_c):
    raise ValueError(
        f"No completed quarter found under {segment_src_root_c} — "
        f"run ingest_3pl_inventory for commercial before staging"
    )
print(f"  Resolved quarter: year={year_c}, quarter={quarter_c}")

src_quarter_root_c = f"{segment_src_root_c}/{year_c}/{quarter_c}"
raw_quarter_root_c = f"{segment_raw_root_c}/{year_c}/{quarter_c}"
print(f"  Src quarter root: {src_quarter_root_c}")
print(f"  Raw quarter root: {raw_quarter_root_c}")


# Discover the SAP report CSV (raw layer)
sap_report_path = discover_sap_file(dbutils, raw_quarter_root_c)
if not sap_report_path:
    raise ValueError(
        f"No SAP report found under {raw_quarter_root_c}/sap_report_files — "
        f"run ingest_3pl_inventory first"
    )
print(f"  SAP report: {sap_report_path}")

# Load + preprocess
sap_df = load_sap_report_file(dbfs_path(sap_report_path))
sap_df["Batch_Number"]                  = sap_df["Batch_Number"].apply(remove_decimal_if_all_zeros)
sap_df["Stock_Quantity__Base_UOM_"]     = pd.to_numeric(sap_df["Stock_Quantity__Base_UOM_"],     errors="coerce")
sap_df["Group_Valuation_Standard_Cost"] = pd.to_numeric(sap_df["Group_Valuation_Standard_Cost"], errors="coerce")

# Safety net: drop any fully-duplicate rows so an identical line in the source
# file can't double-count when the reconciliation aggregates SAP per
# (Plant, Material, Batch).
pre_dedup_rows = len(sap_df)
sap_df = sap_df.drop_duplicates()
if len(sap_df) != pre_dedup_rows:
    print(f"  Dropped {pre_dedup_rows - len(sap_df)} duplicate SAP row(s) → {len(sap_df)}")

_write_delta(sap_df, sap_report_table, "sap_report", segment_c, year_c, quarter_c)

# COMMAND ----------

print(f"\n{'='*60}")
print("SAP report staging complete")
print(f"  Commercial SAP : Year={year_c} Quarter={quarter_c}")
