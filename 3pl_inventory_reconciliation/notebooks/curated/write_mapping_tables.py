# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Write Mapping Tables
# MAGIC Loads the file-based mapping datasets (header mapping, item mapping, UOM mapping,
# MAGIC SAP report) for the target segment/quarter and writes each to a partitioned Delta table.
# MAGIC
# MAGIC Run this notebook once per segment/quarter after the raw processing notebook completes
# MAGIC and the mapping Excel files are in place.

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

import pandas as pd

from lib.curated.data_cache import MappingFilePaths, load_file_mappings
from lib.discovery import discover_mapping_files, discover_sap_file
from common.config_loader import load_config
from common.dbfs_utils import dbfs_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

env     = dbutils.widgets.get("DATAENV")
segment = dbutils.widgets.get("SEGMENT")

print(f"Environment : {env}")
print(f"Segment     : {segment}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config and path resolution

# COMMAND ----------

curated_cfg  = load_config(os.path.join(project_root, "config/curated.json"))

src_root         = f"{curated_cfg['src_bkt_mount_point']}/{curated_cfg['src_data_dir'].format(env=env)}"
raw_root         = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['raw_data_dir']}"
segment_src_root = f"{src_root}/{segment}"
run_mode         = curated_cfg["run_mode"]

header_mapping_table = curated_cfg["header_mapping_table"].format(env=env)
item_mapping_table   = curated_cfg["item_mapping_table"].format(env=env)
uom_mapping_table    = curated_cfg["uom_mapping_table"].format(env=env)
sap_report_table     = curated_cfg["sap_report_table"].format(env=env)

print(f"Run mode : {run_mode}")

# Resolve year/quarter
if run_mode == "historical":
    year    = curated_cfg.get("year")
    quarter = curated_cfg.get("quarter")
    if not year or not quarter:
        raise ValueError("run_mode is 'historical' but 'year' and/or 'quarter' not set in config")
    print(f"Historical load: year={year}, quarter={quarter}")
else:
    from lib.discovery import get_latest_completed_quarter
    year, quarter = get_latest_completed_quarter(dbutils, segment_src_root)
    if not year or not quarter:
        raise RuntimeError(f"No completed quarter found under {segment_src_root}")
    print(f"Auto-detected latest completed quarter: year={year}, quarter={quarter}")

src_quarter_root = f"{segment_src_root}/{year}/{quarter}"
raw_quarter_root = f"{raw_root}/{segment}/{year}/{quarter}"

print(f"Src quarter root : {src_quarter_root}")
print(f"Raw quarter root : {raw_quarter_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Discover files and load cache

# COMMAND ----------

mapping_paths = discover_mapping_files(dbutils, src_quarter_root)
print(f"Mapping files: {mapping_paths}")

if not mapping_paths.get("api") or not mapping_paths.get("dp"):
    raise FileNotFoundError(f"Could not find api/dp mapping files under {src_quarter_root}/mapping_files")

sap_report_path = discover_sap_file(dbutils, raw_quarter_root)
if sap_report_path:
    print(f"SAP report   : {sap_report_path}")
else:
    print(f"SAP report   : not found under {raw_quarter_root}/sap_report_files — will skip sap_report table")

file_paths = MappingFilePaths(
    api_mapping_file_path     = dbfs_path(mapping_paths["api"]),
    dp_mapping_file_path      = dbfs_path(mapping_paths["dp"]),
    header_mapping_sheet_name = "Header Mappings",
    item_mapping_sheet_name   = "Item Mapping",
    uom_mapping_sheet_name    = "UOM Mapping",
    sap_report_file_path      = dbfs_path(sap_report_path) if sap_report_path else None,
)

cache = load_file_mappings(file_paths)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Delta tables

# COMMAND ----------

def _write_delta(df: pd.DataFrame, table: str, label: str) -> None:
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


print(f"\nWriting mapping tables for {segment} / {year} / {quarter}")

_write_delta(cache.header_mapping_df, header_mapping_table, "header_mapping")
_write_delta(cache.item_mapping_df,   item_mapping_table,   "item_mapping")
_write_delta(cache.uom_mapping_df,    uom_mapping_table,    "uom_mapping")

if cache.sap_report_df is not None:
    _write_delta(cache.sap_report_df, sap_report_table, "sap_report")
else:
    print(f"  sap_report: skipped (no SAP report found)")

print("\nDone.")
