# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Write Mapping Tables
# MAGIC Loads the file-based mapping datasets (header mapping, item mapping, UOM mapping,
# MAGIC SAP report) for each configured segment/quarter and writes each to a partitioned
# MAGIC Delta table.
# MAGIC
# MAGIC Run this notebook once per quarter after the raw processing notebook completes
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

from lib.curated.data_cache import MappingFilePaths, RefFilePaths, load_mapping_files
from lib.curated.transformations import match_gilead_receipts, remove_decimal_if_all_zeros
from lib.discovery import discover_mapping_files, discover_sap_file
from common.config_loader import load_config
from common.dbfs_utils import dbfs_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

env         = dbutils.widgets.get("DATAENV")
data_source = "spark" if env == "prd" else "starburst"

print(f"Environment  : {env}")
print(f"Data source  : {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config and path resolution

# COMMAND ----------

curated_cfg = load_config(os.path.join(project_root, "config/curated.json"))

resolved_env = "prod" if env == "prd" else env
src_root     = f"{curated_cfg['src_bkt_mount_point']}/{curated_cfg['src_data_dir'].format(env=resolved_env)}"
raw_root     = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['raw_data_dir']}"
segments     = curated_cfg["segments"]
run_config   = curated_cfg["run_config"]
run_mode     = run_config["run_mode"]

header_mapping_table = curated_cfg["header_mapping_table"].format(env=env)
item_mapping_table   = curated_cfg["item_mapping_table"].format(env=env)
uom_mapping_table    = curated_cfg["uom_mapping_table"].format(env=env)
sap_report_table     = curated_cfg["sap_report_table"].format(env=env)

ref_base = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['ref_data_dir']}"

print(f"Segments : {segments}")
print(f"Run mode : {run_mode}")

# Starburst config — only used in non-prod environments
starburst_config = None
if data_source == "starburst":
    starburst_config = {
        "base_url"        : "jdbc:trino://query.gilead.com:443",
        "username"        : dbutils.secrets.get(scope="pdm-gsc", key="starburst-username"),
        "password"        : dbutils.secrets.get(scope="pdm-gsc", key="starburst-password"),
        "default_catalog" : "pdm",
        "default_schema"  : "default",
    }

if run_mode == "historical":
    hist_year    = run_config.get("year")
    hist_quarter = run_config.get("quarter")
    if not hist_year or not hist_quarter:
        raise ValueError("run_mode is 'historical' but 'year' and/or 'quarter' not set in run_config")
    print(f"Historical load: year={hist_year}, quarter={hist_quarter}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write helper

# COMMAND ----------

def _write_delta(df: pd.DataFrame, table: str, label: str, segment: str, year: str, quarter: str) -> None:
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
# MAGIC #### Process each segment

# COMMAND ----------

from lib.discovery import get_latest_completed_quarter

all_errors = []

for segment in segments:
    print(f"\n{'='*60}")
    print(f"Segment: {segment}")
    print(f"{'='*60}")

    segment_src_root = f"{src_root}/{segment}"
    raw_quarter_root_base = f"{raw_root}/{segment}"

    # ------------------------------------------------------------------
    # Resolve year/quarter
    # ------------------------------------------------------------------
    if run_mode == "historical":
        year    = hist_year
        quarter = hist_quarter
        print(f"  Using historical: year={year}, quarter={quarter}")
    else:
        year, quarter = get_latest_completed_quarter(dbutils, segment_src_root)
        if not year or not quarter:
            print(f"  No completed quarter found under {segment_src_root} — skipping segment")
            all_errors.append((segment, "quarter detection", "No completed quarter found"))
            continue
        print(f"  Auto-detected latest completed quarter: year={year}, quarter={quarter}")

    src_quarter_root = f"{segment_src_root}/{year}/{quarter}"
    raw_quarter_root = f"{raw_quarter_root_base}/{year}/{quarter}"

    print(f"  Src quarter root : {src_quarter_root}")
    print(f"  Raw quarter root : {raw_quarter_root}")

    # ------------------------------------------------------------------
    # Discover files
    # ------------------------------------------------------------------
    try:
        mapping_paths = discover_mapping_files(dbutils, src_quarter_root)
        print(f"  Mapping files: {mapping_paths}")

        if not mapping_paths.get("api") or not mapping_paths.get("dp"):
            print(f"  Could not find api/dp mapping files under {src_quarter_root}/mapping_files — skipping segment")
            all_errors.append((segment, "file discovery", "Missing api/dp mapping files"))
            continue

        sap_report_path = discover_sap_file(dbutils, raw_quarter_root)
        if sap_report_path:
            print(f"  SAP report   : {sap_report_path}")
        else:
            print(f"  SAP report   : not found under {raw_quarter_root}/sap_report_files — will skip sap_report table")

        file_paths = MappingFilePaths(
            api_mapping_file_path     = dbfs_path(mapping_paths["api"]),
            dp_mapping_file_path      = dbfs_path(mapping_paths["dp"]),
            header_mapping_sheet_name = "Header Mapping",
            item_mapping_sheet_name   = "Item Mapping",
            uom_mapping_sheet_name    = "UOM Mapping",
            sap_report_file_path      = dbfs_path(sap_report_path) if sap_report_path else None,
        )

        # Only request what's needed for SAP enrichment at write time
        ref_paths = RefFilePaths(
            gil_receipts_file_path = dbfs_path(f"{ref_base}/gilead_receipts.csv"),
        )

        cache = load_mapping_files(
            file_paths       = file_paths,
            ref_paths        = ref_paths,
            year             = year,
            quarter          = quarter,
            data_source      = data_source,
            starburst_config = starburst_config,
        )

    except Exception as e:
        print(f"  ERROR loading files for {segment}: {e}")
        all_errors.append((segment, "file loading", str(e)))
        continue

    # ------------------------------------------------------------------
    # Write to Delta tables
    # ------------------------------------------------------------------
    print(f"\n  Writing mapping tables for {segment} / {year} / {quarter}")

    _write_delta(cache.header_mapping_df, header_mapping_table, "header_mapping", segment, year, quarter)
    _write_delta(cache.item_mapping_df,   item_mapping_table,   "item_mapping",   segment, year, quarter)
    _write_delta(cache.uom_mapping_df,    uom_mapping_table,    "uom_mapping",    segment, year, quarter)

    if cache.sap_report_df is not None:
        sap_df = cache.sap_report_df.copy()
        if cache.gil_receipts_df is not None:
            sap_df = match_gilead_receipts(sap_df, cache.gil_receipts_df)
            print(f"  sap_report: Gilead_Receipts enrichment applied")
        else:
            print(f"  sap_report: Gilead receipts not available — Gilead_Receipts will be null")
        sap_df["Batch_Number"]                  = sap_df["Batch_Number"].apply(remove_decimal_if_all_zeros)
        sap_df["Stock_Quantity__Base_UOM_"]     = pd.to_numeric(sap_df["Stock_Quantity__Base_UOM_"],     errors="coerce")
        sap_df["Group_Valuation_Standard_Cost"] = pd.to_numeric(sap_df["Group_Valuation_Standard_Cost"], errors="coerce")
        _write_delta(sap_df, sap_report_table, "sap_report", segment, year, quarter)
    else:
        print(f"  sap_report: skipped (no SAP report found)")

# COMMAND ----------

print(f"\n{'='*60}")
print(f"Write mapping tables complete")
print(f"  Segments processed : {len(segments)}")
print(f"  Errors             : {len(all_errors)}")
if all_errors:
    print("  Failed segments:")
    for seg, stage, err in all_errors:
        print(f"    [{seg}] {stage}: {err}")
