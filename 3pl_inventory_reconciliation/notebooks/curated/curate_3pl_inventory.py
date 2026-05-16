# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Curated Processing
# MAGIC Reads raw CSV files for the target quarter, applies curation transformations
# MAGIC (header mapping, material/lot/UOM mapping, cost enrichment, validation flags),
# MAGIC and writes results to the curated Delta table.
# MAGIC
# MAGIC Runs for all segments defined in curated.json. Results from all segments
# MAGIC are written in a single Delta overwrite scoped to the processed year/quarter.
# MAGIC
# MAGIC **DATA_SOURCE options:**
# MAGIC - `spark` — prod: queries run via Spark SQL against Databricks tables
# MAGIC - `starburst` — dev: queries run via Starburst/Trino JDBC
# MAGIC - `file` — local fallback only, skips all live queries

# COMMAND ----------

import os
import sys
import traceback

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

import pandas as pd

from lib.curated.data_cache import MappingFilePaths, RefFilePaths, load_mapping_files
from lib.curated.transformations import curated_processing
from lib.discovery import (
    get_latest_completed_quarter,
    discover_mapping_files,
    discover_all_raw_csvs,
)
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

curated_cfg  = load_config(os.path.join(project_root, "config/curated.json"))
data_source  = "spark" if env == "prd" else "starburst"

src_root      = f"{curated_cfg['src_bkt_mount_point']}/{curated_cfg['src_data_dir'].format(env=env)}"
raw_root      = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['raw_data_dir']}"
ref_base      = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['ref_data_dir']}"
curated_table = curated_cfg["curated_table"].format(env=env)
segments   = curated_cfg["segments"]
run_config = curated_cfg["run_config"]
run_mode   = run_config["run_mode"]

print(f"Source root    : {src_root}")
print(f"Raw root       : {raw_root}")
print(f"Curated table  : {curated_table}")
print(f"Segments       : {segments}")
print(f"Run mode       : {run_mode}")

if run_mode == "historical":
    year    = run_config.get("year")
    quarter = run_config.get("quarter")
    if not year or not quarter:
        raise ValueError("run_mode is 'historical' but 'year' and/or 'quarter' not set in run_config")
    print(f"Historical load: year={year}, quarter={quarter}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Starburst config (if applicable)

# COMMAND ----------

starburst_config = None
if data_source == "starburst":
    starburst_config = {
        "base_url"        : "jdbc:trino://query.gilead.com:443",
        "username"        : dbutils.secrets.get(scope="pdm-gsc", key="starburst-username"),
        "password"        : dbutils.secrets.get(scope="pdm-gsc", key="starburst-password"),
        "default_catalog" : "pdm",
        "default_schema"  : "default",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process each segment

# COMMAND ----------

all_curated_dfs = []
all_errors      = []

for segment in segments:
    print(f"\n{'='*60}")
    print(f"Segment: {segment}")
    print(f"{'='*60}")

    segment_raw_root = f"{raw_root}/{segment}"
    segment_src_root = f"{src_root}/{segment}"

    # ------------------------------------------------------------------
    # Resolve quarter — detected from raw layer output
    # ------------------------------------------------------------------
    if run_mode == "historical":
        print(f"  Using historical: year={year}, quarter={quarter}")
    else:
        year, quarter = get_latest_completed_quarter(dbutils, segment_raw_root)
        if not year or not quarter:
            print(f"  No completed quarter found under {segment_raw_root} — skipping segment")
            continue
        print(f"  Auto-detected latest completed quarter: year={year}, quarter={quarter}")

    src_quarter_root = f"{segment_src_root}/{year}/{quarter}"
    raw_quarter_root = f"{segment_raw_root}/{year}/{quarter}"

    # ------------------------------------------------------------------
    # Discover mapping and SAP files
    # ------------------------------------------------------------------
    mapping_paths = discover_mapping_files(dbutils, src_quarter_root)
    print(f"Mapping files: {mapping_paths}")

    if not mapping_paths.get("api") or not mapping_paths.get("dp"):
        print(f"  Could not find api/dp mapping files under {src_quarter_root}/mapping_files — skipping segment")
        continue

    # ------------------------------------------------------------------
    # Build mapping file paths and load cache
    # ------------------------------------------------------------------
    file_paths = MappingFilePaths(
        api_mapping_file_path     = dbfs_path(mapping_paths["api"]),
        dp_mapping_file_path      = dbfs_path(mapping_paths["dp"]),
        header_mapping_sheet_name = curated_cfg["header_mapping_sheet_name"],
        item_mapping_sheet_name   = curated_cfg["item_mapping_sheet_name"],
        uom_mapping_sheet_name    = curated_cfg["uom_mapping_sheet_name"],
    )

    ref_paths = RefFilePaths(
        plant_name_mapping_file_path   = dbfs_path(f"{ref_base}/plant_name_mapping.csv"),
        material_master_file_path      = dbfs_path(f"{ref_base}/material_master.csv"),
        lot_no_master_file_path        = dbfs_path(f"{ref_base}/lot_no_master.csv"),
        lot_no_mapping_file_path       = dbfs_path(f"{ref_base}/lot_no_mapping.csv"),
        material_description_file_path = dbfs_path(f"{ref_base}/material_description.csv"),
        uom_master_file_path           = dbfs_path(f"{ref_base}/uom_master.csv"),
        unit_cost_file_path            = dbfs_path(f"{ref_base}/unit_cost.csv"),
        material_type_file_path        = dbfs_path(f"{ref_base}/material_type.csv"),
        gil_receipts_file_path         = dbfs_path(f"{ref_base}/gilead_receipts.csv"),
    )

    mapping_cache  = load_mapping_files(
        file_paths       = file_paths,
        ref_paths        = ref_paths,
        year             = year,
        quarter          = quarter,
        data_source      = data_source,
        starburst_config = starburst_config,
    )
    print(f"Header mapping built — {len(mapping_cache.header_mapping)} site/sheet key(s): {sorted(mapping_cache.header_mapping.keys())}")

    # ------------------------------------------------------------------
    # Discover and process raw CSV files
    # ------------------------------------------------------------------
    raw_files_by_site = discover_all_raw_csvs(dbutils, raw_quarter_root)
    total_files = sum(len(v) for v in raw_files_by_site.values())
    print(f"Discovered {len(raw_files_by_site)} site(s), {total_files} CSV file(s) to curate")

    for site_id, raw_paths in raw_files_by_site.items():
        print(f"\n  Site: {site_id}  ({len(raw_paths)} file(s))")
        for raw_path in raw_paths:
            print(f"    Source: {os.path.basename(raw_path)}")
            try:
                raw_df     = pd.read_csv(dbfs_path(raw_path), dtype=str)
                curated_df = curated_processing(raw_df, raw_path, mapping_cache, segment)
                all_curated_dfs.append(curated_df)
                print(f"    Processed {curated_df.shape[0]} rows")
            except Exception as e:
                print(f"    ERROR: {os.path.basename(raw_path)}: {e}")
                traceback.print_exc()
                all_errors.append((segment, site_id, os.path.basename(raw_path), str(e)))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Delta table (idempotent — overwrites this year/quarter partition)

# COMMAND ----------

if all_curated_dfs:
    all_curated = pd.concat(all_curated_dfs, ignore_index=True)
    spark_df = spark.createDataFrame(all_curated)
    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"Year = '{year}' AND Quarter = '{quarter}'")
        .option("overwriteSchema", "false")
        .saveAsTable(curated_table)
    )
    print(f"Wrote {len(all_curated)} rows to {curated_table}")
else:
    print("No results to write — all files failed processing")

# COMMAND ----------

print(f"\n{'='*60}")
print(f"Curated processing complete")
print(f"  Segments processed : {len(segments)}")
print(f"  Rows written       : {len(all_curated) if all_curated_dfs else 0}")
print(f"  Errors             : {len(all_errors)}")
if all_errors:
    print("  Failed files:")
    for segment, site_id, fname, err in all_errors:
        print(f"    [{segment}/{site_id}] {fname}: {err}")
