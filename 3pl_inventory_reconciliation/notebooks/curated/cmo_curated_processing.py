# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Curated Processing
# MAGIC Reads raw CSV files for the target quarter, applies curation transformations
# MAGIC (header mapping, material/lot/UOM mapping, cost enrichment, validation flags),
# MAGIC and writes one curated CSV per sheet per 3PL site to the curated layer.
# MAGIC
# MAGIC **DATA_SOURCE options:**
# MAGIC - `spark` — prod: queries run via Spark SQL against Databricks tables
# MAGIC - `starburst` — dev: queries run via Starburst/Trino JDBC
# MAGIC - `file` — local fallback only, skips all live queries

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

import pandas as pd
import numpy as np

from lib.curated.data_cache import MappingFilePaths, load_mapping_files
from lib.curated.curation_utils import curated_processing, build_header_mapping
from lib.raw.discovery import (
    get_latest_completed_quarter,
    discover_mapping_files,
    discover_sap_file,
    discover_all_raw_csvs,
)
from common.config_loader import load_config
from common.dbfs_utils import dbfs_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

env              = dbutils.widgets.get("DATAENV")
year_override    = dbutils.widgets.get("YEAR").strip()
quarter_override = dbutils.widgets.get("QUARTER").strip()
segment          = dbutils.widgets.get("SEGMENT")
data_source      = dbutils.widgets.get("DATA_SOURCE").strip().lower()

print(f"Environment : {env}")
print(f"Segment     : {segment}")
print(f"Year        : {year_override or '(auto-detect)'}")
print(f"Quarter     : {quarter_override or '(auto-detect)'}")
print(f"Data source : {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config and path resolution

# COMMAND ----------

config      = load_config(os.path.join(project_root, "config/raw.json"))
curated_cfg = load_config(os.path.join(project_root, "config/curated.json"))

resolved_env = "prod" if env == "prd" else env

src_root     = f"{config['src_bkt_mount_point']}/{config['src_data_dir'].format(env=resolved_env)}"
raw_root     = f"{config['tgt_bkt_mount_point']}/{config['tgt_data_dir']}"
curated_root = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['curated_data_dir']}"
ref_base     = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['raw_data_dir']}"

print(f"Source root  : {src_root}")
print(f"Raw root     : {raw_root}")
print(f"Curated root : {curated_root}")

# COMMAND ----------

# Resolve year/quarter from the source landing zone (same as raw notebook)
segment_src_root = f"{src_root}/{segment}"

if year_override and quarter_override:
    year, quarter = year_override, quarter_override
    print(f"Using override: year={year}, quarter={quarter}")
else:
    year, quarter = get_latest_completed_quarter(dbutils, segment_src_root)
    if not year or not quarter:
        raise RuntimeError(f"No completed quarter found under {segment_src_root}")
    print(f"Auto-detected latest completed quarter: year={year}, quarter={quarter}")

src_quarter_root     = f"{segment_src_root}/{year}/{quarter}"
raw_quarter_root     = f"{raw_root}/{segment}/{year}/{quarter}"
curated_quarter_root = f"{curated_root}/{segment}/{year}/{quarter}"

print(f"Source quarter : {src_quarter_root}")
print(f"Raw quarter    : {raw_quarter_root}")
print(f"Curated output : {curated_quarter_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build mapping file paths

# COMMAND ----------

mapping_paths = discover_mapping_files(dbutils, src_quarter_root)
print(f"Mapping files: {mapping_paths}")

if not mapping_paths.get("api") or not mapping_paths.get("dp"):
    raise FileNotFoundError(f"Could not find api/dp mapping files under {src_quarter_root}/mapping_files")

sap_report_path = discover_sap_file(dbutils, src_quarter_root)
if not sap_report_path:
    raise FileNotFoundError(f"No SAP report found under {src_quarter_root}/sap_report_files")
print(f"SAP report   : {sap_report_path}")

file_paths = MappingFilePaths(
    api_mapping_file_path          = dbfs_path(mapping_paths["api"]),
    dp_mapping_file_path           = dbfs_path(mapping_paths["dp"]),
    header_mapping_sheet_name      = config["header_mapping_sheet_name"],
    item_mapping_sheet_name        = "Item Mapping",
    uom_mapping_sheet_name         = "UOM Mapping",
    sap_report_file_path           = dbfs_path(sap_report_path),
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

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load mapping cache

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

mapping_cache = load_mapping_files(
    file_paths       = file_paths,
    year             = year,
    quarter          = quarter,
    data_source      = data_source,
    starburst_config = starburst_config,
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reset curated output for this quarter (idempotent rerun)

# COMMAND ----------

print(f"Removing prior curated output at: {curated_quarter_root}")
dbutils.fs.rm(f"dbfs:{curated_quarter_root}", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pre-build header mapping

# COMMAND ----------

header_mapping = build_header_mapping(mapping_cache.header_mapping_df)
print(f"Header mapping built — {len(header_mapping)} site/sheet key(s): {sorted(header_mapping.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Discover raw CSV files to process

# COMMAND ----------

raw_files_by_site = discover_all_raw_csvs(dbutils, raw_quarter_root)
total_files = sum(len(v) for v in raw_files_by_site.values())
print(f"Discovered {len(raw_files_by_site)} site(s), {total_files} CSV file(s) to curate")
for site_id, paths in raw_files_by_site.items():
    for p in paths:
        print(f"  {site_id}: {os.path.basename(p)}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process each site

# COMMAND ----------

errors = []

for site_id, raw_paths in raw_files_by_site.items():
    print(f"\n{'='*60}")
    print(f"Site: {site_id}  ({len(raw_paths)} file(s))")

    for raw_path in raw_paths:
        file_stem = os.path.splitext(os.path.basename(raw_path))[0]
        out_dir   = f"{curated_quarter_root}/3pl_files/{site_id}"
        out_path  = f"{out_dir}/{file_stem}_curated.csv"

        print(f"\n  Source : {os.path.basename(raw_path)}")
        print(f"  Output : {out_path}")
        try:
            raw_df     = pd.read_csv(dbfs_path(raw_path), dtype=str)
            curated_df = curated_processing(raw_df, raw_path, mapping_cache, header_mapping)
            dbutils.fs.mkdirs(f"dbfs:{out_dir}")
            curated_df.to_csv(dbfs_path(out_path), index=False)
            print(f"  Wrote {curated_df.shape[0]} rows to {out_path}")
        except Exception as e:
            print(f"  ERROR: {e}")
            errors.append((site_id, os.path.basename(raw_path), str(e)))

# COMMAND ----------

print(f"\n{'='*60}")
print(f"Curated processing complete — {segment} {year} {quarter}")
print(f"  Sites processed : {len(raw_files_by_site)}")
print(f"  Files processed : {total_files}")
print(f"  Errors          : {len(errors)}")
if errors:
    print("  Failed files:")
    for site_id, fname, err in errors:
        print(f"    [{site_id}] {fname}: {err}")
