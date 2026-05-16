# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Load Mapping Cache
# MAGIC Loads all reference/mapping data into a `MappingDataCache` for the target quarter.
# MAGIC
# MAGIC Data source is derived from the environment:
# MAGIC - `prd` → `spark` (Spark SQL against Databricks tables)
# MAGIC - any other env → `starburst` (Starburst/Trino JDBC)

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

from lib.curated.data_cache import MappingFilePaths, RefFilePaths, load_mapping_files, load_file_mappings
from lib.discovery import discover_mapping_files, discover_sap_file, get_latest_completed_quarter
from common.config_loader import load_config
from common.dbfs_utils import dbfs_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

env         = dbutils.widgets.get("DATAENV")
segment     = dbutils.widgets.get("SEGMENT")
data_source = "spark" if env == "prd" else "starburst"

print(f"Environment  : {env}")
print(f"Segment      : {segment}")
print(f"Data source  : {data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config and path resolution

# COMMAND ----------

curated_cfg  = load_config(os.path.join(project_root, "config/curated.json"))

resolved_env     = "prod" if env == "prd" else env
src_root         = f"{curated_cfg['src_bkt_mount_point']}/{curated_cfg['src_data_dir'].format(env=resolved_env)}"
raw_root         = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['raw_data_dir']}"
segment_src_root = f"{src_root}/{segment}"
run_config = curated_cfg["run_config"]
run_mode   = run_config["run_mode"]

print(f"Run mode : {run_mode}")

# Resolve year/quarter
if run_mode == "historical":
    year    = run_config.get("year")
    quarter = run_config.get("quarter")
    if not year or not quarter:
        raise ValueError("run_mode is 'historical' but 'year' and/or 'quarter' not set in run_config")
    print(f"Historical load: year={year}, quarter={quarter}")
else:
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
# MAGIC #### Build file paths

# COMMAND ----------

# Mapping Excel files — discovered from the landing zone (source bucket)
mapping_paths = discover_mapping_files(dbutils, src_quarter_root)
print(f"Discovered mapping files: {mapping_paths}")

if not mapping_paths.get("api") or not mapping_paths.get("dp"):
    raise FileNotFoundError(f"Could not find api/dp mapping files under {src_quarter_root}/mapping_files")

# SAP report — processed CSV from the raw layer
sap_report_path = discover_sap_file(dbutils, raw_quarter_root)
if sap_report_path:
    print(f"SAP report   : {sap_report_path}")
else:
    print(f"SAP report   : not found under {raw_quarter_root}/sap_report_files — continuing without it")

# Fallback CSV files — static reference files on the mount
ref_base = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['ref_data_dir']}"
print(f"Reference base: {ref_base}")

file_paths = MappingFilePaths(
    api_mapping_file_path     = dbfs_path(mapping_paths["api"]),
    dp_mapping_file_path      = dbfs_path(mapping_paths["dp"]),
    header_mapping_sheet_name = curated_cfg["header_mapping_sheet_name"],
    item_mapping_sheet_name   = curated_cfg["item_mapping_sheet_name"],
    uom_mapping_sheet_name    = curated_cfg["uom_mapping_sheet_name"],
    sap_report_file_path      = dbfs_path(sap_report_path) if sap_report_path else None,
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

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 — Verify file-only datasets load correctly

# COMMAND ----------

file_cache = load_file_mappings(file_paths)

print("\n--- File mapping summary ---")
print(f"  header_mapping  : {file_cache.header_mapping_df.shape if file_cache.header_mapping_df is not None else 'None'}")
print(f"  pl_type_mapping : {file_cache.pl_type_mapping_df.shape if file_cache.pl_type_mapping_df is not None else 'None'}")
print(f"  item_mapping    : {file_cache.item_mapping_df.shape if file_cache.item_mapping_df is not None else 'None'}")
print(f"  uom_mapping     : {file_cache.uom_mapping_df.shape if file_cache.uom_mapping_df is not None else 'None'}")
print(f"  sap_report      : {file_cache.sap_report_df.shape if file_cache.sap_report_df is not None else 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 — Load full cache from configured data source

# COMMAND ----------

starburst_config = None
if data_source == "starburst":
    starburst_config = {
        'base_url'        : 'jdbc:trino://query.gilead.com:443',
        'username'        : dbutils.secrets.get(scope="pdm-gsc", key="starburst-username"),
        'password'        : dbutils.secrets.get(scope="pdm-gsc", key="starburst-password"),
        'default_catalog' : 'pdm',
        'default_schema'  : 'default',
    }

cache = load_mapping_files(
    file_paths       = file_paths,
    ref_paths        = ref_paths,
    year             = year,
    quarter          = quarter,
    data_source      = data_source,
    starburst_config = starburst_config,
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cache summary

# COMMAND ----------

fields = {
    "header_mapping"       : cache.header_mapping_df,
    "pl_type_mapping"      : cache.pl_type_mapping_df,
    "item_mapping"         : cache.item_mapping_df,
    "uom_mapping"          : cache.uom_mapping_df,
    "sap_report"           : cache.sap_report_df,
    "plant_name_mapping"   : cache.plant_name_mapping_df,
    "material_master"      : cache.material_master_df,
    "lot_no_master"        : cache.lot_no_master_df,
    "lot_no_mapping"       : cache.lot_no_mapping_df,
    "material_description" : cache.material_description_df,
    "uom_master"           : cache.uom_master_df,
    "unit_cost"            : cache.unit_cost_df,
    "material_type"        : cache.material_type_df,
    "gilead_receipts"      : cache.gil_receipts_df,
}

print(f"\n{'Dataset':<25} {'Shape':<15} {'Status'}")
print("-" * 55)
for name, df in fields.items():
    if df is not None and not df.empty:
        print(f"  {name:<23} {str(df.shape):<15} OK")
    else:
        print(f"  {name:<23} {'—':<15} MISSING")