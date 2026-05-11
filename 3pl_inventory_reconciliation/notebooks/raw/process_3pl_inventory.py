# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 3PL Inventory — Raw Processing
# MAGIC Reads 3PL inventory Excel files for the target quarter, cleans them, and writes one CSV per sheet to the raw layer.

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

from lib.raw.discovery import (
    get_latest_completed_quarter,
    discover_3pl_files,
    discover_mapping_files,
)
from lib.raw.mapping_loader import load_quarter_mappings
from lib.raw.excel_processor import process_3pl_file
from common.config_loader import load_config
from common.dbfs_utils import dbfs_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

env     = dbutils.widgets.get("DATAENV")
year_override    = dbutils.widgets.get("YEAR").strip()
quarter_override = dbutils.widgets.get("QUARTER").strip()
print(f"Environment: {env}, year_override={year_override or '(none)'}, quarter_override={quarter_override or '(none)'}")

# COMMAND ----------

config = load_config(os.path.join(project_root, "config/raw.json"))
resolved_env = "prod" if env == "prd" else env

src_bkt_mount_point = config["src_bkt_mount_point"]
tgt_bkt_mount_point = config["tgt_bkt_mount_point"]
src_data_dir        = config["src_data_dir"].format(env=resolved_env)
tgt_data_dir        = config["tgt_data_dir"]
segments          = config["segments"]
header_sheet_name = config["header_mapping_sheet_name"]

src_root = f"{src_bkt_mount_point}/{src_data_dir}"
tgt_root = f"{tgt_bkt_mount_point}/{tgt_data_dir}"
print(f"Source root: {src_root}")
print(f"Target root: {tgt_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resolve quarter to process

# COMMAND ----------

if year_override and quarter_override:
    year, quarter = year_override, quarter_override
    print(f"Using override: year={year}, quarter={quarter}")
else:
    year, quarter = get_latest_completed_quarter(dbutils, src_root)
    if not year or not quarter:
        raise RuntimeError(f"No completed quarter found under {src_root}")
    print(f"Using latest completed quarter: year={year}, quarter={quarter}")

quarter_root = f"{src_root}/{year}/{quarter}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reset target raw layer for this quarter (idempotent rerun)

# COMMAND ----------

target_quarter_root = f"{tgt_root}/{year}/{quarter}"
print(f"Removing prior raw output at: {target_quarter_root}")
dbutils.fs.rm(f"dbfs:{target_quarter_root}", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load mapping files

# COMMAND ----------

mapping_paths = discover_mapping_files(dbutils, quarter_root, segments)
print(f"Mapping paths: {mapping_paths}")

resolved_mapping_paths = {
    seg: {k: dbfs_path(v) if v else v for k, v in paths.items()}
    for seg, paths in mapping_paths.items()
}
header_mapping_df, site_sheet_mapping = load_quarter_mappings(
    resolved_mapping_paths, header_sheet_name
)
print(f"3PLs in mapping: {sorted(site_sheet_mapping.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process 3PL inventory files

# COMMAND ----------

files_3pl = discover_3pl_files(dbutils, quarter_root, segments)
print(f"Discovered {len(files_3pl)} 3PL inventory files")

for entry in files_3pl:
    segment = entry["segment"]
    site_id = entry["site_id"]
    src_path = entry["path"]
    out_dir = f"{target_quarter_root}/3pl_files/{segment}/{site_id}"
    print(f"\nProcessing {segment}/{site_id}")
    try:
        sheets = process_3pl_file(dbfs_path(src_path), site_sheet_mapping.get(site_id))
        dbutils.fs.mkdirs(f"dbfs:{out_dir}")
        for sheet_slug, data in sheets.items():
            out_path = dbfs_path(f"{out_dir}/{sheet_slug}.csv")
            data.to_csv(out_path, index=False, encoding="utf-8")
            print(f"    Wrote {out_path}")
    except Exception as e:
        print(f"  ERROR processing {segment}/{site_id}: {e}")

# COMMAND ----------

print(f"3PL raw processing complete for {year} {quarter}")
