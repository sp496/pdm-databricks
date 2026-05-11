# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Raw Processing
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
# MAGIC #### Parameters

# COMMAND ----------

env              = dbutils.widgets.get("DATAENV")
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
segments            = config["segments"]
header_sheet_name   = config["header_mapping_sheet_name"]

src_root = f"{src_bkt_mount_point}/{src_data_dir}"
tgt_root = f"{tgt_bkt_mount_point}/{tgt_data_dir}"
print(f"Source root: {src_root}")
print(f"Target root: {tgt_root}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process each segment

# COMMAND ----------

for segment in segments:
    print(f"\n{'='*60}")
    print(f"Segment: {segment}")
    print(f"{'='*60}")

    segment_src_root = f"{src_root}/{segment}"

    # ------------------------------------------------------------------
    # Resolve quarter to process
    # ------------------------------------------------------------------
    if year_override and quarter_override:
        year, quarter = year_override, quarter_override
        print(f"Using override: year={year}, quarter={quarter}")
    else:
        year, quarter = get_latest_completed_quarter(dbutils, segment_src_root)
        if not year or not quarter:
            print(f"  No completed quarter found under {segment_src_root} — skipping segment")
            continue
        print(f"Using latest completed quarter: year={year}, quarter={quarter}")

    quarter_root        = f"{segment_src_root}/{year}/{quarter}"
    target_quarter_root = f"{tgt_root}/{segment}/{year}/{quarter}"

    # ------------------------------------------------------------------
    # Reset target raw layer for this quarter (idempotent rerun)
    # ------------------------------------------------------------------
    print(f"Removing prior raw output at: {target_quarter_root}")
    dbutils.fs.rm(f"dbfs:{target_quarter_root}", recurse=True)

    # ------------------------------------------------------------------
    # Load mapping files
    # ------------------------------------------------------------------
    mapping_paths = discover_mapping_files(dbutils, quarter_root)
    print(f"Mapping paths: {mapping_paths}")

    resolved_mapping_paths = {k: dbfs_path(v) if v else v for k, v in mapping_paths.items()}
    _, site_sheet_mapping = load_quarter_mappings(
        {segment: resolved_mapping_paths}, header_sheet_name
    )
    print(f"3PLs in mapping: {sorted(site_sheet_mapping.keys())}")

    # ------------------------------------------------------------------
    # Process 3PL inventory files
    # ------------------------------------------------------------------
    files_3pl = discover_3pl_files(dbutils, quarter_root)
    print(f"Discovered {len(files_3pl)} 3PL inventory file(s)")

    for site_id, src_path in files_3pl.items():
        out_dir  = f"{target_quarter_root}/3pl_files/{site_id}"
        print(f"\n  Processing {site_id}")
        try:
            sheets = process_3pl_file(dbfs_path(src_path), site_sheet_mapping.get(site_id))
            dbutils.fs.mkdirs(f"dbfs:{out_dir}")
            for sheet_slug, data in sheets.items():
                out_path = dbfs_path(f"{out_dir}/{sheet_slug}.csv")
                data.to_csv(out_path, index=False, encoding="utf-8")
                print(f"    Wrote {out_path}")
        except Exception as e:
            print(f"  ERROR processing {site_id}: {e}")

# COMMAND ----------

print(f"\n3PL raw processing complete for {year_override or year} {quarter_override or quarter}")
