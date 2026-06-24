# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAP Report — Raw Processing
# MAGIC Reads the commercial SAP report for the target quarter, cleans it, and writes a CSV to the raw layer.

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

from lib.discovery import resolve_quarter_from_source, discover_sap_file
from lib.raw.excel_processor import process_sap_file
from common.config_loader import load_config
from common.dbfs_utils import dbfs_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print(f"Environment: {env}")

# COMMAND ----------

config = load_config(os.path.join(project_root, "config/raw.json"))
resolved_env = "prod" if env == "prd" else env

src_bkt_mount_point  = config["src_bkt_mount_point"]
data_bkt_mount_point = config["data_bkt_mount_point"]
src_data_dir         = config["src_data_dir"].format(env=resolved_env)
raw_data_dir         = config["raw_data_dir"]

src_root = f"{src_bkt_mount_point}/{src_data_dir}"
tgt_root = f"{data_bkt_mount_point}/{raw_data_dir}"
print(f"Source root : {src_root}")
print(f"Target root : {tgt_root}")

# SAP report is commercial-only
segment             = "commercial"
segment_src_root    = f"{src_root}/{segment}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resolve quarter to process

# COMMAND ----------

run_mode, year, quarter = resolve_quarter_from_source(config, segment, dbutils, segment_src_root)
print(f"Run mode    : {run_mode}")
if run_mode != "historical" and (not year or not quarter):
    raise RuntimeError(f"No completed quarter found under {segment_src_root}")
print(f"Using year={year}, quarter={quarter}")

quarter_root = f"{segment_src_root}/{year}/{quarter}"
sap_out_dir  = f"{tgt_root}/{segment}/{year}/{quarter}/sap_report_files"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reset target SAP raw layer for this quarter (idempotent rerun)

# COMMAND ----------

print(f"Removing prior SAP output at: {sap_out_dir}")
dbutils.fs.rm(f"dbfs:{sap_out_dir}", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process SAP report

# COMMAND ----------

sap_path = discover_sap_file(dbutils, quarter_root)
if not sap_path:
    raise RuntimeError(f"No SAP report file found under {quarter_root}/sap_report_files")

print(f"SAP source: {sap_path}")
sap_out_name = os.path.basename(sap_path).replace(".xlsx", ".csv").replace(".xls", ".csv")
df = process_sap_file(dbfs_path(sap_path))
dbutils.fs.mkdirs(f"dbfs:{sap_out_dir}")
out_path = dbfs_path(f"{sap_out_dir}/{sap_out_name}")
df.to_csv(out_path, index=False, encoding="utf-8")
print(f"  Wrote {out_path}")

# COMMAND ----------

print(f"SAP raw processing complete for {year} {quarter}")
