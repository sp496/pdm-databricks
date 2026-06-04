# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Stage Clinical EBS Inventory (quarterly refresh)
# MAGIC Self-contained, standalone notebook that snapshots the clinical lot-level
# MAGIC inventory from Oracle EBS (`pdm.ebs_processed.*`) into the
# MAGIC `ebs_clinical_inventory` Delta table (partition: Year/Quarter).
# MAGIC
# MAGIC The EBS query is expensive and the snapshot only changes quarter-to-quarter,
# MAGIC so this notebook is intentionally **decoupled** from the per-run pipeline —
# MAGIC run it on its own (roughly quarterly) cadence rather than on every
# MAGIC reconciliation run.
# MAGIC
# MAGIC Inventory orgs are discovered from the SOURCE-landing site_id folders
# MAGIC (`{src}/clinical/{year}/{quarter}/3pl_files/{site_id}/`). This notebook
# MAGIC reads no raw-layer data at all (it queries EBS live), so it depends on
# MAGIC neither the raw layer nor the curated table.

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

import pandas as pd

from lib.curated.queries import get_clinical_inventory_query
from lib.discovery import (
    get_latest_completed_quarter,
    discover_3pl_files,
)
from common.backends import DataBackend
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

ebs_clinical_inventory_table = curated_cfg["ebs_clinical_inventory_table"].format(env=env)

run_config = curated_cfg["run_config"]
run_mode   = run_config["run_mode"]

print(f"Source root                 : {src_root}")
print(f"EBS clinical inventory table: {ebs_clinical_inventory_table}")
print(f"Run mode                    : {run_mode}")

if run_mode == "historical":
    hist_year    = run_config.get("year")
    hist_quarter = run_config.get("quarter")
    if not hist_year or not hist_quarter:
        raise ValueError("run_mode is 'historical' but 'year' and/or 'quarter' not set in run_config")
    print(f"Historical load: year={hist_year}, quarter={hist_quarter}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Starburst config (dev only)

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
# MAGIC #### Stage clinical EBS inventory

# COMMAND ----------

segment_l          = "clinical"
segment_src_root_l = f"{src_root}/{segment_l}"

print(f"\n{'='*60}")
print(f"Segment: {segment_l}")
print(f"{'='*60}")

# Resolve year/quarter from the SOURCE landing
if run_mode == "historical":
    year_l    = hist_year
    quarter_l = hist_quarter
    print(f"  Using historical: year={year_l}, quarter={quarter_l}")
else:
    year_l, quarter_l = get_latest_completed_quarter(dbutils, segment_src_root_l)
    if not year_l or not quarter_l:
        raise ValueError(
            f"No completed quarter found under {segment_src_root_l} — "
            f"ensure clinical source files are landed before staging"
        )
    print(f"  Auto-detected latest completed quarter: year={year_l}, quarter={quarter_l}")

src_quarter_root_l = f"{segment_src_root_l}/{year_l}/{quarter_l}"

# Discover inventory orgs from SOURCE-landing site_id folders (folder
# enumeration only — no raw-layer data is read in this notebook).
files_by_site  = discover_3pl_files(dbutils, src_quarter_root_l)
inventory_orgs = sorted(
    {str(k).strip() for k in files_by_site.keys() if str(k).strip()}
)
print(
    f"  Discovered {len(inventory_orgs)} clinical inventory org(s) "
    f"under {src_quarter_root_l}: {inventory_orgs}"
)
if not inventory_orgs:
    raise ValueError(
        f"No clinical plant directories found under "
        f"{src_quarter_root_l}/3pl_files — ensure clinical source files are landed"
    )

# Run EBS query
backend = DataBackend(data_source=data_source, starburst_config=starburst_config)
query   = get_clinical_inventory_query(inventory_orgs, data_source)
df      = backend.run_query("ebs_clinical_inventory", query)
print(f"  Fetched {len(df)} row(s) from EBS")

# Cast numerics
for c in ("onhand_quantity", "reservation_quantity", "available_quantity"):
    df[c] = pd.to_numeric(df[c], errors="coerce")

# Stamp partition columns
df["Segment"] = segment_l
df["Year"]    = year_l
df["Quarter"] = quarter_l

# Write — ebs_clinical_inventory is partitioned by (Year, Quarter) only,
# so the replaceWhere clause omits Segment.
spark_df = spark.createDataFrame(df)
(
    spark_df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"Year = '{year_l}' AND Quarter = '{quarter_l}'")
    .option("overwriteSchema", "false")
    .saveAsTable(ebs_clinical_inventory_table)
)
print(f"  ebs_clinical_inventory: wrote {len(df)} rows to {ebs_clinical_inventory_table}")

# COMMAND ----------

print(f"\n{'='*60}")
print("Clinical EBS inventory staging complete")
print(f"  Clinical EBS : Year={year_l} Quarter={quarter_l}")
