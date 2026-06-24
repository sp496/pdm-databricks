# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Stage Clinical EBS Inventory, ALL orgs (quarterly refresh)
# MAGIC Self-contained, standalone variant of `stage_clinical_inventory` that
# MAGIC snapshots **all** clinical lot-level inventory from Oracle EBS
# MAGIC (`pdm.ebs_processed.*`) into `ebs_clinical_inventory`
# MAGIC (partition: Year/Quarter) — with **no plant/org filter**.
# MAGIC
# MAGIC Use this version when you want the full EBS clinical inventory regardless
# MAGIC of which 3PL plants have source files landed for the quarter. (The
# MAGIC filtered `stage_clinical_inventory` notebook restricts the query to the
# MAGIC orgs discovered under the source landing.)
# MAGIC
# MAGIC The EBS query is expensive and the snapshot only changes quarter-to-quarter,
# MAGIC so run this on its own (roughly quarterly) cadence rather than on every
# MAGIC reconciliation run. Year/Quarter are still resolved from the source landing
# MAGIC purely to stamp the partition; no source 3pl_files are otherwise read.

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
from lib.discovery import resolve_quarter_from_source
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

print(f"Source root                 : {src_root}")
print(f"EBS clinical inventory table: {ebs_clinical_inventory_table}")

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
# MAGIC #### Stage clinical EBS inventory (all orgs)

# COMMAND ----------

segment_l          = "clinical"
segment_src_root_l = f"{src_root}/{segment_l}"

print(f"\n{'='*60}")
print(f"Segment: {segment_l}  (all orgs — no plant filter)")
print(f"{'='*60}")

# Resolve year/quarter from the SOURCE landing per the segment's run_mode
# (historical | latest_completed | latest_available)
run_mode, year_l, quarter_l = resolve_quarter_from_source(curated_cfg, segment_l, dbutils, segment_src_root_l)
print(f"  Run mode: {run_mode}")
if run_mode != "historical" and (not year_l or not quarter_l):
    raise ValueError(
        f"No completed quarter found under {segment_src_root_l} — "
        f"ensure clinical source files are landed before staging"
    )
print(f"  Resolved quarter: year={year_l}, quarter={quarter_l}")

# Run EBS query with NO org filter — inventory_orgs=None returns all orgs.
backend = DataBackend(data_source=data_source, starburst_config=starburst_config)
query   = get_clinical_inventory_query(inventory_orgs=None, data_source=data_source)
df      = backend.run_query("ebs_clinical_inventory", query)
print(f"  Fetched {len(df)} row(s) from EBS (all orgs)")

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
print("Clinical EBS inventory staging complete (all orgs)")
print(f"  Clinical EBS : Year={year_l} Quarter={quarter_l}")
