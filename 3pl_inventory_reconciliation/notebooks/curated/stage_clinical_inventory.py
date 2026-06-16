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
# MAGIC Inventory orgs are discovered from the CURATED clinical table — the
# MAGIC distinct `3PL` values for this Segment/Year/Quarter. Source folder names
# MAGIC are no longer the source of truth for orgs: facility-mapped files (e.g.
# MAGIC `almac`) carry a non-plant folder name and resolve their real plant per
# MAGIC row at curation time, so the authoritative org list lives in the curated
# MAGIC table. This notebook therefore DEPENDS on the curated table being
# MAGIC populated for the quarter (run it after curate_3pl_inventory); it still
# MAGIC reads no raw-layer data and queries EBS live.

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
from lib.discovery import get_latest_completed_quarter
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
curated_table                = curated_cfg["curated_table"].format(env=env)

run_config = curated_cfg["run_config"]
run_mode   = run_config["run_mode"]

print(f"Source root                 : {src_root}")
print(f"EBS clinical inventory table: {ebs_clinical_inventory_table}")
print(f"Curated table               : {curated_table}")
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

# Discover inventory orgs from the CURATED clinical table — the distinct,
# non-null `3PL` values for this Segment/Year/Quarter. This is the post-curation
# org list, so facility-mapped files (e.g. `almac`, whose folder name is not a
# plant) contribute their per-row resolved plants rather than the folder token.
if not spark.catalog.tableExists(curated_table):
    raise ValueError(
        f"Curated table {curated_table} does not exist — run curate_3pl_inventory "
        f"for {year_l}/{quarter_l} before staging clinical EBS inventory"
    )

curated_orgs_df = (
    spark.table(curated_table)
    .filter(f"Segment = '{segment_l}' AND Year = '{year_l}' AND Quarter = '{quarter_l}'")
    .select("3PL")
    .distinct()
    .toPandas()
)
inventory_orgs = sorted(
    {str(o).strip() for o in curated_orgs_df["3PL"].dropna() if str(o).strip()}
)
print(
    f"  Discovered {len(inventory_orgs)} clinical inventory org(s) from "
    f"{curated_table} ({segment_l}/{year_l}/{quarter_l}): {inventory_orgs}"
)
if not inventory_orgs:
    raise ValueError(
        f"No clinical orgs found in {curated_table} for "
        f"{segment_l}/{year_l}/{quarter_l} — ensure curate_3pl_inventory ran for "
        f"this quarter and produced clinical rows with a resolved 3PL"
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
