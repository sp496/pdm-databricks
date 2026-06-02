# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Write System-of-Record Inventory Tables
# MAGIC Stages the per-quarter system-of-record inventory snapshots into Delta:
# MAGIC
# MAGIC - **Cell 1 — Commercial SAP**: reads the SAP report CSV produced by the
# MAGIC   raw ingestion (`raw/commercial/{year}/{quarter}/sap_report_files/`),
# MAGIC   applies the standard pre-write preprocessing, and writes to
# MAGIC   `sap_report` (partition: Segment/Year/Quarter).
# MAGIC - **Cell 2 — Clinical EBS**: discovers clinical inventory orgs from
# MAGIC   `raw/clinical/{year}/{quarter}/3pl_files/{site_id}/`, queries Oracle
# MAGIC   EBS (`pdm.ebs_processed.*`) for the lot-level inventory snapshot, and
# MAGIC   writes to `ebs_clinical_inventory` (partition: Year/Quarter).
# MAGIC
# MAGIC Run order: after `ingest_3pl_inventory` for the target quarter, in
# MAGIC parallel with `write_mapping_tables`. Must complete before
# MAGIC `curate_3pl_inventory` and the processed-layer reconciliation.

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
from lib.curated.transformations import remove_decimal_if_all_zeros
from lib.curated.data_cache import load_sap_report_file
from lib.discovery import (
    get_latest_completed_quarter,
    discover_3pl_files,
    discover_sap_file,
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
raw_root = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['raw_data_dir']}"

sap_report_table             = curated_cfg["sap_report_table"].format(env=env)
ebs_clinical_inventory_table = curated_cfg["ebs_clinical_inventory_table"].format(env=env)

run_config = curated_cfg["run_config"]
run_mode   = run_config["run_mode"]

print(f"Source root                 : {src_root}")
print(f"Raw root                    : {raw_root}")
print(f"SAP report table            : {sap_report_table}")
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
# MAGIC #### Delta write helper

# COMMAND ----------

def _write_delta(df: pd.DataFrame, table: str, label: str, segment: str, year: str, quarter: str) -> None:
    """Stamp Segment/Year/Quarter, convert to Spark, and overwrite the
    matching (Segment, Year, Quarter) partition of the Delta table."""
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
# MAGIC ## Cell 1 — Commercial SAP staging

# COMMAND ----------

segment_c             = "commercial"
segment_src_root_c    = f"{src_root}/{segment_c}"
segment_raw_root_c    = f"{raw_root}/{segment_c}"

print(f"\n{'='*60}")
print(f"Segment: {segment_c}")
print(f"{'='*60}")

# Resolve year/quarter
if run_mode == "historical":
    year_c    = hist_year
    quarter_c = hist_quarter
    print(f"  Using historical: year={year_c}, quarter={quarter_c}")
else:
    year_c, quarter_c = get_latest_completed_quarter(dbutils, segment_src_root_c)
    if not year_c or not quarter_c:
        raise ValueError(
            f"No completed quarter found under {segment_src_root_c} — "
            f"run ingest_3pl_inventory for commercial before staging"
        )
    print(f"  Auto-detected latest completed quarter: year={year_c}, quarter={quarter_c}")

src_quarter_root_c = f"{segment_src_root_c}/{year_c}/{quarter_c}"
raw_quarter_root_c = f"{segment_raw_root_c}/{year_c}/{quarter_c}"
print(f"  Src quarter root: {src_quarter_root_c}")
print(f"  Raw quarter root: {raw_quarter_root_c}")

# Discover 3PL site folders from the SOURCE landing — site_ids are the SAP
# Plant codes. This is folder enumeration only (the source layer has the same
# {site_id}/ layout as raw), so building the plant filter list needs no raw
# dependency. The SAP report *data* is still read from raw below.
files_by_site_c = discover_3pl_files(dbutils, src_quarter_root_c)
plant_numbers_c = sorted({str(k).strip() for k in files_by_site_c.keys() if str(k).strip()})
print(f"  Discovered {len(plant_numbers_c)} plant(s) from source layer: {plant_numbers_c}")

# Discover the SAP report CSV (raw layer)
sap_report_path = discover_sap_file(dbutils, raw_quarter_root_c)
if not sap_report_path:
    raise ValueError(
        f"No SAP report found under {raw_quarter_root_c}/sap_report_files — "
        f"run ingest_3pl_inventory first"
    )
print(f"  SAP report: {sap_report_path}")

# Load + preprocess
sap_df = load_sap_report_file(dbfs_path(sap_report_path))
sap_df["Batch_Number"]                  = sap_df["Batch_Number"].apply(remove_decimal_if_all_zeros)
sap_df["Stock_Quantity__Base_UOM_"]     = pd.to_numeric(sap_df["Stock_Quantity__Base_UOM_"],     errors="coerce")
sap_df["Group_Valuation_Standard_Cost"] = pd.to_numeric(sap_df["Group_Valuation_Standard_Cost"], errors="coerce")

# Filter to plants that have source 3PL files for this quarter.
# This keeps the sap_report table scoped to relevant plants and avoids the
# need for a curated-table lookup in the downstream reconciliation step.
pre_filter_rows = len(sap_df)
if plant_numbers_c:
    sap_df = sap_df[sap_df["Plant"].isin(plant_numbers_c)]
print(f"  SAP rows: {pre_filter_rows} total → {len(sap_df)} after plant filter")

_write_delta(sap_df, sap_report_table, "sap_report", segment_c, year_c, quarter_c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2 — Clinical EBS staging
# MAGIC Inventory orgs are discovered from the SOURCE-landing site_id folders
# MAGIC (`{src}/clinical/{year}/{quarter}/3pl_files/{site_id}/`). This cell reads
# MAGIC no raw-layer data at all (it queries EBS live), so it depends on neither
# MAGIC the raw layer nor the curated table and can run fully in parallel.

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
# enumeration only — no raw-layer data is read in this cell).
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
print("Inventory tables write complete")
print(f"  Commercial SAP : Year={year_c} Quarter={quarter_c}")
print(f"  Clinical EBS   : Year={year_l} Quarter={quarter_l}")
