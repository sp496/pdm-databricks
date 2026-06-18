# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Write Mapping Tables
# MAGIC Loads the file-based mapping datasets (header mapping, item mapping, UOM mapping,
# MAGIC SAP report) for each configured segment/quarter and writes each to a partitioned
# MAGIC Delta table.
# MAGIC
# MAGIC Run this notebook once per quarter after the raw processing notebook completes
# MAGIC and the mapping Excel files are in place.

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

import pandas as pd

from lib.curated.data_cache import MappingFilePaths, load_file_mappings
from lib.discovery import discover_mapping_file
from common.config_loader import load_config
from common.dbfs_utils import dbfs_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parameters

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print(f"Environment  : {env}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config and path resolution

# COMMAND ----------

curated_cfg = load_config(os.path.join(project_root, "config/curated.json"))

resolved_env = "prod" if env == "prd" else env
src_root     = f"{curated_cfg['src_bkt_mount_point']}/{curated_cfg['src_data_dir'].format(env=resolved_env)}"
raw_root     = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['raw_data_dir']}"
segments     = curated_cfg["segments"]
run_config   = curated_cfg["run_config"]
run_mode     = run_config["run_mode"]

header_mapping_table = curated_cfg["header_mapping_table"].format(env=env)
item_mapping_table   = curated_cfg["item_mapping_table"].format(env=env)
uom_mapping_table    = curated_cfg["uom_mapping_table"].format(env=env)

print(f"Segments : {segments}")
print(f"Run mode : {run_mode}")

if run_mode == "historical":
    hist_year    = run_config.get("year")
    hist_quarter = run_config.get("quarter")
    if not hist_year or not hist_quarter:
        raise ValueError("run_mode is 'historical' but 'year' and/or 'quarter' not set in run_config")
    print(f"Historical load: year={hist_year}, quarter={hist_quarter}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write helper

# COMMAND ----------

def _write_delta(df: pd.DataFrame, table: str, label: str, segment: str, year: str, quarter: str) -> None:
    df = df.copy()
    df["Segment"] = segment
    df["Year"]    = year
    df["Quarter"] = quarter

    if df.empty:
        # No rows for this segment/quarter — clear any prior content of
        # this partition (idempotent re-runs) and skip the Spark write.
        # Spark's createDataFrame can't infer a schema from an empty
        # pandas DataFrame, so we bypass it entirely. `DELETE` is a no-op
        # if the table doesn't exist yet or no rows match.
        try:
            spark.sql(
                f"DELETE FROM {table} "
                f"WHERE Segment = '{segment}' "
                f"AND Year = '{year}' AND Quarter = '{quarter}'"
            )
            print(f"  {label}: empty for {segment}/{year}/{quarter} "
                  f"— cleared partition (no write)")
        except Exception as e:
            print(f"  {label}: empty for {segment}/{year}/{quarter} "
                  f"— skipped (table not yet created: {e.__class__.__name__})")
        return

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
# MAGIC #### Process each segment

# COMMAND ----------

from lib.discovery import get_latest_completed_quarter

all_errors = []

for segment in segments:
    print(f"\n{'='*60}")
    print(f"Segment: {segment}")
    print(f"{'='*60}")

    segment_src_root = f"{src_root}/{segment}"
    raw_quarter_root_base = f"{raw_root}/{segment}"

    # ------------------------------------------------------------------
    # Resolve year/quarter
    # ------------------------------------------------------------------
    if run_mode == "historical":
        year    = hist_year
        quarter = hist_quarter
        print(f"  Using historical: year={year}, quarter={quarter}")
    else:
        year, quarter = get_latest_completed_quarter(dbutils, segment_src_root)
        if not year or not quarter:
            print(f"  No completed quarter found under {segment_src_root} — skipping segment")
            all_errors.append((segment, "quarter detection", "No completed quarter found"))
            continue
        print(f"  Auto-detected latest completed quarter: year={year}, quarter={quarter}")

    src_quarter_root = f"{segment_src_root}/{year}/{quarter}"
    raw_quarter_root = f"{raw_quarter_root_base}/{year}/{quarter}"

    print(f"  Src quarter root : {src_quarter_root}")
    print(f"  Raw quarter root : {raw_quarter_root}")

    # ------------------------------------------------------------------
    # Discover files
    # ------------------------------------------------------------------
    try:
        mapping_path = discover_mapping_file(dbutils, src_quarter_root, segment)
        print(f"  Mapping file: {mapping_path}")

        if not mapping_path:
            print(f"  Could not find mapping file under {src_quarter_root}/mapping_files — skipping segment")
            all_errors.append((segment, "file discovery", "Missing mapping file"))
            continue

        file_paths = MappingFilePaths(
            mapping_file_path         = dbfs_path(mapping_path),
            header_mapping_sheet_name = "Header Mapping",
            item_mapping_sheet_name   = "Item Mapping",
            uom_mapping_sheet_name    = "UOM Mapping",
        )

        # File-only loader — no DataBackend / Starburst / Spark probing needed,
        # since this notebook never queries live sources (no ref_paths involved).
        cache = load_file_mappings(file_paths)

    except Exception as e:
        print(f"  ERROR loading files for {segment}: {e}")
        all_errors.append((segment, "file loading", str(e)))
        continue

    # ------------------------------------------------------------------
    # Sanity log — verifies the mapping file actually loaded for this
    # segment matches the segment we're about to write under.
    # ------------------------------------------------------------------
    print(f"\n  Loaded mapping content for segment='{segment}':")
    print(f"    Source path : {dbfs_path(mapping_path)}")
    print(f"    header_mapping rows : {len(cache.header_mapping_df)}")
    if "3PL" in cache.header_mapping_df.columns:
        sample_3pls = sorted(cache.header_mapping_df["3PL"].dropna().astype(str).unique())[:10]
        print(f"    Sample 3PL codes    : {sample_3pls}")

    # ------------------------------------------------------------------
    # Write to Delta tables
    # ------------------------------------------------------------------
    print(f"\n  Writing mapping tables for {segment} / {year} / {quarter}")

    _write_delta(cache.header_mapping_df, header_mapping_table, "header_mapping", segment, year, quarter)
    _write_delta(cache.item_mapping_df,   item_mapping_table,   "item_mapping",   segment, year, quarter)
    _write_delta(cache.uom_mapping_df,    uom_mapping_table,    "uom_mapping",    segment, year, quarter)

# COMMAND ----------

print(f"\n{'='*60}")
print(f"Write mapping tables complete")
print(f"  Segments processed : {len(segments)}")
print(f"  Errors             : {len(all_errors)}")
if all_errors:
    print("  Failed segments:")
    for seg, stage, err in all_errors:
        print(f"    [{seg}] {stage}: {err}")
