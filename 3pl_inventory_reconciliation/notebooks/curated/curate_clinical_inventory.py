# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3PL Inventory — Curated Processing (CLINICAL)
# MAGIC Curates the clinical segment only and writes its own `Segment='clinical'`
# MAGIC partition of `curated_3pl_inventory`, independent of the commercial run.
# MAGIC The shared pipeline lives in `lib.curated.pipeline`.
# MAGIC
# MAGIC Clinical files may be facility-mapped (e.g. the `almac` folder): the real
# MAGIC plant is resolved per row at curation time from the workbook's
# MAGIC "Facility Mapping" sheet. Run the commercial counterpart
# MAGIC (`curate_commercial_inventory`) separately.
# MAGIC
# MAGIC **DATA_SOURCE:** `spark` in prd, else `starburst` (resolved from DATAENV).

# COMMAND ----------

import os
import sys

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # 3pl_inventory_reconciliation
repo_root    = os.path.dirname(project_root)
sys.path.extend([project_root, repo_root])

# COMMAND ----------

from lib.curated.pipeline import curate_segment
from lib.discovery import resolve_quarter_from_source
from common.config_loader import load_config

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resolve run context (env, config, data source, quarter)

# COMMAND ----------

segment = "clinical"

env          = dbutils.widgets.get("DATAENV")
curated_cfg  = load_config(os.path.join(project_root, "config/curated.json"))
data_source  = "spark" if env == "prd" else "starburst"
resolved_env = "prod" if env == "prd" else env

starburst_config = None
if data_source == "starburst":
    starburst_config = {
        "base_url"        : "jdbc:trino://query.gilead.com:443",
        "username"        : dbutils.secrets.get(scope="pdm-gsc", key="starburst-username"),
        "password"        : dbutils.secrets.get(scope="pdm-gsc", key="starburst-password"),
        "default_catalog" : "pdm",
        "default_schema"  : "default",
    }

# Resolve the target quarter from the SOURCE landing (authoritative "which quarter").
src_root         = f"{curated_cfg['src_bkt_mount_point']}/{curated_cfg['src_data_dir'].format(env=resolved_env)}"
segment_src_root = f"{src_root}/{segment}"

run_mode, year, quarter = resolve_quarter_from_source(curated_cfg, segment, dbutils, segment_src_root)
if run_mode != "historical" and (not year or not quarter):
    raise ValueError(f"No completed quarter found under {segment_src_root}")

print(f"{segment}: env={env}  data_source={data_source}  run_mode={run_mode}  year={year}  quarter={quarter}")

# COMMAND ----------

summary = curate_segment(
    spark, dbutils, curated_cfg, env, segment, year, quarter,
    data_source=data_source, starburst_config=starburst_config,
)
print(summary)
