# Databricks notebook source
# MAGIC %md
# MAGIC ## Pipeline Setup — Mount S3 Buckets
# MAGIC
# MAGIC Run this notebook **once at the start of the pipeline** (or as the first task in a Databricks Job)
# MAGIC before any raw / curated / processed notebooks execute.
# MAGIC
# MAGIC All buckets are defined centrally in `common/config/infrastructure.json`.
# MAGIC Individual pipeline notebooks can then reference mount points directly
# MAGIC without performing any mounting themselves.

# COMMAND ----------

import sys
import os

current_dir = os.getcwd()
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)

from common.storage import mount_all

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print(f"Environment: {env}")

# COMMAND ----------

mount_all(dbutils, env)
print("All buckets mounted successfully.")

# COMMAND ----------

dbutils.fs.mounts()