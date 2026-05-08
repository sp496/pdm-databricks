# Databricks notebook source
import os
import json

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print("Environment: ", env)

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS  `pdm-pdm-gsc-bi-{env}`.`3pl_inventory`
""")
