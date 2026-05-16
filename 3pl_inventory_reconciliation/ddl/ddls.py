# Databricks notebook source
import os
import json

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print("Environment: ", env)

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`.`curated_3pl_inventory` (
    `Segment`                 STRING,
    `3PL`                     STRING,
    `3PL_Name`                STRING,
    `3PL_Material_Code`       STRING,
    `Gilead_Material_Code`    STRING,
    `3PL_Batch_Number`        STRING,
    `Gilead_Batch_Number`     STRING,
    `3PL_UOM`                 STRING,
    `Gilead_UOM`              STRING,
    `Conversion_Factor`       DOUBLE,
    `3PL_Quantity`            DOUBLE,
    `3PL_Converted_Quantity`  DOUBLE,
    `Cost`                    DOUBLE,
    `3PL_Material_Type`       STRING,
    `3PL_Type`                STRING,
    `Has_Error`               BOOLEAN,
    `Validation_Remark`       STRING,
    `File_Name`               STRING,
    `Year`                    STRING,
    `Quarter`                 STRING,
    `Date_Processed`          STRING
)
USING DELTA
PARTITIONED BY (Year, Quarter)
COMMENT '3PL inventory reconciliation curated layer'
""")
