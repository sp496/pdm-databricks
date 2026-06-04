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
CREATE TABLE IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`.`file_tracking` (
    `file_name`   STRING,
    `3PL`         STRING,
    `source_path` STRING,
    `stage`       STRING,
    `status`      STRING,
    `rows`        BIGINT,
    `recorded_at` TIMESTAMP,
    `segment`     STRING,
    `year`        STRING,
    `quarter`     STRING
)
USING DELTA
PARTITIONED BY (segment, year, quarter)
COMMENT '3PL inventory reconciliation — per-file pipeline tracking (source → ingest → curated)'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`.`header_mapping` (
    `3PL`                  STRING,
    `3PL_Classification`   STRING,
    `Sheet_Name`           STRING,
    `3PL_Column_Header`    STRING,
    `Gilead_Column_Header` STRING,
    `Comments`             STRING,
    `Segment`              STRING,
    `Year`                 STRING,
    `Quarter`              STRING
)
USING DELTA
PARTITIONED BY (Segment, Year, Quarter)
COMMENT '3PL inventory reconciliation — header column mapping'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`.`item_mapping` (
    `Plant_Number`    STRING,
    `Plant_Name`      STRING,
    `Material_Number` STRING,
    `3PL_Part`        STRING,
    `Comments`        STRING,
    `Segment`         STRING,
    `Year`            STRING,
    `Quarter`         STRING
)
USING DELTA
PARTITIONED BY (Segment, Year, Quarter)
COMMENT '3PL inventory reconciliation — item/material code mapping'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`.`uom_mapping` (
    `Plant_Number`      STRING,
    `Plant_Name`        STRING,
    `Gilead_UOM`        STRING,
    `3PL_Part`          STRING,
    `3PL_UOM`           STRING,
    `Conversion_Factor` DOUBLE,
    `Comments`          STRING,
    `Segment`           STRING,
    `Year`              STRING,
    `Quarter`           STRING
)
USING DELTA
PARTITIONED BY (Segment, Year, Quarter)
COMMENT '3PL inventory reconciliation — UOM and conversion factor mapping'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`.`sap_report` (
    `Company_Code`                                    STRING,
    `Name_of_Company_Code_or_Company`                 STRING,
    `Plant`                                           STRING,
    `Plant_Name`                                      STRING,
    `Profit_Center`                                   STRING,
    `G/L_Account_Number`                              STRING,
    `External_Material_Group`                         STRING,
    `Product_Hierarchy_Level_2_Description`           STRING,
    `Product_Hierarchy_Level_3_Description`           STRING,
    `Material_Number`                                 STRING,
    `Material_Description_in_Uppercase_for_Matchcodes` STRING,
    `Material_Type`                                   STRING,
    `Description_of_Material_Type`                    STRING,
    `Batch_Number`                                    STRING,
    `Batch_Status`                                    STRING,
    `Date_of_Manufacture`                             STRING,
    `Next_Inspection_Date`                            STRING,
    `Shelf_Life_Expiration`                           STRING,
    `Storage_Location`                                STRING,
    `Description_of_Storage_Location`                 STRING,
    `Base_UOM`                                        STRING,
    `Reporting_Unit_of_Measure`                       STRING,
    `Alternative_UOM`                                 STRING,
    `System_Information`                              STRING,
    `Batch_Last_Change_Date_YYYY/MM/DD_`              STRING,
    `Batch_Last_Change_Timestamp_HH:MM:SS_`           STRING,
    `Standard_Price`                                  STRING,
    `Group_Valuation_Standard_Price`                  STRING,
    `Price_Unit`                                      STRING,
    `Reporting_Unit_of_Measure_Conversion_Rate`       STRING,
    `Stock_Quantity__Base_UOM_`                       DOUBLE,
    `Value_of_Total_Valuated_Stock`                   STRING,
    `Reporting_Unit_of_Measure_Quantity`              STRING,
    `Standard_Extended_Cost`                          STRING,
    `Group_Valuation_Standard_Cost`                   DOUBLE,
    `Segment`                                         STRING,
    `Year`                                            STRING,
    `Quarter`                                         STRING
)
USING DELTA
PARTITIONED BY (Segment, Year, Quarter)
COMMENT '3PL inventory reconciliation — SAP stock report'
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
    `Material_Description`    STRING,
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

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`.`ebs_clinical_inventory` (
    `operating_unit_name`   STRING,
    `legal_entity_name`     STRING,
    `inventory_org`         STRING,
    `inventory_org_name`    STRING,
    `material_group`        STRING,
    `item`                  STRING,
    `description`           STRING,
    `lot_number`            STRING,
    `lot_status`            STRING,
    `lot_expiry_date`       STRING,
    `lot_retest_date`       STRING,
    `primary_uom`           STRING,
    `onhand_quantity`       DOUBLE,
    `reservation_quantity`  DOUBLE,
    `available_quantity`    DOUBLE,
    `subinventory_code`     STRING,
    `inventory_item_id`     STRING,
    `organization_id`       STRING,
    `Segment`               STRING,
    `Year`                  STRING,
    `Quarter`               STRING
)
USING DELTA
PARTITIONED BY (Year, Quarter)
COMMENT '3PL inventory reconciliation — staged clinical inventory snapshot from EBS (per quarter)'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`.`reconciled_3pl_commercial_inventory` (
    `Plant_Number`                        STRING,
    `Plant_Name`                          STRING,
    `External_Material_Group`             STRING,
    `Material_Number`                     STRING,
    `Gilead_Material_Code`                STRING,
    `Material_Description`                STRING,
    `Cost_ea`                             DOUBLE,
    `Cost_ea_per_unit`                    DOUBLE,
    `Group_Valuation_Standard_Cost`       DOUBLE,
    `Cost`                                DOUBLE,
    `Batch_Number`                        STRING,
    `Stock_OH`                            DOUBLE,
    `UOM`                                 STRING,
    `3PL_Quantity`                        DOUBLE,
    `3PL_Converted_Quantity`              DOUBLE,
    `3PL_UOM`                             STRING,
    `3PL`                                 STRING,
    `3PL_Name`                            STRING,
    `3PL_Material_Code`                   STRING,
    `3PL_Material_Type`                   STRING,
    `Line_item_variance_threshold_amount` INTEGER,
    `File_Name`                           STRING,
    `Date_Processed`                      STRING,
    `Has_Error`                           BOOLEAN,
    `Validation_Remark`                   STRING,
    `Gilead_Batch_Number`                 STRING,
    `3PL_Batch_Number`                    STRING,
    `Plant_Classification`                STRING,
    `Effective_Material_Code`             STRING,
    `Effective_Batch_Number`              STRING,
    `Processing_Timestamp`                STRING,
    `Segment`                             STRING,
    `Year`                                STRING,
    `Quarter`                             STRING
)
USING DELTA
PARTITIONED BY (Segment, Year, Quarter)
COMMENT '3PL inventory reconciliation — reconciled commercial output (SAP ↔ curated 3PL)'
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `pdm-pdm-gsc-bi-{env}`.`3pl_inventory_recon`.`reconciled_3pl_clinical_inventory` (
    `Org_Code`                      STRING,
    `Inventory_Org_Name`            STRING,
    `Operating_Unit_Name`           STRING,
    `Legal_Entity_Name`             STRING,
    `Material_Group`                STRING,
    `Item_Number`                   STRING,
    `Gilead_Material_Code`          STRING,
    `Item_Description`              STRING,
    `Material_Description`          STRING,
    `Lot_Number`                    STRING,
    `Lot_Status`                    STRING,
    `Lot_Expiry_Date`               STRING,
    `Lot_Retest_Date`               STRING,
    `Onhand_Quantity`               DOUBLE,
    `Allocated_Quantity`            DOUBLE,
    `Available_To_Reserve_Quantity` DOUBLE,
    `3PL_Quantity`                  DOUBLE,
    `3PL_Converted_Quantity`        DOUBLE,
    `Primary_UOM`                   STRING,
    `3PL_UOM`                       STRING,
    `3PL`                           STRING,
    `3PL_Name`                      STRING,
    `3PL_Material_Code`             STRING,
    `3PL_Material_Type`             STRING,
    `File_Name`                     STRING,
    `Date_Processed`                STRING,
    `Has_Error`                     BOOLEAN,
    `Validation_Remark`             STRING,
    `Gilead_Batch_Number`           STRING,
    `3PL_Batch_Number`              STRING,
    `Plant_Classification`          STRING,
    `Effective_Material_Code`       STRING,
    `Effective_Batch_Number`        STRING,
    `Processing_Timestamp`          STRING,
    `Segment`                       STRING,
    `Year`                          STRING,
    `Quarter`                       STRING
)
USING DELTA
PARTITIONED BY (Segment, Year, Quarter)
COMMENT '3PL inventory reconciliation — reconciled clinical inventory (EBS ↔ curated 3PL)'
""")
