# Databricks notebook source
import os
import json

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print("Environment: ", env)

# COMMAND ----------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS  `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### clinical_subject_summary

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_subject_summary`
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_subject_summary` (
  `study_protocol`								STRING,
  `site_id`                                     INT,
  `country`                                     STRING,
  `parent_depot`                                INT,
  `investigator`                                STRING,
  `subject_number`                              BIGINT,
  `year_of_birth`                               INT,
  `gender`                                      STRING,
  `tpc`                                         STRING,
  `date_randomized`                             DATE,
  `date_treatment_discontinued`                 DATE,
  `date_crossover_enrolled`                     DATE,
  `date_crossover_approved`                     DATE,
  `date_crossover_treatment_discontinued`       DATE,
  `subject_status`                              STRING,
  `randomized_treatment`                        STRING,
  `last_study_visit_recorded`                   STRING,
  `last_study_visit_date`                       DATE,
  `last_study_visit_number`                     STRING,
  `next_min_study_visit_date`                   DATE,
  `next_max_study_visit_date`                   DATE,
  `additional_drug_status`                      STRING,
  `last_additional_drug_visit_recorded`         STRING,
  `last_additional_drug_visit_date`             DATE,
  `last_additional_drug_visit_number`           INT, 
  `next_min_additional_drug_visit_date`         DATE,
  `next_max_additional_drug_visit_date`         DATE,
  `extract_date`                                   DATE,
  `source_file`                                 STRING,
  `processed_timestamp`                         TIMESTAMP
)
USING DELTA
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC #### clinical_depot_inventory

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_depot_inventory`
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_depot_inventory` (
  study_protocol								STRING,
  depot_id INT,
  country STRING,
  depot_type STRING,
  study_drug_type STRING,
  unblinded_study_drug_name STRING,
  britestock_lot_number STRING,
  finished_lot_number STRING,
  part_number STRING,
  fp_expiry_date DATE,
  quantity_study_drug_requested BIGINT,
  quantity_study_drug_available BIGINT,
  quantity_study_drug_lost BIGINT,
  quantity_study_drug_damaged BIGINT,
  quantity_study_drug_quarantined BIGINT,
  quantity_study_drug_rejected BIGINT,
  quantity_study_drug_do_not_ship BIGINT,
  quantity_study_drug_expired BIGINT,
  quantity_study_drug_packaged_unavailable BIGINT,
  quantity_study_drug_total BIGINT,
  approved_countries STRING,
  extract_date DATE,
  source_file STRING,
  processed_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### clinical_site_inventory

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_site_inventory`
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_site_inventory` (
  study_protocol STRING,
  site_id INT,
  country STRING,
  investigator STRING,
  location STRING,
  parent_depot INT,
  site_status STRING,
  study_drug_type STRING,
  unblinded_study_drug_name STRING,
  britestock_lot_number STRING,
  finished_lot_number STRING,
  part_number STRING,
  fp_expiry_date DATE,
  quantity_study_drug_requested BIGINT,
  quantity_study_drug_available BIGINT,
  quantity_study_drug_assigned BIGINT,
  quantity_study_drug_lost BIGINT,
  quantity_study_drug_damaged BIGINT,
  quantity_study_drug_quarantined BIGINT,
  quantity_study_drug_rejected BIGINT,
  quantity_study_drug_do_not_dispense BIGINT,
  quantity_study_drug_expired BIGINT,
  quantity_study_drug_total BIGINT,
  extract_date DATE,
  source_file STRING,
  processed_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### clinical_supply_method_country_level

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_country_level`
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_country_level` (
  study_protocol STRING,
  country STRING,
  comparator_name STRING,
  country_level_supply_method STRING,
  extract_date DATE,
  source_file STRING,
  processed_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### clinical_supply_method_site_level

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_site_level`
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_site_level` (
  study_protocol STRING,
  country STRING,
  site_id STRING,
  comparator_name STRING,
  site_level_supply_method STRING,
  site_status STRING,
  extract_date DATE,
  source_file STRING,
  processed_timestamp TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### clinical_treatment_groups

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_treatment_groups`
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_treatment_groups` (
  `therapeutic_area` STRING,
  `study_protocol` STRING,
  `randomized_treatment` STRING,
  `subject_status` STRING,
  `tpc` STRING,
  `study_drug_dispensed` STRING,
  `additional_study_drug_dispensed` STRING,
  `additional_study_drug_prefix` STRING,
  `country` STRING,
  `visit_days` STRING,
  `dispensing_quantity` BIGINT,
  `dispensing_frequency_days` BIGINT,
  `max_cycles` DOUBLE
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### clinical_supply_method_site_level

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_site_level`
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_site_level` (
    `study_protocol` STRING,
    `country` STRING,
    `site_id` STRING,
    `comparator_name` STRING,
    `site_level_supply_method` STRING,
    `site_status` STRING,
    `extract_date` DATE
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### clinical_supply_method_country_level

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_country_level`
""")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_country_level` (
    `study_protocol` STRING,
    `country` STRING,
    `comparator_name` STRING,
    `country_level_supply_method` STRING,
    `extract_date` DATE
)
USING DELTA
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### clinical_demand_plan

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_demand_plan`
""")

# COMMAND ----------

spark.sql(f"""

CREATE TABLE `pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_demand_plan` (
    `study_name` STRING,
    `parent_depot` INT,
    `site_id` INT,
    `subject_number` INT,
    `subject_status` STRING,
    `subject_country` STRING,
    `randomized_treatment` STRING,
    `tpc` STRING,
    `drug_dispensed` STRING,
    `dispensing_quantity` INT,
    `predicted_study_visit` STRING,
    `cycle` INT,
    `day` INT,
    `predicted_next_visit_date` DATE, 
    `extract_date` DATE,
    `processed_timestamp` TIMESTAMP
)
USING DELTA
""")
