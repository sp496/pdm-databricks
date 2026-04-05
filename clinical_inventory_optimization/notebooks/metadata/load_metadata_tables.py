# Databricks notebook source
import sys
import os

current_dir  = os.getcwd()
project_root = os.path.dirname(os.path.dirname(current_dir))  # clinical_inventory_optimization/
repo_root    = os.path.dirname(project_root)                   # pdm-databricks/
sys.path.extend([project_root, repo_root])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import re
import logging
import pandas as pd
from datetime import datetime
from typing import List, Tuple
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DateType, TimestampType, DoubleType, StructType, StructField, BooleanType
)
from lib.curated.data_curator import Constants
from common.config_loader import load_config

# COMMAND ----------

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration Loading

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
logger.info(f"Environment: {env}")

config = load_config(os.path.join(project_root, "config/metadata.json"))

resolved_env = "prod" if env == "prd" else env

mapping_bkt_mount_point = config["mapping_bkt_mount_point"]
studylist_file_path = config["studylist_file_path"].format(env=resolved_env)
ingested_data_dir = config["ingested_data_dir"].format(env=resolved_env)

hist = config["historical_load"]
historical_load = hist["enabled"]
start_date = hist["start_date"]
end_date = hist["end_date"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Identify date folders to be processed

# COMMAND ----------

def get_available_date_folders(base_path: str):
    """Return sorted list of (folder_name, datetime_obj) tuples."""
    folders = []
    for f in dbutils.fs.ls(base_path):
        name = f.name.strip("/")
        if not name.isdigit():
            continue
        try:
            date_obj = datetime.strptime(name, Constants.DATE_FOLDER_FORMAT)
            folders.append((name, date_obj))
        except ValueError:
            continue
    return sorted(folders, key=lambda x: x[1])


ingested_data_path = f"{mapping_bkt_mount_point}/{ingested_data_dir}"
folders = get_available_date_folders(ingested_data_path)

if not folders:
    raise Exception(f"No valid date folders found in {ingested_data_path}")

# Determine folders to process
selected_folders = []

if historical_load:
    start_dt = datetime.strptime(start_date, Constants.DATE_FOLDER_FORMAT) if start_date else None
    end_dt = datetime.strptime(end_date, Constants.DATE_FOLDER_FORMAT) if end_date else None

    if not start_dt and not end_dt:
        selected_folders = [f for f, _ in folders]
        logger.info(f"Historical load enabled — processing ALL {len(selected_folders)} folders")
    elif start_dt and not end_dt:
        selected_folders = [f for f, d in folders if d >= start_dt]
        logger.info(f"Historical load from {start_date} onwards — {len(selected_folders)} folders")
    elif not start_dt and end_dt:
        selected_folders = [f for f, d in folders if d <= end_dt]
        logger.info(f"Historical load up to {end_date} — {len(selected_folders)} folders")
    else:
        selected_folders = [f for f, d in folders if start_dt <= d <= end_dt]
        logger.info(f"Historical load from {start_date} to {end_date} — {len(selected_folders)} folders")
else:
    latest_folder = folders[-1][0]
    selected_folders = [latest_folder]
    logger.info(f"Standard mode — processing latest folder only: {latest_folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load metadata table

# COMMAND ----------

studylist_file_path = f"/dbfs{os.path.join(mapping_bkt_mount_point, studylist_file_path)}"
studylist_df = pd.read_excel(studylist_file_path, dtype='str', engine='openpyxl')

studylist_df = spark.createDataFrame(studylist_df)

studylist_df = studylist_df \
    .withColumnRenamed("S. No", "s_no") \
    .withColumnRenamed("Study Name", "study_name") \
    .withColumnRenamed("UI Mode", "ui_mode") \
    .withColumnRenamed("Report Name", "report_name") \
    .withColumnRenamed("SubReport Name", "subreport_name")

# COMMAND ----------

def create_file_lookup_dataframe(date_folder_path):
    """
    Create a DataFrame mapping study_name, report_name, subreport_name to file_name
    by scanning the filesystem and extracting the latest file per study/report/subreport combination.
    
    Args:
        date_folder_path (str): Path to the date folder
        
    Returns:
        DataFrame: Spark DataFrame with columns: study_name, report_name, subreport_name, file_name
    """
    
    # Regex pattern to extract timestamp from filename: YYYY-MM-DD-HH-MM-SS before extension
    timestamp_pattern = r'(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})\.(csv|enc)$'
    
    file_records = []
    
    try:
        # Get all study subfolders
        subfolders = [item for item in dbutils.fs.ls(date_folder_path) if item.isDir()]
        
        for subfolder in subfolders:
            subfolder_path = subfolder.path
            study_name = subfolder_path.rstrip('/').split('/')[-1]
            
            # print(f"\n📁 Scanning study folder: {study_name}")
            
            # Track latest file per report/subreport combination in this study
            latest_files = {}
            
            try:
                files = dbutils.fs.ls(subfolder_path)
                
                for file in files:
                    file_name = file.name
                    file_name_lower = file_name.lower()
                    
                    # Skip non-CSV/ENC files
                    if not (file_name_lower.endswith('.csv') or file_name_lower.endswith('.enc')):
                        continue
                    
                    # Extract timestamp from filename
                    match = re.search(timestamp_pattern, file_name)
                    if not match:
                        print(f"   ⚠️ Could not extract timestamp from filename: {file_name}")
                        continue
                    
                    timestamp_str = match.group(1)
                    try:
                        file_datetime = datetime.strptime(timestamp_str, '%Y-%m-%d-%H-%M-%S')
                    except ValueError as e:
                        print(f"   ⚠️ Could not parse timestamp '{timestamp_str}': {e}")
                        continue
                    
                    # Determine report_name and subreport_name
                    report_name = None
                    subreport_name = None
                    
                    # Subject/Participant Summary
                    if ('subject summary' in file_name_lower) or ('participant summary' in file_name_lower):
                        if 'unblinded' in file_name_lower:
                            if 'subject' in file_name_lower:
                                report_name = "Subject Summary (Unblinded)"
                            else:
                                report_name = "Participant Summary (Unblinded)"
                        else:
                            report_name = "Subject Summary"
                        subreport_name = None
                    
                    # Subject Visit Summary
                    elif 'subject visit summary' in file_name_lower:
                        report_name = "Subject Visit Summary"
                        subreport_name = None
                    
                    # Inventory Summary (Unblinded)
                    elif 'inventory summary' in file_name_lower:
                        report_name = "Inventory Summary (Unblinded)"
                        if 'site' in file_name_lower:
                            subreport_name = "Site Inventory Summary"
                        elif 'depot' in file_name_lower:
                            subreport_name = "Depot Inventory Summary"
                    
                    # Inventory Levels
                    elif 'inventory levels' in file_name_lower:
                        report_name = "Inventory Levels"
                        if 'site' in file_name_lower:
                            subreport_name = "Site"
                        elif 'depot' in file_name_lower:
                            subreport_name = "Depot"
                    
                    # Comparator Supply (Unblinded)
                    elif 'comparator supply' in file_name_lower or \
                         ('supply method' in file_name_lower and 'comparator' not in file_name_lower):
                        report_name = "Comparator Supply (Unblinded)"
                        if 'site' in file_name_lower or 'site level' in file_name_lower:
                            subreport_name = "Site Level Supply Method"
                        elif 'country' in file_name_lower or 'country level' in file_name_lower:
                            subreport_name = "Country Level Supply Method"
                    
                    if report_name:
                        # Create unique key for this report/subreport combination
                        key = (report_name, subreport_name)
                        
                        # Keep only the latest file for this combination
                        current_entry = latest_files.get(key)
                        if current_entry is None or file_datetime > current_entry['datetime']:
                            latest_files[key] = {
                                'file_name': file_name,
                                'datetime': file_datetime
                            }
                
                # Add all latest files from this study to records
                for (report_name, subreport_name), info in latest_files.items():
                    file_records.append({
                        "study_name": study_name,
                        "report_name": report_name,
                        "subreport_name": subreport_name,
                        "file_name": info['file_name']
                    })
                    # print(f"   ✅ {report_name} / {subreport_name} → {info['file_name']}")
            
            except Exception as e:
                print(f"   ⚠️ Error scanning subfolder {subfolder_path}: {str(e)}")
    
    except Exception as e:
        print(f"❌ Error scanning date folder {date_folder_path}: {str(e)}")
    
    # Create DataFrame
    schema = StructType([
        StructField("study_name", StringType(), True),
        StructField("report_name", StringType(), True),
        StructField("subreport_name", StringType(), True),
        StructField("file_name", StringType(), True)
    ])
    
    file_df = spark.createDataFrame(file_records, schema=schema)
    
    return file_df


# COMMAND ----------

def map_target_table(report_name, subreport_name):
    """Map report and subreport to target table"""
    
    if report_name is None:
        return "NA"
    
    subreport = subreport_name if subreport_name else None
    
    mapping = {
        ("Comparator Supply (Unblinded)", "Country Level Supply Method"): "clinical_supply_method_country_level",
        ("Comparator Supply (Unblinded)", "Site Level Supply Method"): "clinical_supply_method_site_level",
        ("Inventory Levels", "Site"): "NA",
        ("Inventory Levels", "Depot"): "NA",
        ("Inventory Summary (Unblinded)", "Site Inventory Summary"): "clinical_site_inventory",
        ("Inventory Summary (Unblinded)", "Depot Inventory Summary"): "clinical_depot_inventory",
        ("Participant Summary (Unblinded)", None): "clinical_subject_summary",
        ("Subject Summary", None): "NA",
        ("Subject Summary (Unblinded)", None): "clinical_subject_summary",
        ("Subject Visit Summary", None): "NA",
    }
    
    return mapping.get((report_name, subreport), "NA")

# COMMAND ----------

def get_protocols_dict(tables, catalog=f"pdm-pdm-gsc-bi-{env}", schema="clinical_inventory"):
    table_protocols_dict = {}
    
    for table_name in tables:
        try:
            df = spark.table(f"`{catalog}`.`{schema}`.{table_name}")
            latest_date = df.agg(F.max("extract_date")).collect()[0][0]
            
            if latest_date:
                protocols = (df.filter((F.col("extract_date") == latest_date) & 
                                      (F.col("study_protocol").isNotNull()))
                              .select("study_protocol")
                              .distinct()
                              .orderBy("study_protocol")
                              .rdd.flatMap(lambda x: x)
                              .collect())
                table_protocols_dict[table_name] = protocols
            else:
                table_protocols_dict[table_name] = []
        except Exception as e:
            print(f"Error processing {table_name}: {e}")
            table_protocols_dict[table_name] = []
    
    return table_protocols_dict


def check_load_successful(file_name, target_table, study_protocol, protocols_dict):
    if file_name is None or target_table is None or target_table == "NA" or study_protocol is None:
        return None
    if target_table not in protocols_dict:
        return None
    return study_protocol in protocols_dict[target_table]

# COMMAND ----------

for date_folder in selected_folders:
    logger.info(f"\n{'#' * 80}")
    logger.info(f"Processing date folder: {date_folder}")
    logger.info(f"{'#' * 80}")

    date_folder_path = f"{ingested_data_path}/{date_folder}"

    # Create file lookup DataFrame
    file_df = create_file_lookup_dataframe(date_folder_path)

    # Use null-safe join condition
    metadata_df = studylist_df.alias("studylist").join(
        file_df.alias("files"),
        (F.col("studylist.study_name") == F.col("files.study_name")) &
        (F.col("studylist.report_name") == F.col("files.report_name")) &
        (F.col("studylist.subreport_name").eqNullSafe(F.col("files.subreport_name"))),
        how="left"
    ).select("studylist.*", F.col("files.file_name"))


    tables = [
    "clinical_depot_inventory",
    "clinical_site_inventory",
    "clinical_subject_summary",
    "clinical_supply_method_country_level",
    "clinical_supply_method_site_level"
]

    table_protocols_dict = get_protocols_dict(tables)

    map_target_udf = F.udf(map_target_table, StringType())

    check_load_udf = F.udf(lambda file_name, target, protocol: check_load_successful(file_name, target, protocol, table_protocols_dict), BooleanType())


    metadata_df = metadata_df \
                    .withColumn("target_table", map_target_udf(F.col("report_name"), F.col("subreport_name"))) \
                    .withColumn("study_protocol", F.regexp_replace("studylist.study_name", "^Gilead\\s+",
                     "")) \
                    .withColumn("load_successful", check_load_udf(F.col("file_name"), F.col("target_table"), F.col("study_protocol"))) \
                    .withColumn("extract_date", F.to_date(F.lit(date_folder), "yyyyMMdd"))

    metadata_df = metadata_df.withColumn("s_no", F.col("s_no").cast("integer")).orderBy("s_no")

    metadata_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("replaceWhere", f"extract_date = to_date('{date_folder}', 'yyyyMMdd')") \
    .saveAsTable(f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_studylist_status`")