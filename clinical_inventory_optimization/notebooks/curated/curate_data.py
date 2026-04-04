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
    StringType, IntegerType, LongType, DateType, TimestampType, DoubleType
)
from lib.curated.data_curator import DataCurator, Constants, logger, load_excel_mapping
from common.config_loader import load_config

# COMMAND ----------

# Configure logging
logging.basicConfig(level=logging.INFO)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration Loading

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
logger.info(f"Environment: {env}")

config = load_config(os.path.join(project_root, "config/curated.json"))

resolved_env = "prod" if env == "prd" else env

mapping_bkt_mount_point = config["mapping_bkt_mount_point"]

ss_header_mapping_file_path = config["ss_header_mapping_file_path"].format(env=resolved_env)
d_header_mapping_file_path = config["d_header_mapping_file_path"].format(env=resolved_env)
s_header_mapping_file_path = config["s_header_mapping_file_path"].format(env=resolved_env)

treatment_group_mapping_file_path = config["treatment_group_mapping_file_path"].format(env=resolved_env)

data_bkt_mount_point = config["data_bkt_mount_point"]

raw_data_dir = config["raw_data_dir"]

# Historical load controls
historical_load = config.get("historical_load", False)
start_date = config.get("start_date")
end_date = config.get("end_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### File Reading Helper Functions

# COMMAND ----------

def read_csv_with_dynamic_header(dbfs_path: str, max_rows: int = 10) -> pd.DataFrame:
    """
    Read CSV file from DBFS with dynamic header detection.

    Args:
        dbfs_path: DBFS path (e.g., "dbfs:/mnt/...")
        max_rows: Maximum rows to search for header

    Returns:
        pandas DataFrame
    """
    # Convert dbfs:/ path to /dbfs/ path for pandas
    local_path = dbfs_path.replace("dbfs:", "/dbfs")

    logger.info(f"Reading CSV: {dbfs_path}")

    with open(local_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= max_rows:
                raise ValueError(f"No valid header found in first {max_rows} rows")

            cells = [c.strip() for c in line.split(',')]

            # Check if all cells are non-empty and has multiple columns
            if len(cells) > 1 and all(cells):
                logger.debug(f"Found header at line {i}")
                df = pd.read_csv(local_path, dtype=str, encoding='utf-8', skiprows=i)
                logger.info(f"Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")
                return df

    raise ValueError(f"No fully populated header line found in {dbfs_path}")


def load_csv_files(file_list: List[Tuple[str, str]]) -> List[Tuple[pd.DataFrame, str]]:
    """
    Load multiple CSV files into DataFrames.

    Args:
        file_list: List of (file_path, file_name) tuples from dbutils

    Returns:
        List of (DataFrame, filename) tuples
    """
    dataframes = []

    for file_path, file_name in file_list:
        try:
            df = read_csv_with_dynamic_header(file_path)
            dataframes.append((df, file_name))
            logger.info(f"✓ Loaded {file_name}: {df.shape}")
        except Exception as e:
            logger.error(f"✗ Error loading {file_name}: {str(e)}")
            continue

    return dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize DataCurator

# COMMAND ----------

# Load mapping file
ss_header_mapping_file_path_full = f"/dbfs{os.path.join(mapping_bkt_mount_point, ss_header_mapping_file_path)}"
ss_header_mapping_df = load_excel_mapping(ss_header_mapping_file_path_full)

d_header_mapping_file_path_full = f"/dbfs{os.path.join(mapping_bkt_mount_point, d_header_mapping_file_path)}"
d_header_mapping_df = load_excel_mapping(d_header_mapping_file_path_full)

s_header_mapping_file_path_full = f"/dbfs{os.path.join(mapping_bkt_mount_point, s_header_mapping_file_path)}"
s_header_mapping_df = load_excel_mapping(s_header_mapping_file_path_full)

# Initialize DataCurator with mapping
curator = DataCurator(subject_mapping_df=ss_header_mapping_df, depot_mapping_df=d_header_mapping_df, site_mapping_df=s_header_mapping_df)
logger.info("DataCurator initialized with mapping file")

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


raw_data_path = f"{data_bkt_mount_point}/{raw_data_dir}"
folders = get_available_date_folders(raw_data_path)

if not folders:
    raise Exception(f"No valid date folders found in {raw_data_path}")

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
# MAGIC #### File Discovery Functions

# COMMAND ----------

ALLOWED_STUDIES = [
    # "Gilead GS-US-412-5624",
    # "Gilead GS-US-521-6317",
    # "Gilead GS-US-528-6020",
    # "Gilead GS-US-528-6363",
    # "Gilead GS-US-528-9023",
    # "Gilead GS-US-563-5926",
    # "Gilead GS-US-577-6153",
    # "Gilead GS-US-592-6173",
    # "Gilead GS-US-592-6238",
    # "Gilead GS-US-595-6184",
    # "Gilead GS-US-598-6168",
    # "Gilead GS-US-600-6165",
    # "Gilead GS-US-626-6216",
    # "Gilead GS-US-682-6769",
    # "Gilead GS-US-200-6712",
    # "Gilead GS-US-570-6015",
    # "Gilead GS-US-569-6172",
    # "Gilead GS-US-576-6220",
    # "Gilead GS-US-576-7321"
    # "Gilead GS-US-536-5939"
]

# COMMAND ----------

def find_latest_summary_files_by_filename_timestamp(date_folder_path):
    """
    For each study subfolder within a date folder, find the latest CSV file for each category
    by extracting the timestamp from the filename itself (not file modification time).

    Filename format expected: *YYYY-MM-DD-HH-MM-SS.csv
    Example: Gilead GS-US-577-6153_Subject Summary (Unblinded)Subject Summary2025-11-10-08-19-19.csv

    Args:
        date_folder_path (str): Path to the date folder

    Returns:
        dict: Dictionary with keys 'subject', 'site', and 'depot',
              each containing a list of tuples (file_path, file_name)
              representing the latest file per study folder.
    """
    summary_files = {
        'subject': [],
        'site': [],
        'depot': [],
        'slsm': [],
        'clsm': []
    }

    # Regex pattern to extract timestamp from filename: YYYY-MM-DD-HH-MM-SS before .csv
    timestamp_pattern = r'(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})\.csv$'

    try:
        # Get all study subfolders
        subfolders = [item for item in dbutils.fs.ls(date_folder_path) if item.isDir()]

        for subfolder in subfolders:
            subfolder_path = subfolder.path

            folder_name = subfolder_path.rstrip('/').split('/')[-1]
 
            if ALLOWED_STUDIES and folder_name not in ALLOWED_STUDIES:
                print(f"⏩ Skipping unapproved folder: {folder_name}")
                continue

            print(f"\n📁 Scanning study folder: {subfolder_path}")

            latest_in_study = {
                'subject': None,
                'site': None,
                'depot': None,
                'slsm': None,
                'clsm': None
            }

            try:
                files = dbutils.fs.ls(subfolder_path)
                for file in files:
                    file_name = file.name
                    file_name_lower = file_name.lower()

                    if not file_name_lower.endswith('.csv'):
                        continue

                    # Extract timestamp from filename
                    match = re.search(timestamp_pattern, file_name)
                    if not match:
                        print(f"   ⚠️ Could not extract timestamp from filename: {file_name}")
                        continue

                    timestamp_str = match.group(1)
                    try:
                        # Parse timestamp: YYYY-MM-DD-HH-MM-SS
                        file_datetime = datetime.strptime(timestamp_str, '%Y-%m-%d-%H-%M-%S')
                    except ValueError as e:
                        print(f"   ⚠️ Could not parse timestamp '{timestamp_str}' from file {file_name}: {e}")
                        continue

                    # Determine category
                    category = None
                    if ('subject summary' in file_name_lower) or ('participant summary' in file_name_lower):
                        category = 'subject'
                    elif 'site' in file_name_lower and 'inventory' in file_name_lower:
                        category = 'site'
                    elif 'depot' in file_name_lower and 'inventory' in file_name_lower:
                        category = 'depot'
                    elif 'site' in file_name_lower and ('supplymethod' in file_name_lower or 'supply method' in file_name_lower):
                        category = 'slsm'
                    elif 'country' in file_name_lower and ('supplymethod' in file_name_lower or 'supply method' in file_name_lower):
                        category = 'clsm'

                    if category:
                        current_entry = latest_in_study[category]
                        if (
                            current_entry is None or
                            file_datetime > current_entry['datetime']
                        ):
                            latest_in_study[category] = {
                                'path': file.path,
                                'name': file_name,
                                'datetime': file_datetime
                            }

                # Add the latest file from this subfolder to overall list
                for category, info in latest_in_study.items():
                    if info:
                        summary_files[category].append((info['path'], info['name']))
                        print(f"   ✅ Latest {category.title()} → {info['name']} (timestamp: {info['datetime']})")

            except Exception as e:
                print(f"   ⚠️ Error scanning subfolder {subfolder_path}: {str(e)}")

    except Exception as e:
        print(f"❌ Error scanning date folder {date_folder_path}: {str(e)}")

    return summary_files

# COMMAND ----------

def find_latest_summary_files(date_folder_path: str) -> dict:
    """
    For each study subfolder within a date folder, find the latest CSV file for each category.

    Args:
        date_folder_path: Path to the date folder

    Returns:
        Dictionary with keys for each file type, containing list of (file_path, file_name) tuples
    """
    summary_files = {
        'subject': [],
        'site': [],
        'depot': [],
        'slsm': [],
        'clsm': []
    }

    try:
        subfolders = [item for item in dbutils.fs.ls(date_folder_path) if item.isDir()]

        for subfolder in subfolders:
            subfolder_path = subfolder.path
            logger.info(f"Scanning study folder: {subfolder_path}")

            latest_in_study = {key: None for key in summary_files.keys()}

            try:
                files = dbutils.fs.ls(subfolder_path)
                for file in files:
                    file_name = file.name
                    file_name_lower = file_name.lower()

                    if not file_name_lower.endswith('.csv'):
                        continue

                    modified_time = file.modificationTime / 1000
                    modified_datetime = datetime.fromtimestamp(modified_time)

                    # Determine category
                    category = None
                    if 'subject summary' in file_name_lower:
                        category = 'subject'
                    elif 'site' in file_name_lower and 'inventory' in file_name_lower:
                        category = 'site'
                    elif 'depot' in file_name_lower and 'inventory' in file_name_lower:
                        category = 'depot'
                    elif 'site' in file_name_lower and 'supplymethod' in file_name_lower:
                        category = 'slsm'
                    elif 'country' in file_name_lower and 'supplymethod' in file_name_lower:
                        category = 'clsm'

                    if category:
                        current_entry = latest_in_study[category]
                        if current_entry is None or modified_datetime > current_entry['datetime']:
                            latest_in_study[category] = {
                                'path': file.path,
                                'name': file_name,
                                'datetime': modified_datetime
                            }

                # Add the latest file from this subfolder to overall list
                for category, info in latest_in_study.items():
                    if info:
                        summary_files[category].append((info['path'], info['name']))
                        logger.info(f"Latest {category} → {info['name']}")

            except Exception as e:
                logger.error(f"Error scanning subfolder {subfolder_path}: {str(e)}")

    except Exception as e:
        logger.error(f"Error scanning date folder {date_folder_path}: {str(e)}")

    return summary_files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema and Configuration Mapping

# COMMAND ----------

# Define all mapping configurations
MAPPING_CONFIG = {
    "subject": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Site ID": "site_id",
            "Country": "country",
            "Parent Depot": "parent_depot",
            "Investigator": "investigator",
            "Subject Number": "subject_number",
            "Year of Birth": "year_of_birth",
            "Gender": "gender",
            "TPC": "tpc",
            "Date Randomized": "date_randomized",
            "Date Treatment Discontinued": "date_treatment_discontinued",
            "Date Crossover Enrolled": "date_crossover_enrolled",
            "Date Crossover Approved": "date_crossover_approved",
            "Date Crossover Treatment Discontinued": "date_crossover_treatment_discontinued",
            "Subject Status": "subject_status",
            "Randomized Treatment": "randomized_treatment",
            "Last Study Visit Recorded": "last_study_visit_recorded",
            "Last Study Visit Date": "last_study_visit_date",
            "Last Study Visit Number": "last_study_visit_number",
            "Next Min. Study Visit Date": "next_min_study_visit_date",
            "Next Max. Study Visit Date": "next_max_study_visit_date",
            "Additional Drug Status": "additional_drug_status",
            "Last Additional Drug Visit Recorded": "last_additional_drug_visit_recorded",
            "Last Additional Drug Visit Date": "last_additional_drug_visit_date",
            "Last Additional Drug Visit Number": "last_additional_drug_visit_number",
            "Next Min. Additional Drug Visit Date": "next_min_additional_drug_visit_date",
            "Next Max. Additional Drug Visit Date": "next_max_additional_drug_visit_date"
        },
        "date_columns": [
            "date_randomized","date_treatment_discontinued", "date_crossover_enrolled", "date_crossover_approved",
            "date_crossover_treatment_discontinued", "last_study_visit_date",
            "next_min_study_visit_date", "next_max_study_visit_date",
            "last_additional_drug_visit_date", "next_min_additional_drug_visit_date",
            "next_max_additional_drug_visit_date"
        ],
        "schema_mapping": {
            "study_protocol": StringType(),
            "site_id": IntegerType(),
            "country": StringType(),
            "parent_depot": IntegerType(),
            "investigator": StringType(),
            "subject_number": LongType(),
            "year_of_birth": IntegerType(),
            "gender": StringType(),
            "tpc": StringType(),
            "date_randomized": DateType(),
            "date_treatment_discontinued": DateType(),
            "date_crossover_enrolled": DateType(),
            "date_crossover_approved": DateType(),
            "date_crossover_treatment_discontinued": DateType(),
            "subject_status": StringType(),
            "randomized_treatment": StringType(),
            "last_study_visit_recorded": StringType(),
            "last_study_visit_date": DateType(),
            "last_study_visit_number": StringType(),
            "next_min_study_visit_date": DateType(),
            "next_max_study_visit_date": DateType(),
            "additional_drug_status": StringType(),
            "last_additional_drug_visit_recorded": StringType(),
            "last_additional_drug_visit_date": DateType(),
            "last_additional_drug_visit_number": IntegerType(),
            "next_min_additional_drug_visit_date": DateType(),
            "next_max_additional_drug_visit_date": DateType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_subject_summary`"
    },

    "depot": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Depot ID": "depot_id",
            "Country": "country",
            "Depot Type": "depot_type",
            "Study Drug Type": "study_drug_type",
            "Unblinded Study Drug Name": "unblinded_study_drug_name",
            "Britestock Lot Number": "britestock_lot_number",
            "Finished Lot Number": "finished_lot_number",
            "Part Number": "part_number",
            "FP Expiry Date": "fp_expiry_date",
            "Quantity Study Drug - Requested": "quantity_study_drug_requested",
            "Quantity Study Drug - Available": "quantity_study_drug_available",
            "Quantity Study Drug - Lost": "quantity_study_drug_lost",
            "Quantity Study Drug - Damaged": "quantity_study_drug_damaged",
            "Quantity Study Drug - Quarantined": "quantity_study_drug_quarantined",
            "Quantity Study Drug - Rejected": "quantity_study_drug_rejected",
            "Quantity Study Drug - Do Not Ship": "quantity_study_drug_do_not_ship",
            "Quantity Study Drug - Expired": "quantity_study_drug_expired",
            "Quantity Study Drug - Packaged (Unavailable)": "quantity_study_drug_packaged_unavailable",
            "Quantity Study Drug - Total": "quantity_study_drug_total",
            "Approved Countries": "approved_countries"
        },
        "date_columns": ["fp_expiry_date"],
        "schema_mapping": {
            "study_protocol": StringType(),
            "depot_id": IntegerType(),
            "country": StringType(),
            "depot_type": StringType(),
            "study_drug_type": StringType(),
            "unblinded_study_drug_name": StringType(),
            "britestock_lot_number": StringType(),
            "finished_lot_number": StringType(),
            "part_number": StringType(),
            "fp_expiry_date": DateType(),
            "quantity_study_drug_requested": LongType(),
            "quantity_study_drug_available": LongType(),
            "quantity_study_drug_lost": LongType(),
            "quantity_study_drug_damaged": LongType(),
            "quantity_study_drug_quarantined": LongType(),
            "quantity_study_drug_rejected": LongType(),
            "quantity_study_drug_do_not_ship": LongType(),
            "quantity_study_drug_expired": LongType(),
            "quantity_study_drug_packaged_unavailable": LongType(),
            "quantity_study_drug_total": LongType(),
            "approved_countries": StringType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_depot_inventory`"
    },

    "site": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Site ID": "site_id",
            "Country": "country",
            "Investigator": "investigator",
            "Location": "location",
            "Parent Depot": "parent_depot",
            "Site Status": "site_status",
            "Study Drug Type": "study_drug_type",
            "Unblinded Study Drug Name": "unblinded_study_drug_name",
            "Britestock Lot Number": "britestock_lot_number",
            "Finished Lot Number": "finished_lot_number",
            "Part Number": "part_number",
            "FP Expiry Date": "fp_expiry_date",
            "Quantity Study Drug - Requested": "quantity_study_drug_requested",
            "Quantity Study Drug - Available": "quantity_study_drug_available",
            "Quantity Study Drug - Assigned": "quantity_study_drug_assigned",
            "Quantity Study Drug - Lost": "quantity_study_drug_lost",
            "Quantity Study Drug - Damaged": "quantity_study_drug_damaged",
            "Quantity Study Drug - Quarantined": "quantity_study_drug_quarantined",
            "Quantity Study Drug - Rejected": "quantity_study_drug_rejected",
            "Quantity Study Drug - Do Not Dispense": "quantity_study_drug_do_not_dispense",
            "Quantity Study Drug - Expired": "quantity_study_drug_expired",
            "Quantity Study Drug - Total": "quantity_study_drug_total"
        },
        "date_columns": ["fp_expiry_date"],
        "schema_mapping": {
            "study_protocol": StringType(),
            "site_id": IntegerType(),
            "country": StringType(),
            "investigator": StringType(),
            "parent_depot": IntegerType(),
            "site_status": StringType(),
            "study_drug_type": StringType(),
            "unblinded_study_drug_name": StringType(),
            "britestock_lot_number": StringType(),
            "finished_lot_number": StringType(),
            "part_number": StringType(),
            "fp_expiry_date": DateType(),
            "quantity_study_drug_requested": LongType(),
            "quantity_study_drug_available": LongType(),
            "quantity_study_drug_assigned": LongType(),
            "quantity_study_drug_lost": LongType(),
            "quantity_study_drug_damaged": LongType(),
            "quantity_study_drug_quarantined": LongType(),
            "quantity_study_drug_rejected": LongType(),
            "quantity_study_drug_do_not_dispense": LongType(),
            "quantity_study_drug_expired": LongType(),
            "quantity_study_drug_total": LongType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_site_inventory`"
    },

    "slsm": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Country": "country",
            "Site ID": "site_id",
            "Comparator Name": "comparator_name",
            "Site Level Supply Method": "site_level_supply_method",
            "Site Status": "site_status",
        },
        "date_columns": [],
        "schema_mapping": {
            "study_protocol": StringType(),
            "country": StringType(),
            "site_id": StringType(),
            "comparator_name": StringType(),
            "site_level_supply_method": StringType(),
            "site_status": StringType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_site_level`"
    },

    "clsm": {
        "column_mapping": {
            "Study Protocol": "study_protocol",
            "Country": "country",
            "Comparator Name": "comparator_name",
            "Country Level Supply Method": "country_level_supply_method"
        },
        "date_columns": [],
        "schema_mapping": {
            "study_protocol": StringType(),
            "country": StringType(),
            "comparator_name": StringType(),
            "country_level_supply_method": StringType(),
            "extract_date": DateType(),
            "source_file": StringType(),
            "processed_timestamp": TimestampType()
        },
        "table_name": f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_supply_method_country_level`"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark Write Functions

# COMMAND ----------

def cast_and_write_to_delta(pandas_df, table_name: str, date_folder: str, schema_mapping:  dict, use_replace_where: bool = True):
    """
    Convert pandas DataFrame to Spark, cast types, and write to Delta table.

    Args:
        pandas_df: Pandas DataFrame to write
        table_name: Target Delta table name
        date_folder: Date folder for partition overwrite
        schema_mapping: Dictionary of column names to Spark types
        use_replace_where: If True, use replaceWhere to update only matching partitions.
                          If False, overwrite the entire table. Defaults to True.
    """
    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)

    # Cast each column to correct type
    for col_name, data_type in schema_mapping.items():
        if col_name in spark_df.columns:
            if isinstance(data_type, DateType):
                spark_df = spark_df.withColumn(col_name, F.to_date(F.col(col_name), "yyyy-MM-dd"))
            elif isinstance(data_type, TimestampType):
                spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(TimestampType()))
            else:
                spark_df = spark_df.withColumn(col_name, F.col(col_name).cast(data_type))

    # Select only columns in schema
    spark_df = spark_df.select(*schema_mapping.keys())

    # Build the write operation
    writer = (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "false")
    )

    # Conditionally add replaceWhere option
    if use_replace_where:
        writer = writer.option("replaceWhere", f"extract_date = to_date('{date_folder}', 'yyyyMMdd')")
        logger.info(f"Writing {len(pandas_df)} rows to {table_name} using replaceWhere for date {date_folder}")
    else:
        logger.info(f"Writing {len(pandas_df)} rows to {table_name} with full table overwrite")

    # Execute the write
    writer.saveAsTable(table_name)

    logger.info(f"Successfully written {len(pandas_df)} rows to {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main Processing Loop

# COMMAND ----------

def process_file_type(file_type, files, date_folder, curator, label):
    if not files:
        return
    logger.info(f"\n→ Processing {len(files)} {label} files")
    config = MAPPING_CONFIG[file_type]
    processed_dfs = [
        processed_df
        for df, filename in load_csv_files(files)
        if (processed_df := curator.process_data(
            df=df,
            file_type=file_type,
            filename=filename,
            date_folder=date_folder,
            table_column_mapping=config["column_mapping"],
            date_columns=config["date_columns"]
        )) is not None
    ]

    if processed_dfs:
        cast_and_write_to_delta(
            pandas_df=pd.concat(processed_dfs, ignore_index=True),
            table_name=config["table_name"],
            date_folder=date_folder,
            schema_mapping=config["schema_mapping"]
        )

# COMMAND ----------

FILE_TYPE_LABELS = {
    "subject": "Subject Summary",
    "depot": "Depot Inventory",
    "site": "Site Inventory",
    'slsm': "Site-Level Supply Method",
    'clsm': "Country-Level Supply Method"
}

for date_folder in selected_folders:
    logger.info(f"\n{'#' * 80}")
    logger.info(f"Processing date folder: {date_folder}")
    logger.info(f"{'#' * 80}")

    date_folder_path = f"{raw_data_path}/{date_folder}"

    summary_files = find_latest_summary_files_by_filename_timestamp(date_folder_path)

    for file_type, label in FILE_TYPE_LABELS.items():
        process_file_type(file_type, summary_files[file_type], date_folder, curator, label)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Treatment Plan Mapping

# COMMAND ----------

treatment_group_mapping_file_path = f"/dbfs{os.path.join(mapping_bkt_mount_point, treatment_group_mapping_file_path)}"
tgm_df = pd.read_excel(treatment_group_mapping_file_path, sheet_name='Treatment Group Mapping', dtype='str', engine='openpyxl')

# COMMAND ----------

# 🗺️ Column rename mapping (DataFrame → table)

column_mapping = {
    "Therapeutic Area": "therapeutic_area",
    "Study Protocol": "study_protocol",
    "Randomized Treatment": "randomized_treatment",
    "Subject Status": "subject_status",
    "TPC\nTreatment of Physician's Choice\nTopotecan OR Amrubicin Choice Of Drug\nIntended TPC": "tpc",
    "Study Drug Dispensed": "study_drug_dispensed",
    "Additional Study Drug Dispensed": "additional_study_drug_dispensed",
    "Additional Study Drug Prefix": "additional_study_drug_prefix",
    "Country": "country",
    "Visit Days": "visit_days",
    "Dispensing Quantity": "dispensing_quantity",
    "Dispensing Frequency (Days)": "dispensing_frequency_days",
    "Max Cycles": "max_cycles"
}

# 🧱 Expected schema for casting
schema_mapping = {
    "therapeutic_area": StringType(),
    "study_protocol": StringType(),
    "randomized_treatment": StringType(),
    "subject_status": StringType(),
    "tpc": StringType(),
    "study_drug_dispensed": StringType(),
    "additional_study_drug_dispensed": StringType(),
    "additional_study_drug_prefix": StringType(),
    "country": StringType(),
    "visit_days": StringType(),
    "dispensing_quantity": LongType(),
    "dispensing_frequency_days": LongType(),
    "max_cycles": DoubleType()
}

tgm_df = tgm_df[column_mapping.keys()]

# 🧩 Step 1: Rename columns
tgm_df_renamed = tgm_df.rename(columns=column_mapping)

tgm_df_cleaned = tgm_df_renamed.dropna(subset=['visit_days', 'dispensing_quantity', 'dispensing_frequency_days'])

# 🧱 Step 2: Convert pandas → Spark
spark_tgm_df = spark.createDataFrame(tgm_df_cleaned)

# 🧮 Step 3: Cast each column to correct type
for col_name, data_type in schema_mapping.items():
    if col_name in spark_tgm_df.columns:
        spark_tgm_df = spark_tgm_df.withColumn(col_name,  F.col(col_name).try_cast(data_type))

# 💾 Step 4: Truncate and load (overwrite entire table)
(
    spark_tgm_df.write
    .format("delta")
    .mode("overwrite")  # full overwrite (truncate + load)
    .option("overwriteSchema", "false")
    .saveAsTable(f"`pdm-pdm-gsc-bi-{env}`.`clinical_inventory`.`clinical_treatment_groups`")
)

print("✅ Successfully loaded clinical_treatment_group_mapping table (truncate and load).")