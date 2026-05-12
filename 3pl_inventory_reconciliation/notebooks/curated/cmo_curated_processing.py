# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import json
import os
import re
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import PurePath
from typing import Dict, Any, Optional
import data_cache_starburst as dc
import curation_utils as cutils

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC #### Identify latest year and quarter

# COMMAND ----------

def get_latest_year_quarter(root_directory):
    latest_year_dir = None
    latest_quarter_dir = None
    # Find the latest year directory
    year_dirs = sorted(
        [int(PurePath(d.path).name) for d in dbutils.fs.ls(root_directory)
         if PurePath(d.path).name.isdigit() and len(PurePath(d.path).name) == 4],
        reverse=True
    )
    if not year_dirs:
        return latest_year_dir, latest_quarter_dir
    latest_year_dir = str(year_dirs[0])
    year_path = os.path.join(root_directory, latest_year_dir)
    # Find the latest quarter directory within the latest year
    quarter_dirs = sorted(
        [
            PurePath(d.path).name
            for d in dbutils.fs.ls(year_path)
            if re.fullmatch(r"Q[1-4]", PurePath(d.path).name)
        ],
        key=lambda q: int(q[1:]),
        reverse=True
    )
    if not quarter_dirs:
        return latest_year_dir, latest_quarter_dir
    latest_quarter_dir = quarter_dirs[0]
    # latest_quarter_dir = 'Q4'
    return latest_year_dir, latest_quarter_dir

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resolve config

# COMMAND ----------

def resolve_placeholders(data, variables):
    if isinstance(data, dict):
        return {k: resolve_placeholders(v, variables) for k, v in data.items()}
    elif isinstance(data, list):
        return [resolve_placeholders(item, variables) for item in data]
    elif isinstance(data, str):
        return data.format(**variables)
    else:
        return data

# COMMAND ----------

with open("config.json") as f:
    raw_config = json.load(f)

mount_point = "/mnt/pdm-gsc-bi"
raw_data_path = os.path.join(mount_point, raw_config["s3_paths"]["raw_data_path"])
latest_year, latest_quarter = get_latest_year_quarter(raw_data_path) 

variables = {
    "year": latest_year,
    "quarter": latest_quarter
}
 
config = resolve_placeholders(raw_config, variables)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Mounting s3 bucket

# COMMAND ----------


mount_point = "/mnt/pdm-gsc-bi"

source_bucket = config["s3_paths"]["bucket"]  
raw_data_path = os.path.join(mount_point, config["s3_paths"]["raw_data_path"])

curated_data_path = os.path.join(mount_point, config["s3_paths"]["curated_data_path"])

plant_name_mapping_file_path = os.path.join(mount_point, config["s3_paths"]["plant_name_mapping_file_path"])

api_mapping_file_path = os.path.join(mount_point, config["s3_paths"]["api_mapping_file_path"])
dp_mapping_file_path = os.path.join(mount_point, config["s3_paths"]["dp_mapping_file_path"])

header_mapping_sheet_name = config["s3_paths"]["header_mapping_sheet_name"] 
item_mapping_sheet_name = config["s3_paths"]["item_mapping_sheet_name"]
uom_mapping_sheet_name = config["s3_paths"]["uom_mapping_sheet_name"]
uom_master_file_path = os.path.join(mount_point, config["s3_paths"]["uom_master_file_path"])
material_master_file_path = os.path.join(mount_point, config["s3_paths"]["material_master_file_path"])
lot_no_mapping_file_path = os.path.join(mount_point, config["s3_paths"]["lot_no_mapping_file_path"])
lot_no_master_file_path = os.path.join(mount_point, config["s3_paths"]["lot_no_master_file_path"])
gil_receipts_file_path = os.path.join(mount_point, config["s3_paths"]["gilead_receipts_file_path"])
sap_report_file_path = os.path.join(mount_point, config["s3_paths"]["sap_report_path"])
unit_cost_file_path = os.path.join(mount_point, config["s3_paths"]["unit_cost_file_path"])
material_type_file_path = os.path.join(mount_point, config["s3_paths"]["material_type_file_path"])
material_description_file_path = os.path.join(mount_point, config["s3_paths"]["material_description_file_path"])
starburst_config = config["starburst_config"]

# COMMAND ----------

if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
        source = source_bucket,
        mount_point = mount_point,
      )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Function for identifying files to process

# COMMAND ----------

def find_latest_files_s3(root_directory):
    latest_files = {}
    latest_year_dir = None
    latest_quarter_dir = None
 
    print(f"\n[INFO] ===== STARTING SEARCH IN {root_directory} =====")

    year_dirs = sorted(
        [int(PurePath(d.path).name) for d in dbutils.fs.ls(root_directory)
         if PurePath(d.path).name.isdigit() and len(PurePath(d.path).name) == 4],
        reverse=True
    )

    if not year_dirs:
        print(f"[INFO] No year directories found in root: {root_directory}")
        return latest_files
    latest_year_dir = str(year_dirs[0])
    year_path = os.path.join(root_directory, latest_year_dir)
    print(f"[INFO] Found latest year directory: {latest_year_dir}")
    print(f"[INFO] \tPath: {year_path}")

    # List all directories within the latest year and filter for quarter directories (Q1-Q4)
    quarter_dirs = sorted(
        [
            PurePath(d.path).name
            for d in dbutils.fs.ls(year_path)
            if re.fullmatch(r"Q[1-4]", PurePath(d.path).name)
        ],
        key=lambda q: int(q[1:]),
        reverse=True
    )

    if not quarter_dirs:
        print(f"[INFO] No quarter directories (Q1-Q4) found in year: {year_path}")
        return latest_files

    latest_quarter_dir = quarter_dirs[0]
    # latest_quarter_dir = 'Q4'
    quarter_path = os.path.join(year_path, latest_quarter_dir, '3pl_files')
    print(f"\n[INFO] Found latest quarter: {latest_quarter_dir}")
    print(f"[INFO] \tFull path: {quarter_path}")

    # List subfolders within the '3pl_files' directory
    subfolders = [
        PurePath(d.path).name
        for d in dbutils.fs.ls(quarter_path)
        if d.name.endswith('/')
    ]
    print(f"\n[INFO] Found {len(subfolders)} subfolders in {latest_quarter_dir}/3pl_files:")
    for sf in subfolders:
        print(f"[INFO] \t- {sf}")
        
    for subfolder in subfolders:
        subfolder_path = os.path.join(quarter_path, subfolder)
        print(f"\n[INFO] Scanning subfolder: {subfolder}")
        print(f"[INFO] \tFull path: {subfolder_path}")
 
        all_files_info = dbutils.fs.ls(subfolder_path)
        latest_file_info = None
        latest_time = None
        for file_info in all_files_info:
            if not file_info.path.endswith('/'):
                modified_time_seconds = file_info.modificationTime / 1000
                modified_datetime = datetime.fromtimestamp(modified_time_seconds)
                if latest_time is None or modified_datetime > latest_time:
                    latest_time = modified_datetime
                    latest_file_info = file_info
 
        if latest_file_info:
            mounted_path = latest_file_info.path.replace("dbfs:", "/dbfs")
            latest_files[subfolder] = mounted_path
            print(f"[SUCCESS] Found latest file in {subfolder}:")
            print(f"[SUCCESS] \tFile: {os.path.basename(mounted_path)}")
            print(f"[SUCCESS] \tModified: {latest_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"[SUCCESS] \tFull path: {mounted_path}")
        else:
            print(f"[WARNING] No files found in subfolder: {subfolder}")
        print("")  # Empty line after processing each subfolder
 
 
    print(f"\n[SUMMARY] ===== SEARCH COMPLETE =====")
    print(f"[SUMMARY] Found {len(latest_files)} latest files across {len(subfolders) if 'subfolders' in locals() else 0} subfolders")
    if latest_files:
        print("[SUMMARY] Files found by subfolder:")
        for subfolder, path in latest_files.items():
            print(f"  - {subfolder}: {os.path.basename(path)}")
    print("=" * 50)
    return latest_files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main function for curating single file

# COMMAND ----------

def curated_processing(
        raw_df: pd.DataFrame,
        raw_file_path: str,
        mapping_cache: dc.MappingDataCache
) -> pd.DataFrame:

    pd.set_option('display.max_rows', None)
    # Load input data with proper error handling
    df = raw_df.copy()
    df = df.replace([None, 'None', 'nan', 'NaN', ''], np.nan)

    pl_combined_key = cutils.get_3pl_combined_key(raw_file_path)
    print(f"\nProcessing data for 3PL: {pl_combined_key}")
    print(f"\tInput data loaded: {df.shape} rows and columns")

    # Add metadata columns
    print("\tAdding metadata")
    df = cutils.add_3pl_details(df, raw_file_path, mapping_cache.plant_name_mapping_df, mapping_cache.3pl_type_mapping_df)
    print('\t---------------', df.shape)
    
    

    # Load and apply header mapping
    print("\tApplying header mapping")
    print(df.columns)
    df = cutils.map_filter_3pl_df(df, pl_combined_key, mapping_cache.header_mapping_df)
    print('\t---------------', df.shape)
    
    

    print("\tAdding info columns")
    df = cutils.add_metadata(df, raw_file_path)
    print('\t---------------', df.shape)
    
    
    #Aggregate quantities
    print("\tAggregating Quantities")
    df = cutils.aggregate_quantities(df)
    print('\t---------------', df.shape)
    

    # Map material codes
    print("\tMapping material codes")
    df = cutils.map_material_code(df, mapping_cache.item_mapping_df, mapping_cache.material_master_df)
    print('\t---------------', df.shape)

    # Map lot numbers
    print("\tMapping lot numbers")
    df = cutils.map_lot_no_wildcard(df, mapping_cache.lot_no_master_df, mapping_cache.lot_no_mapping_df, mapping_cache.sap_report_df)
    print('\t---------------', df.shape)
    

    # Process UOM mapping and conversion
    print("\tProcessing UOM mapping and conversion")
    df = cutils.map_uom_and_convert(df, mapping_cache.uom_mapping_df, mapping_cache.uom_master_df)
    print('\t---------------', df.shape)
    

    # Get Unit cost
    print("\tGet Unit Costs")
    df = cutils.get_unit_cost(df, mapping_cache.unit_cost_df)
    print('\t---------------', df.shape)
    

    # Get Material type
    print("\tGet Material Type")
    df = cutils.get_material_type(df, mapping_cache.material_type_df)
    print('\t---------------', df.shape)
    

    desired_order = [
        '3PL',
        '3PL_Name',
        'Gilead_Material_Code',
        'Gilead_Batch_Number',
        'Gilead_UOM',
        'Conversion_Factor',
        '3PL_Material_Code',
        '3PL_Batch_Number',
        '3PL_Quantity',
        '3PL_Converted_Quantity',
        '3PL_UOM',
        '3PL_Material_Type',
        'Cost',
        '3PL_Type',
        'File_Name',
        'Year',
        'Quarter',
        'Date_Processed',
        'Has_Error',
        'Validation_Remark'
    ]

    df = df[desired_order]
    df = df.replace('nan', np.nan)

    print(f"Processing complete. Output shape: {df.shape}")
    return df


def save_processed_data(df: pd.DataFrame, output_directory: str, output_filename: str) -> None:
    output_directory_dbu = output_directory.replace("/dbfs", "dbfs:")
    print(f"Ensuring output directory exists: {output_directory_dbu}")
    dbutils.fs.mkdirs(output_directory_dbu)
    full_output_path = os.path.join(output_directory, output_filename)
    df.to_csv(full_output_path, index=False)
    print(f"File saved successfully to {full_output_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading mapping data cache

# COMMAND ----------

mapping_cache = dc.load_mapping_files(
    api_mapping_file_path = f"/dbfs{api_mapping_file_path}",
    dp_mapping_file_path = f"/dbfs{dp_mapping_file_path}",
    header_mapping_sheet_name = header_mapping_sheet_name,
    plant_name_mapping_file_path=f"/dbfs{plant_name_mapping_file_path}",
    item_mapping_sheet_name=item_mapping_sheet_name,
    uom_mapping_sheet_name=uom_mapping_sheet_name,
    uom_master_file_path=f"/dbfs{uom_master_file_path}",
    material_master_file_path=f"/dbfs{material_master_file_path}",
    lot_no_mapping_file_path=f"/dbfs{lot_no_mapping_file_path}",
    material_description_file_path =f"/dbfs{material_description_file_path}",
    unit_cost_file_path=f"/dbfs{unit_cost_file_path}",
    material_type_file_path=f"/dbfs{material_type_file_path}",
    gil_receipts_file_path=f"/dbfs{gil_receipts_file_path}",
    sap_report_file_path=f"/dbfs{sap_report_file_path}",
    lot_no_master_file_path=f"/dbfs{lot_no_master_file_path}",
    year=latest_year,
    quarter=latest_quarter,
    starburst_config=starburst_config,
    create_mapping_tables_from_files=False,
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Identify files to be processed

# COMMAND ----------

latest_files_from_s3 = find_latest_files_s3(raw_data_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clear previous data for the identified Quarter

# COMMAND ----------

dbutils.fs.rm(f'dbfs:{os.path.join(curated_data_path, latest_year, latest_quarter)}', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process identified files

# COMMAND ----------

if latest_files_from_s3:
    raw_data_path = config["s3_paths"]["raw_data_path"]
    curated_data_path = config["s3_paths"]["curated_data_path"]
    
    for subfolder, raw_file_path in latest_files_from_s3.items():
        print(f"\n\n\nSubfolder '{subfolder}': {raw_file_path}")

        curated_output_file_path = raw_file_path.replace(raw_data_path, curated_data_path)
        print(f"Calculated curated_output_path: {curated_output_file_path}")

        print(f"Raw file path for processing: {raw_file_path}")

        curated_output_directory = os.path.dirname(curated_output_file_path)
        print(f"Curated output directory: {curated_output_directory}")

        curated_file_name = os.path.basename(curated_output_file_path)
        print(f"Curated file_name: {curated_file_name}\n")
        try:
            print(f"Loading input file from {raw_file_path}")
            # 1. Display all columns
            pd.set_option('display.max_columns', None)

            # 2. Display all rows
            pd.set_option('display.max_rows', None)
            raw_df = pd.read_csv(raw_file_path, dtype=str)
            # print("------------------Raw DF--------------------------------")
            # print(raw_df)
            df = curated_processing(raw_df, raw_file_path, mapping_cache)
            save_processed_data(df, curated_output_directory, curated_file_name)
        except Exception as e:
            print(e)
            # if subfolder not in ():
            #     raise e
else:
    print("No suitable files found in the specified folder structure.")