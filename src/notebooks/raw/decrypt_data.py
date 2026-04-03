# Databricks notebook source
import os
import sys
import json
from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql.types import DateType

# Make the clinical_inventory package importable from src/
sys.path.insert(0, os.path.normpath(os.path.join(os.getcwd(), "../../")))
from clinical_inventory.raw import decrypt_file as dc

# COMMAND ----------

env = dbutils.widgets.get("DATAENV")
print(f"Environment: {env}")

# COMMAND ----------

with open("../../../config/raw.json") as f:
    config = json.load(f)

resolved_env = "prod" if env == "prd" else env

src_bkt = config["src_bkt"].format(env=env)
src_bkt_mount_point = config["src_bkt_mount_point"].rstrip("/")
tgt_bkt = config["tgt_bkt"].format(env=env)
tgt_bkt_mount_point = config["tgt_bkt_mount_point"].rstrip("/")

#decryption_key = config["decryption_key"]
decryption_key = dbutils.secrets.get(scope = "clinical_inventory_rpa", key = "rpa_decryption_key")

src_data_dir = config["src_data_dir"].format(env=resolved_env)
tgt_data_dir = config["tgt_data_dir"]

# New historical load controls
historical_load = config.get("historical_load", False)
start_date = config.get("start_date")
end_date = config.get("end_date")

# COMMAND ----------

# Ensure mounts
if not any(m.mountPoint == src_bkt_mount_point for m in dbutils.fs.mounts()):
    dbutils.fs.mount(source=f"s3a://{src_bkt}", mount_point=src_bkt_mount_point)

if not any(m.mountPoint == tgt_bkt_mount_point for m in dbutils.fs.mounts()):
    dbutils.fs.mount(source=f"s3a://{tgt_bkt}", mount_point=tgt_bkt_mount_point)

# COMMAND ----------

def get_available_date_folders(base_path):
    """Return sorted list of (folder_name, datetime_obj) tuples."""
    folders = []
    for f in dbutils.fs.ls(base_path):
        name = f.name.strip("/")
        if not name.isdigit():
            continue
        try:
            date_obj = datetime.strptime(name, "%Y%m%d")
            folders.append((name, date_obj))
        except ValueError:
            continue
    return sorted(folders, key=lambda x: x[1])

# COMMAND ----------

src_root_path = f"{src_bkt_mount_point}/{src_data_dir}"
folders = get_available_date_folders(src_root_path)

if not folders:
    raise Exception(f"No valid date folders found in {src_root_path}")

# Determine folders to process
selected_folders = []

if historical_load:
    # Convert date strings to datetime if present
    start_dt = datetime.strptime(start_date, "%Y%m%d") if start_date else None
    end_dt = datetime.strptime(end_date, "%Y%m%d") if end_date else None

    if not start_dt and not end_dt:
        # Process all available folders
        selected_folders = [f for f, _ in folders]
        print(f"Historical load enabled — processing ALL {len(selected_folders)} folders.")
    elif start_dt and not end_dt:
        selected_folders = [f for f, d in folders if d >= start_dt]
        print(f"Historical load from {start_date} onwards — {len(selected_folders)} folders.")
    elif not start_dt and end_dt:
        selected_folders = [f for f, d in folders if d <= end_dt]
        print(f"Historical load up to {end_date} — {len(selected_folders)} folders.")
    else:
        selected_folders = [f for f, d in folders if start_dt <= d <= end_dt]
        print(f"Historical load from {start_date} to {end_date} — {len(selected_folders)} folders.")
else:
    latest_folder = folders[-1][0]
    selected_folders = [latest_folder]
    print(f"Standard mode — processing latest folder only: {latest_folder}")

# COMMAND ----------

decryptor = dc.AESDecryptor(decryption_key, debug=False)
src_base_mount = f"{src_bkt_mount_point}/{src_data_dir}".rstrip("/")

def list_enc_files_one_level(base_path):
    """Return all .enc files directly under base_path and its immediate subfolders."""
    files = [
        i.path.replace("dbfs:", "").rstrip("/")
        for i in dbutils.fs.ls(base_path)
        if i.path.endswith(".enc")
    ]

    subdirs = [
        i.path.replace("dbfs:", "").rstrip("/")
        for i in dbutils.fs.ls(base_path)
        if i.isDir()
    ]

    for d in subdirs:
        files += [
            f.path.replace("dbfs:", "").rstrip("/")
            for f in dbutils.fs.ls(d)
            if f.path.endswith(".enc")
        ]
    return files


for folder in selected_folders:
    folder_path = f"{src_root_path}/{folder}"
    files = list_enc_files_one_level(folder_path)

    if not files:
        print(f"No encrypted files found in {folder_path}")
        continue

    print(f"🔹 Decrypting {len(files)} files in {folder_path}...")

    for file in files:
        rel_path = file[len(src_base_mount):].lstrip("/") if file.startswith(src_base_mount) else f"{folder}/{os.path.basename(file)}"
        out_mount = f"{tgt_bkt_mount_point}/{tgt_data_dir}/{rel_path}".replace(".enc", ".csv")

        dbutils.fs.mkdirs(os.path.dirname(out_mount))
        decryptor.decrypt_file(f"/dbfs{file}", f"/dbfs{out_mount}")

    print(f"✅ Completed folder: {folder}")

print("🎉 Decryption process complete.")
 