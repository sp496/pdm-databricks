"""Excel → CSV processing for 3PL inventory and SAP report files."""
import os
import pandas as pd

from lib.raw import excel_utils as eu


def _to_dbfs_read_path(path):
    if path.startswith("dbfs:"):
        return path.replace("dbfs:", "/dbfs", 1)
    if path.startswith("/dbfs"):
        return path
    return f"/dbfs{path}"


def _to_dbfs_write_path(path):
    return path if path.startswith("/dbfs") else f"/dbfs{path.lstrip('/')}"


def _ensure_dir(dbutils, dbfs_dir):
    target = dbfs_dir.replace("/dbfs/", "dbfs:/", 1) if dbfs_dir.startswith("/dbfs/") else dbfs_dir
    try:
        dbutils.fs.mkdirs(target)
    except Exception as e:
        if "Directory already exists" not in str(e):
            print(f"  Warning: could not create {target}: {e}")


def process_3pl_file(
    dbutils,
    file_path,
    output_dir,
    site_id,
    segment,
    cmo_sheet_dict,
    cmo_column_dict,
):
    """Process a single 3PL inventory workbook into one CSV per relevant sheet.

    Args:
        file_path:        source xlsx path (dbfs:/... or /dbfs/...)
        output_dir:       destination directory (dbfs path, no trailing slash needed)
        site_id:          3PL site id (folder name); used to look up sheet/column mappings
        segment:          'clinical' | 'commercial'
        cmo_sheet_dict:   {site_id: [allowed sheet names lowercased]}
        cmo_column_dict:  {site_id: [expected column headers]}
    """
    read_path = _to_dbfs_read_path(file_path)
    print(f"  Reading {read_path}")
    xls = pd.ExcelFile(read_path)
    sheet_names = xls.sheet_names
    is_single_sheet = len(sheet_names) == 1
    allowed_sheets = cmo_sheet_dict.get(site_id, [])

    for sheet in sheet_names:
        sheet_lc = sheet.strip().lower()
        if not ((not allowed_sheets and is_single_sheet) or sheet_lc in allowed_sheets):
            print(f"    Skipping sheet '{sheet}' (not in mapping for site {site_id})")
            continue

        df = pd.read_excel(xls, sheet_name=sheet, header=None)
        if df.empty:
            print(f"    Skipping empty sheet '{sheet}'")
            continue

        boundaries = eu.find_table_boundaries(df, site_id, cmo_column_dict)
        if boundaries:
            data = boundaries["data"]
            header = boundaries.get("header")
            if header is not None:
                data.columns = header
            data = eu.remove_rows_with_n_values(data)
            data = eu.remove_aggregate_rows(data)
            data = eu.remove_special_characters(data)
        else:
            print(f"    No table boundaries found in '{sheet}', writing raw")
            data = df

        data["site_id"] = site_id
        data["segment"] = segment

        sheet_slug = sheet.strip().replace(" ", "_") or "sheet"
        out_subdir = output_dir.rstrip("/")
        _ensure_dir(dbutils, out_subdir)
        out_path = _to_dbfs_write_path(f"{out_subdir}/{sheet_slug}.csv")
        data.to_csv(out_path, index=False, encoding="utf-8")
        print(f"    Wrote {out_path}")


def process_sap_file(dbutils, file_path, output_dir, output_filename):
    """Process the quarterly SAP report into a single CSV."""
    read_path = _to_dbfs_read_path(file_path)
    print(f"  Reading {read_path}")
    df = pd.read_excel(read_path, header=None, engine="openpyxl")

    df = eu.remove_rows_with_n_values(df, 1)
    df = eu.extract_first_dataframe(df)
    df = eu.trim_rows_and_cols(df)
    df = eu.remove_aggregate_rows(df)
    df = eu.remove_special_characters(df)

    out_subdir = output_dir.rstrip("/")
    _ensure_dir(dbutils, out_subdir)
    out_path = _to_dbfs_write_path(f"{out_subdir}/{output_filename}")
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"  Wrote {out_path}")
