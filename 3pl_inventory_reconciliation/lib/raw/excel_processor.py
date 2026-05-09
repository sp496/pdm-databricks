"""Excel → CSV processing for 3PL inventory and SAP report files."""
import pandas as pd

from lib.raw import excel_utils as eu


def _to_dbfs_read_path(path):
    if path.startswith("dbfs:"):
        return path.replace("dbfs:", "/dbfs", 1)
    if path.startswith("/dbfs"):
        return path
    return f"/dbfs{path}"


def process_3pl_file(file_path, site_id, segment, sheet_dict, column_dict):
    """Process a single 3PL inventory workbook, returning one DataFrame per relevant sheet.

    Returns:
        list of (sheet_slug, DataFrame) tuples
    """
    read_path = _to_dbfs_read_path(file_path)
    print(f"  Reading {read_path}")
    xls = pd.ExcelFile(read_path)
    sheet_names = xls.sheet_names
    is_single_sheet = len(sheet_names) == 1
    allowed_sheets = sheet_dict.get(site_id, [])

    results = []
    for sheet in sheet_names:
        sheet_lc = sheet.strip().lower()
        if not ((not allowed_sheets and is_single_sheet) or sheet_lc in allowed_sheets):
            print(f"    Skipping sheet '{sheet}' (not in mapping for site {site_id})")
            continue

        df = pd.read_excel(xls, sheet_name=sheet, header=None)
        if df.empty:
            print(f"    Skipping empty sheet '{sheet}'")
            continue

        boundaries = eu.find_table_boundaries(df, column_dict.get(site_id, []))
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

        data["3pl"] = site_id
        data["segment"] = segment

        sheet_slug = sheet.strip().replace(" ", "_") or "sheet"
        results.append((sheet_slug, data))

    return results


def process_sap_file(file_path):
    """Process the quarterly SAP report, returning a cleaned DataFrame."""
    read_path = _to_dbfs_read_path(file_path)
    print(f"  Reading {read_path}")
    df = pd.read_excel(read_path, header=None, engine="openpyxl")

    df = eu.remove_rows_with_n_values(df, 1)
    df = eu.extract_first_dataframe(df)
    df = eu.trim_rows_and_cols(df)
    df = eu.remove_aggregate_rows(df)
    df = eu.remove_special_characters(df)

    return df
