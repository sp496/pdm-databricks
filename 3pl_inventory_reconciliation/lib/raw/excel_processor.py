"""Excel → CSV processing for 3PL inventory and SAP report files."""
import pandas as pd

from lib.raw import excel_utils as eu


def process_3pl_file(file_path, site_id, site_sheet_mapping):
    """Process a single 3PL inventory workbook, returning one DataFrame per relevant sheet.

    Args:
        file_path:          plain readable path to the source xlsx
        site_id:            3PL site id; used to look up sheet/column mappings
        site_sheet_mapping: {site_id: {sheet_name: [expected_columns]}}

    Returns:
        list of (sheet_slug, DataFrame) tuples
    """
    if site_id not in site_sheet_mapping:
        print(f"  Skipping {file_path} (site {site_id} not found in mapping)")
        return []

    print(f"  Reading {file_path}")
    xls = pd.ExcelFile(file_path)
    sheet_names = xls.sheet_names
    is_single_sheet = len(sheet_names) == 1
    site_mapping = site_sheet_mapping[site_id]
    named_sheets = {k: v for k, v in site_mapping.items() if k is not None}

    results = []
    for sheet in sheet_names:
        sheet_lc = sheet.strip().lower()

        if named_sheets:
            if sheet_lc not in named_sheets:
                print(f"    Skipping sheet '{sheet}' (not in mapping for site {site_id})")
                continue
            columns = named_sheets[sheet_lc]
        elif is_single_sheet:
            columns = site_mapping.get(None, [])
        else:
            print(f"    Skipping '{sheet}' (no sheet specified in mapping and file has multiple sheets)")
            continue

        df = pd.read_excel(xls, sheet_name=sheet, header=None)
        if df.empty:
            print(f"    Skipping empty sheet '{sheet}'")
            continue

        boundaries = eu.find_table_boundaries(df, columns)
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

        sheet_slug = sheet.strip().replace(" ", "_") or "sheet"
        results.append((sheet_slug, data))

    return results


def process_sap_file(file_path):
    """Process the quarterly SAP report, returning a cleaned DataFrame.

    Args:
        file_path: plain readable path to the source xlsx
    """
    print(f"  Reading {file_path}")
    df = pd.read_excel(file_path, header=None, engine="openpyxl")

    df = eu.remove_rows_with_n_values(df, 1)
    df = eu.extract_first_dataframe(df)
    df = eu.trim_rows_and_cols(df)
    df = eu.remove_aggregate_rows(df)
    df = eu.remove_special_characters(df)

    return df
