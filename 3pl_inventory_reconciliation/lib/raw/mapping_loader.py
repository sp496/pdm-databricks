"""Load and combine API + DP header mapping workbooks for a quarter.

Produces:
    header_mapping_df: combined DataFrame with CMO_Type and Segment columns
    cmo_column_dict:   {site_id: [expected column headers]}
    cmo_sheet_dict:    {site_id: [expected sheet names lowercased]}
"""
import os
import pandas as pd


def _load_sheet(file_path, sheet_name):
    dbfs_path = f"/dbfs{file_path}" if not file_path.startswith("/dbfs") else file_path
    if not os.path.exists(dbfs_path):
        raise FileNotFoundError(f"Mapping file not found: {dbfs_path}")
    sheets = pd.read_excel(dbfs_path, sheet_name=None)
    df = sheets.get(sheet_name)
    if df is None:
        raise ValueError(f"Sheet '{sheet_name}' not found in {dbfs_path}")
    return df.astype(str)


def _strip_dbfs_prefix(path):
    return path.replace("dbfs:", "") if path and path.startswith("dbfs:") else path


def load_quarter_mappings(mapping_paths_by_segment, sheet_name):
    """Load and combine API+DP mapping files across all segments.

    Args:
        mapping_paths_by_segment: {segment: {"api": path, "dp": path}}
        sheet_name: name of the header-mapping sheet within each workbook

    Returns:
        (header_mapping_df, cmo_column_dict, cmo_sheet_dict)
    """
    frames = []
    for segment, paths in mapping_paths_by_segment.items():
        for cmo_type_key, cmo_type_label in (("dp", "DP"), ("api", "API")):
            file_path = paths.get(cmo_type_key)
            if not file_path:
                print(f"  No {cmo_type_key.upper()} mapping for segment '{segment}', skipping")
                continue
            file_path = _strip_dbfs_prefix(file_path)
            df = _load_sheet(file_path, sheet_name).assign(
                CMO_Type=cmo_type_label, Segment=segment
            )
            frames.append(df)

    if not frames:
        raise RuntimeError("No mapping files loaded for the quarter")

    header_mapping_df = pd.concat(frames, ignore_index=True)
    print(f"  Loaded {len(header_mapping_df)} header-mapping rows across segments")

    cmo_column_dict = (
        header_mapping_df.groupby("CMO")["CMO Column Header"].apply(list).to_dict()
    )

    header_mapping_df["Sheet Name"] = (
        header_mapping_df.groupby("CMO")["Sheet Name"].ffill().str.lower()
    )
    header_mapping_df["Sheet Name"] = header_mapping_df["Sheet Name"].replace("nan", None)

    cmo_sheet_dict = (
        header_mapping_df.groupby("CMO")["Sheet Name"]
        .apply(
            lambda x: sorted(
                {s.strip() for name in x.dropna() for s in name.split(",")}
            )
        )
        .to_dict()
    )

    return header_mapping_df, cmo_column_dict, cmo_sheet_dict
