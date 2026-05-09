"""Load and combine API + DP header mapping workbooks for a quarter.

Produces:
    header_mapping_df: combined DataFrame with Type and Segment columns
    column_dict:   {site_id: [expected column headers]} (flattened)
    sheet_dict:    {site_id: [expected sheet names lowercased]}
"""
import os
import pandas as pd

from lib.raw.excel_utils import flattened_column_dict

_SITE_ID_COL    = "3PL"
_COL_HEADER_COL = "3PL Column Header"
_SHEET_NAME_COL = "Sheet Name"


def _load_sheet(file_path, sheet_name):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Mapping file not found: {file_path}")
    sheets = pd.read_excel(file_path, sheet_name=None, dtype=str)
    df = sheets.get(sheet_name)
    if df is None:
        raise ValueError(f"Sheet '{sheet_name}' not found in {file_path}")
    return df


def load_quarter_mappings(mapping_paths_by_segment, sheet_name):
    """Load and combine API+DP mapping files across all segments.

    Args:
        mapping_paths_by_segment: {segment: {"api": path, "dp": path}}
            Paths must be local-filesystem paths (e.g. /dbfs/mnt/...).
        sheet_name: name of the header-mapping sheet within each workbook

    Returns:
        (header_mapping_df, column_dict, sheet_dict)
    """
    frames = []
    for segment, paths in mapping_paths_by_segment.items():
        for type_key, type_label in (("dp", "DP"), ("api", "API")):
            file_path = paths.get(type_key)
            if not file_path:
                print(f"  No {type_key.upper()} mapping for segment '{segment}', skipping")
                continue
            df = _load_sheet(file_path, sheet_name).assign(
                CMO_Type=type_label, Segment=segment
            )
            frames.append(df)

    if not frames:
        raise RuntimeError("No mapping files loaded for the quarter")

    header_mapping_df = pd.concat(frames, ignore_index=True)
    print(f"  Loaded {len(header_mapping_df)} header-mapping rows across segments")

    raw_column_dict = (
        header_mapping_df.groupby(_SITE_ID_COL)[_COL_HEADER_COL].apply(list).to_dict()
    )
    column_dict = flattened_column_dict(raw_column_dict)

    header_mapping_df[_SHEET_NAME_COL] = (
        header_mapping_df.groupby(_SITE_ID_COL)[_SHEET_NAME_COL].ffill().str.lower()
    )
    header_mapping_df[_SHEET_NAME_COL] = header_mapping_df[_SHEET_NAME_COL].replace("nan", None)

    sheet_dict = (
        header_mapping_df.groupby(_SITE_ID_COL)[_SHEET_NAME_COL]
        .apply(
            lambda x: sorted(
                {s.strip() for name in x.dropna() for s in name.split(",")}
            )
        )
        .to_dict()
    )

    return header_mapping_df, column_dict, sheet_dict
