"""Load the per-quarter header mapping workbook(s) for one or more segments.

Produces:
    header_mapping_df:  combined DataFrame with a Segment column
    site_sheet_mapping: {site_id: {sheet_name: [expected_columns]}}
"""
import os
import re
import pandas as pd

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


def _split_items(raw, sep):
    """Split a raw cell value on sep, returning stripped non-empty parts."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return []
    return [s.strip() for s in re.split(sep, str(raw)) if s.strip()]


def load_quarter_mappings(mapping_path_by_segment, sheet_name):
    """Load the header-mapping sheet from one mapping file per segment.

    Args:
        mapping_path_by_segment: {segment: path}
            Paths must be local-filesystem paths (e.g. /dbfs/mnt/...).
        sheet_name: name of the header-mapping sheet within each workbook

    Returns:
        (header_mapping_df, site_sheet_mapping)
        where site_sheet_mapping = {site_id: {sheet_name: [expected_columns]}}
        sheet_name is None when no sheet was specified in the mapping (columns
        apply to any single-sheet workbook for that site)
    """
    frames = []
    for segment, file_path in mapping_path_by_segment.items():
        if not file_path:
            print(f"  No mapping file for segment '{segment}', skipping")
            continue
        df = _load_sheet(file_path, sheet_name).assign(Segment=segment)
        frames.append(df)

    if not frames:
        raise RuntimeError("No mapping files loaded for the quarter")

    header_mapping_df = pd.concat(frames, ignore_index=True)
    print(f"  Loaded {len(header_mapping_df)} header-mapping rows across segments")

    header_mapping_df[_SHEET_NAME_COL] = (
        header_mapping_df.groupby(_SITE_ID_COL)[_SHEET_NAME_COL].ffill().str.lower()
    )
    header_mapping_df[_SHEET_NAME_COL] = header_mapping_df[_SHEET_NAME_COL].replace("nan", None)

    site_sheet_mapping = {}
    for _, row in header_mapping_df.iterrows():
        site_id    = row[_SITE_ID_COL]
        sheet_names = _split_items(row[_SHEET_NAME_COL], sep=r",")
        columns     = _split_items(row[_COL_HEADER_COL], sep=r"[,+]")

        if site_id not in site_sheet_mapping:
            site_sheet_mapping[site_id] = {}

        # None key = columns with no sheet constraint (single-sheet files only)
        keys = sheet_names if sheet_names else [None]
        for key in keys:
            if key not in site_sheet_mapping[site_id]:
                site_sheet_mapping[site_id][key] = []
            site_sheet_mapping[site_id][key].extend(columns)

    return header_mapping_df, site_sheet_mapping
