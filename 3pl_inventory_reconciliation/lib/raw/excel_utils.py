"""Excel cleaning utilities, ported from the legacy cmo_inventory_utils notebook."""
import re
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Column-dict helpers
# ---------------------------------------------------------------------------

def flattened_column_dict(column_dict):
    """Flatten comma/plus-separated column header entries into individual strings.

    e.g. {"1121": ["Lot No., Supplier Lot", "Qty"]} → {"1121": ["Lot No.", "Supplier Lot", "Qty"]}
    """
    result = {}
    for site_id, column_list in column_dict.items():
        flat = []
        for item in column_list:
            if item is None or (isinstance(item, float) and pd.isna(item)):
                continue
            parts = [s.strip() for s in re.split(r"[,+]", str(item)) if s.strip()]
            flat.extend(parts)
        result[site_id] = flat
    return result


# ---------------------------------------------------------------------------
# Table boundary detection
# ---------------------------------------------------------------------------

def find_table_boundaries(df, expected_columns):
    """Locate the header row and data block within a sparsely-populated sheet.

    Args:
        df: raw sheet read with header=None.
        expected_columns: list of header names to look for. Used both to locate
            the header row and to apply the null-ratio filter. Pass [] if none.

    Returns dict with keys: start_row, end_row, start_col, end_col, header, data.
    Returns None if no usable table is found.
    """
    if df.empty:
        print("Error: DataFrame is empty.")
        return None

    # Drop columns that are entirely null (trim trailing blank columns)
    for col_name in df.columns:
        if df[col_name].isnull().all():
            col_index = df.columns.get_loc(col_name)
            df = df.iloc[:, :col_index].copy()
            break

    expected_columns = [col.lower() for col in expected_columns]

    # --- Locate header row ---
    start_row = None

    min_matches = 2
    for idx, row in df.iterrows():
        row_values = [str(x).strip().lower() for x in row.dropna().tolist()]
        matches = sum(1 for col in expected_columns if col in row_values)
        if matches >= min_matches:
            start_row = idx
            print(f"Header found at row {start_row} ({matches} column matches)")
            break

    if start_row is None:
        for idx, row in df.iterrows():
            if row.notnull().all():
                start_row = idx
                print(f"Header found at row {start_row} (all-non-null fallback)")
                break

    if start_row is None:
        for i in range(df.shape[0]):
            if df.iloc[i].count() >= 3:
                start_row = i
                print(f"Header found at row {start_row} (≥3 values fallback)")
                break

    if start_row is None:
        print("Error: Could not identify a header row.")
        return None

    # --- Locate start/end columns from header row ---
    header = [str(h) for h in df.iloc[start_row].tolist()]

    start_col = 0
    for i, val in enumerate(header):
        if val.strip() not in ("nan", ""):
            start_col = i
            break
    else:
        print("Error: No valid start column found in header.")
        return None

    end_col = len(header) - 1
    for i in range(len(header) - 1, start_col - 1, -1):
        if header[i].strip() not in ("nan", ""):
            end_col = i
            break

    # --- Locate end row ---
    end_row = df.shape[0] - 1
    for i in range(df.shape[0] - 1, start_row, -1):
        if df.iloc[i, start_col:end_col + 1].count() > 0:
            end_row = i
            break

    # --- Extract data ---
    data = df.iloc[start_row + 1:end_row + 1, start_col:end_col + 1].copy()
    data.columns = header[start_col:end_col + 1]
    data = data.dropna(how="all")

    # 60% null-ratio filter on expected columns only
    final_data = data.copy()
    data_cols_lower = [col.lower() for col in final_data.columns]
    relevant_cols = [
        col for col, col_l in zip(final_data.columns, data_cols_lower)
        if col_l in expected_columns
    ]
    if relevant_cols:
        null_ratio = final_data[relevant_cols].isna().sum(axis=1) / len(relevant_cols)
        filtered = final_data[null_ratio <= 0.6]
        final_data = data if filtered.empty else filtered

    return {
        "start_row": start_row,
        "end_row": end_row,
        "start_col": start_col,
        "end_col": end_col,
        "header": header[start_col:end_col + 1],
        "data": final_data,
    }


# ---------------------------------------------------------------------------
# Row / column trimming
# ---------------------------------------------------------------------------

def _trim_cols(df):
    return df.dropna(axis=1, how="all")


def _trim_rows(df):
    """Find first fully non-null row, use it as header, return remaining rows."""
    start_row = None
    for idx, row in df.iterrows():
        if row.notnull().all():
            start_row = idx
            break
    if start_row is None:
        raise ValueError("No fully non-null row found to use as header.")
    result = df.iloc[start_row + 1:].reset_index(drop=True)
    result.columns = df.iloc[start_row]
    result.columns.name = None
    return result.dropna(how="all").reset_index(drop=True)


def trim_rows_and_cols(df):
    return _trim_rows(_trim_cols(df))


# ---------------------------------------------------------------------------
# Row filtering
# ---------------------------------------------------------------------------

def remove_rows_with_n_values(df, n=1):
    """Drop rows whose count of non-null values is <= n."""
    mask = df.notna().sum(axis=1) > n
    return df[mask].reset_index(drop=True)


def remove_aggregate_rows(df):
    """Drop rows that contain the word 'total' (case-insensitive) in any cell."""
    mask = df.apply(
        lambda row: row.astype(str).str.contains(r"total", case=False, na=False).any(),
        axis=1,
    )
    return df[~mask]


# ---------------------------------------------------------------------------
# String cleaning
# ---------------------------------------------------------------------------

def remove_special_characters(df):
    """Replace non-breaking spaces, collapse whitespace in cells and column names."""
    def _clean(x):
        if isinstance(x, str):
            return re.sub(r"\s+", " ", x.replace("\xa0", " ")).strip()
        return x

    def _clean_col(col):
        if isinstance(col, str):
            return re.sub(r"\s+", " ", col.replace("\xa0", " ")).strip()
        return str(col).strip()

    try:
        cleaned = df.applymap(_clean)  # pandas < 2.1
    except AttributeError:
        cleaned = df.map(_clean)       # pandas >= 2.1

    cleaned.columns = [_clean_col(c) for c in cleaned.columns]
    return cleaned


# ---------------------------------------------------------------------------
# Multi-table sheet handling (SAP report)
# ---------------------------------------------------------------------------

def extract_first_dataframe(df):
    """When a sheet contains multiple side-by-side tables, return only the first."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame.")

    empty_col_indices = np.where(df.isna().all())[0].tolist()
    start_col = 0
    for col_idx in empty_col_indices + [df.shape[1]]:
        if start_col < col_idx:
            table = df.iloc[:, start_col:col_idx]
            if not table.empty:
                return table
        start_col = col_idx + 1

    return pd.DataFrame()
