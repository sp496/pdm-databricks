import os
from collections import defaultdict
import pandas as pd
from dataclasses import dataclass
from typing import Optional, Dict, Any

from common.backends import DataBackend
from .queries import get_queries, get_clinical_queries

_COL_CLEAN_PATTERN = r'[ ,;{}()\n\t=]'


def quarter_end_date(year: str, quarter: str) -> str:
    quarter_end_map = {"Q1": "03-31", "Q2": "06-30", "Q3": "09-30", "Q4": "12-31"}
    quarter = quarter.upper()
    if quarter not in quarter_end_map:
        raise ValueError("quarter must be one of Q1, Q2, Q3, Q4")
    return f"{year}-{quarter_end_map[quarter]}"


@dataclass
class MappingFilePaths:
    """Paths to the per-quarter Excel mapping file and optional SAP report."""
    mapping_file_path: str
    header_mapping_sheet_name: str
    item_mapping_sheet_name: str
    uom_mapping_sheet_name: str
    sap_report_file_path: Optional[str] = None


@dataclass
class RefFilePaths:
    """Paths to the static reference CSV files on the data bucket mount.

    All fields are optional — set a field to None (or omit it) to skip loading
    that dataset entirely.  Only the datasets whose path is provided will be
    fetched (live query + CSV fallback).
    """
    plant_name_mapping_file_path:   Optional[str] = None
    material_master_file_path:      Optional[str] = None
    lot_no_master_file_path:        Optional[str] = None
    lot_no_mapping_file_path:       Optional[str] = None
    material_description_file_path: Optional[str] = None
    uom_master_file_path:           Optional[str] = None
    unit_cost_file_path:            Optional[str] = None
    material_type_file_path:        Optional[str] = None


class MappingDataCache:
    """Holds all reference dataframes loaded at pipeline startup."""

    def __init__(self):
        self.header_mapping_df = None
        self.header_mapping = None     # pre-built dict from build_header_mapping()
        self.plant_name_mapping_df = None
        self.item_mapping_df = None
        self.material_master_df = None
        self.lot_no_master_df = None
        self.lot_no_mapping_df = None
        self.lot_no_mapping_lookup = None  # {matnr: [(atwrt, charg), ...]} — built once from lot_no_mapping_df
        self.sap_report_df = None
        self.uom_mapping_df = None
        self.uom_master_df = None
        self.unit_cost_df = None
        self.material_type_df = None
        self.material_description_df = None


def load_sap_report_file(path: str) -> pd.DataFrame:
    """Read a SAP report CSV and apply the standard column-name cleanup.

    Used by:
      - notebooks/curated/write_inventory_tables.py (direct caller)
      - load_file_mappings (internal — kept for backwards compatibility;
        the SAP path through that loader is no longer exercised by any
        in-tree notebook).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"SAP report not found at {path}")
    df = pd.read_csv(path, dtype=str)
    df.columns = df.columns.str.replace(_COL_CLEAN_PATTERN, '_', regex=True)
    return df


def _load_excel_sheet(file_path: str, sheet_name: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Required mapping file not found at {file_path}")
    print(f"\t\tLoading sheet '{sheet_name}' from {file_path}...")
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
    except Exception as e:
        raise Exception(f"Failed to read sheet '{sheet_name}' from file {file_path}: {e}")
    return df


def _load_sheet(file_path: str, sheet_name: str) -> pd.DataFrame:
    """Load one sheet from the per-quarter mapping workbook.

    Applies the standard column-name cleanup (`_COL_CLEAN_PATTERN`) and
    strips non-breaking spaces. Replaces the legacy DP/API split helper —
    the new layout ships one workbook per segment, with no DP/API tagging.
    """
    df = _load_excel_sheet(file_path, sheet_name)
    df.columns = df.columns.str.replace(_COL_CLEAN_PATTERN, '_', regex=True)
    df = df.replace("\xa0", "", regex=False)
    return df


def _coerce_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all columns to string, preserving nulls as NaN.

    Used for live-query results (Spark/Starburst) which return mixed dtypes.
    File-based loads use dtype=str directly, which already preserves nulls as NaN.
    """
    for col in df.columns:
        df[col] = df[col].where(df[col].isna(), df[col].astype(str))
    return df


def build_header_mapping(header_mapping_df: pd.DataFrame) -> dict:
    """Normalise the header mapping DataFrame and build a lookup dict.

    Built once after loading the cache and stored as MappingDataCache.header_mapping.

    Returns {combined_key: {3pl_col: gilead_col}} where combined_key is
    either "{site_id}_{sheet_name}" or plain "{site_id}" for single-sheet sites.
    """
    hdf = header_mapping_df.dropna(subset=["3PL"]).copy()
    hdf["Sheet_Name"] = (
        hdf.groupby("3PL")["Sheet_Name"].ffill()
        .str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    )
    hdf["3PL_Column_Header"] = (
        hdf["3PL_Column_Header"].str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    )
    hdf["3PL"] = hdf["3PL"].astype("int").astype("str")
    hdf["combined_key"] = hdf.apply(
        lambda row: (
            f"{row['3PL']}_{row['Sheet_Name'].replace(' ', '_')}"
            if pd.notna(row.get("Sheet_Name")) and row.get("Sheet_Name")
            else row["3PL"]
        ),
        axis=1,
    )
    return (
        hdf.groupby("combined_key")
        .apply(lambda g: dict(zip(g["3PL_Column_Header"], g["Gilead_Column_Header"])))
        .to_dict()
    )


def _build_lot_no_mapping_lookup(lot_no_mapping_df: pd.DataFrame) -> dict:
    """Pre-process lot_no_mapping_df into a dict for fast per-material lookup.

    Returns {matnr: [(atwrt, charg), ...]} with leading zeros stripped from atwrt
    and duplicates removed — ready for the wildcard substring match in map_lot_no_wildcard.
    """
    lnm = lot_no_mapping_df.dropna(subset=["atwrt", "charg", "matnr"]).copy().astype(str)
    lnm["atwrt"] = lnm["atwrt"].str.lstrip("0")
    lnm = lnm.drop_duplicates(["charg", "matnr", "atwrt"])
    lookup = defaultdict(list)
    for row in lnm.itertuples(index=False):
        lookup[row.matnr].append((row.atwrt, row.charg))
    return lookup


def load_file_mappings(file_paths: MappingFilePaths) -> MappingDataCache:
    """
    Load only the datasets that always come from files — Excel mapping sheets
    and the SAP report. No live connection required.

    Can be called independently when you only need these datasets (e.g. writing
    them to tables), or internally by load_mapping_files to populate the full cache.
    """
    print("Loading file-based mappings...")
    cache = MappingDataCache()

    try:
        print("\tLoading header mappings...")
        cache.header_mapping_df = _load_sheet(
            file_paths.mapping_file_path,
            file_paths.header_mapping_sheet_name,
        )
        print(f"\tSuccessfully loaded Header Mapping ({len(cache.header_mapping_df)} rows)")
        cache.header_mapping = build_header_mapping(cache.header_mapping_df)
    except FileNotFoundError as e:
        print(f"\tFailed to load Header Mapping (File Not Found): {e}")
        raise
    except Exception as e:
        print(f"\tFailed to load Header Mapping: {e}")
        raise

    try:
        print("\tLoading Item Mappings...")
        cache.item_mapping_df = _load_sheet(
            file_paths.mapping_file_path,
            file_paths.item_mapping_sheet_name,
        )
        print(f"\tSuccessfully loaded Item Mappings ({len(cache.item_mapping_df)} rows)")
        cache.item_mapping_df = cache.item_mapping_df.dropna(subset=["3PL_Part"])
    except FileNotFoundError as e:
        print(f"\tFailed to load Item Mappings (File Not Found): {e}")
        raise
    except Exception as e:
        print(f"\tFailed to load Item Mappings: {e}")
        raise

    try:
        print("\tLoading UOM Mappings...")
        cache.uom_mapping_df = _load_sheet(
            file_paths.mapping_file_path,
            file_paths.uom_mapping_sheet_name,
        )
        print(f"\tSuccessfully loaded UOM Mappings ({len(cache.uom_mapping_df)} rows)")
        cache.uom_mapping_df = cache.uom_mapping_df.dropna(subset=["3PL_Part"])
        cache.uom_mapping_df["Conversion_Factor"] = cache.uom_mapping_df["Conversion_Factor"].astype(float)
    except FileNotFoundError as e:
        print(f"\tFailed to load UOM Mappings (File Not Found): {e}")
        raise
    except Exception as e:
        print(f"\tFailed to load UOM Mappings: {e}")
        raise

    print("\tLoading SAP report...")
    if not file_paths.sap_report_file_path:
        print("\tNo SAP report path provided — skipping")
    elif not os.path.exists(file_paths.sap_report_file_path):
        print(f"\tSAP report not found at {file_paths.sap_report_file_path} — skipping")
    else:
        cache.sap_report_df = load_sap_report_file(file_paths.sap_report_file_path)

    print("File-based mappings loaded successfully!")
    return cache


def load_mapping_files(
        file_paths: MappingFilePaths,
        ref_paths: RefFilePaths,
        year: str,
        quarter: str,
        data_source: str = "spark",
        starburst_config: Optional[Dict[str, Any]] = None,
        segment: str = "commercial",
) -> MappingDataCache:
    """
    Load all reference data and return a fully populated MappingDataCache.

    Calls load_file_mappings() first for the Excel/CSV datasets, then fetches
    the remaining datasets from the live source with file fallback.

    Args:
        file_paths: Per-quarter Excel mapping files and SAP report.
        ref_paths: Static reference CSV file paths on the data bucket mount.
        data_source: Where to load live data from. One of:
            'spark'      — prod, queries via Spark SQL against Databricks tables.
            'starburst'  — dev, queries via Starburst/Trino JDBC (requires starburst_config).
            'file'       — local/testing, skips all live queries and loads from files directly.
        starburst_config: Required when data_source='starburst'. Dict with keys:
            base_url, username, password, default_catalog, default_schema.
        segment: 'commercial' (SAP/S4H reference queries) or 'clinical'
            (EBS reference queries). Selects which query set to run; the result
            column names are identical (the clinical queries alias to the same
            canonical names), so the downstream transforms are segment-agnostic.
            The clinical set has no 'unit_cost' — callers should pass
            ref_paths.unit_cost_file_path=None for clinical so that block skips.
    """
    cache = load_file_mappings(file_paths)

    backend = DataBackend(data_source=data_source, starburst_config=starburst_config)
    queries = (
        get_clinical_queries(data_source)
        if segment == "clinical"
        else get_queries(quarter_end_date(year, quarter), data_source)
    )

    print("Loading live-query datasets...")

    if ref_paths.plant_name_mapping_file_path is not None:
        print("\tLoading Plant name mapping...")
        cache.plant_name_mapping_df = _coerce_string_columns(backend.load(
            'plant_name_mapping', queries['plant_name_mapping'],
            ref_paths.plant_name_mapping_file_path, pd.read_csv, dtype=str))

    if ref_paths.material_master_file_path is not None:
        print("\tLoading material master...")
        cache.material_master_df = _coerce_string_columns(backend.load(
            'material_master', queries['material_master'],
            ref_paths.material_master_file_path, pd.read_csv, dtype=str))

    if ref_paths.lot_no_master_file_path is not None:
        print("\tLoading lot number master...")
        cache.lot_no_master_df = _coerce_string_columns(backend.load(
            'lot_no_master', queries['lot_no_master'],
            ref_paths.lot_no_master_file_path, pd.read_csv, dtype=str))

    if ref_paths.lot_no_mapping_file_path is not None:
        print("\tLoading lot number mapping...")
        cache.lot_no_mapping_df = _coerce_string_columns(backend.load(
            'lot_no_mapping', queries['lot_no_mapping'],
            ref_paths.lot_no_mapping_file_path, pd.read_csv, dtype=str))
        cache.lot_no_mapping_lookup = _build_lot_no_mapping_lookup(cache.lot_no_mapping_df)

    if ref_paths.material_description_file_path is not None:
        print("\tLoading material description...")
        cache.material_description_df = _coerce_string_columns(backend.load(
            'material_description', queries['material_description'],
            ref_paths.material_description_file_path, pd.read_csv, dtype=str))

    if ref_paths.uom_master_file_path is not None:
        print("\tLoading UOM master...")
        cache.uom_master_df = _coerce_string_columns(backend.load(
            'uom_master', queries['uom_master'],
            ref_paths.uom_master_file_path, pd.read_csv, dtype=str))

    if ref_paths.unit_cost_file_path is not None:
        print("\tLoading unit cost...")
        cache.unit_cost_df = _coerce_string_columns(backend.load(
            'unit_cost', queries['unit_cost'],
            ref_paths.unit_cost_file_path, pd.read_csv, dtype=str))
        cache.unit_cost_df["standard_cost_usd"] = cache.unit_cost_df["standard_cost_usd"].astype(float)

    if ref_paths.material_type_file_path is not None:
        print("\tLoading material type...")
        cache.material_type_df = _coerce_string_columns(backend.load(
            'material_type', queries['material_type'],
            ref_paths.material_type_file_path, pd.read_csv, dtype=str))

    print("Live-query datasets loaded successfully!")
    return cache
