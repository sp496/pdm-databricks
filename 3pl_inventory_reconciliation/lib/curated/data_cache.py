import os
import pandas as pd
from dataclasses import dataclass
from typing import Optional, Dict, Any

from common.backends import DataBackend, VALID_SOURCES
from .queries import get_queries

_COL_CLEAN_PATTERN = r'[ ,;{}()\n\t=]'


def quarter_end_date(year: str, quarter: str) -> str:
    quarter_end_map = {"Q1": "03-31", "Q2": "06-30", "Q3": "09-30", "Q4": "12-31"}
    quarter = quarter.upper()
    if quarter not in quarter_end_map:
        raise ValueError("quarter must be one of Q1, Q2, Q3, Q4")
    return f"{year}-{quarter_end_map[quarter]}"


@dataclass
class MappingFilePaths:
    api_mapping_file_path: str
    dp_mapping_file_path: str
    plant_name_mapping_file_path: str
    uom_master_file_path: str
    material_master_file_path: str
    lot_no_mapping_file_path: str
    material_description_file_path: str
    lot_no_master_file_path: str
    unit_cost_file_path: str
    material_type_file_path: str
    gil_receipts_file_path: str
    sap_report_file_path: str
    header_mapping_sheet_name: str
    item_mapping_sheet_name: str
    uom_mapping_sheet_name: str


class MappingDataCache:
    """Holds all reference dataframes loaded at pipeline startup."""

    def __init__(self):
        self.header_mapping_df = None
        self.pl_type_mapping_df = None
        self.plant_name_mapping_df = None
        self.item_mapping_df = None
        self.material_master_df = None
        self.lot_no_master_df = None
        self.lot_no_mapping_df = None
        self.sap_report_df = None
        self.uom_mapping_df = None
        self.uom_master_df = None
        self.unit_cost_df = None
        self.material_type_df = None
        self.material_description_df = None
        self.gil_receipts_df = None


def _load_excel_sheet(file_path: str, sheet_name: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Required mapping file not found at {file_path}")
    print(f"\t\tLoading sheet '{sheet_name}' from {file_path}...")
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
    except Exception as e:
        raise Exception(f"Failed to read sheet '{sheet_name}' from file {file_path}: {e}")
    return df.astype(str)


def _load_and_combine_sheet(dp_path: str, api_path: str, sheet_name: str) -> pd.DataFrame:
    dp_df = _load_excel_sheet(dp_path, sheet_name).assign(**{'3PL_Type': 'DP'})
    api_df = _load_excel_sheet(api_path, sheet_name).assign(**{'3PL_Type': 'API'})
    combined = pd.concat([dp_df, api_df], ignore_index=True)
    combined.columns = combined.columns.str.replace(_COL_CLEAN_PATTERN, '_', regex=True)
    return combined


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
        cache.header_mapping_df = _load_and_combine_sheet(
            file_paths.dp_mapping_file_path,
            file_paths.api_mapping_file_path,
            file_paths.header_mapping_sheet_name
        )
        print(f"\tSuccessfully loaded and combined Header Mappings ({len(cache.header_mapping_df)} rows)")
        cache.pl_type_mapping_df = cache.header_mapping_df[['3PL', '3PL_Type']].drop_duplicates(
            ignore_index=True).astype(str)
    except FileNotFoundError as e:
        print(f"\tFailed to load Header Mappings (File Not Found): {e}")
        raise
    except Exception as e:
        print(f"\tFailed to load Header Mappings: {e}")
        raise

    try:
        print("\tLoading Item Mappings...")
        cache.item_mapping_df = _load_and_combine_sheet(
            file_paths.dp_mapping_file_path,
            file_paths.api_mapping_file_path,
            file_paths.item_mapping_sheet_name
        )
        print(f"\tSuccessfully loaded and combined Item Mappings ({len(cache.item_mapping_df)} rows)")
    except FileNotFoundError as e:
        print(f"\tFailed to load Item Mappings (File Not Found): {e}")
        raise
    except Exception as e:
        print(f"\tFailed to load Item Mappings: {e}")
        raise

    try:
        print("\tLoading UOM Mappings...")
        cache.uom_mapping_df = _load_and_combine_sheet(
            file_paths.dp_mapping_file_path,
            file_paths.api_mapping_file_path,
            file_paths.uom_mapping_sheet_name
        )
        print(f"\tSuccessfully loaded and combined UOM Mappings ({len(cache.uom_mapping_df)} rows)")
    except FileNotFoundError as e:
        print(f"\tFailed to load UOM Mappings (File Not Found): {e}")
        raise
    except Exception as e:
        print(f"\tFailed to load UOM Mappings: {e}")
        raise

    print("\tLoading SAP report...")
    if not os.path.exists(file_paths.sap_report_file_path):
        raise FileNotFoundError(f"Required file not found: sap_report at {file_paths.sap_report_file_path}")
    cache.sap_report_df = pd.read_csv(file_paths.sap_report_file_path, dtype=str)
    cache.sap_report_df.columns = cache.sap_report_df.columns.str.replace(_COL_CLEAN_PATTERN, '_', regex=True)

    print("File-based mappings loaded successfully!")
    return cache


def load_mapping_files(
        file_paths: MappingFilePaths,
        year: str,
        quarter: str,
        data_source: str = "spark",
        starburst_config: Optional[Dict[str, Any]] = None,
) -> MappingDataCache:
    """
    Load all reference data and return a fully populated MappingDataCache.

    Calls load_file_mappings() first for the Excel/CSV datasets, then fetches
    the remaining datasets from the live source with file fallback.

    Args:
        file_paths: All file paths and sheet names needed for loading.
        data_source: Where to load live data from. One of:
            'spark'      — prod, queries via Spark SQL against Databricks tables.
            'starburst'  — dev, queries via Starburst/Trino JDBC (requires starburst_config).
            'file'       — local/testing, skips all live queries and loads from files directly.
        starburst_config: Required when data_source='starburst'. Dict with keys:
            base_url, username, password, default_catalog, default_schema.
    """
    cache = load_file_mappings(file_paths)

    backend = DataBackend(data_source=data_source, starburst_config=starburst_config)
    queries = get_queries(quarter_end_date(year, quarter), data_source)

    print("Loading live-query datasets...")

    print("\tLoading Plant name mapping...")
    cache.plant_name_mapping_df = backend.load(
        'plant_name_mapping', queries['plant_name_mapping'],
        file_paths.plant_name_mapping_file_path, pd.read_csv, dtype=str)

    print("\tLoading material master...")
    cache.material_master_df = backend.load(
        'material_master', queries['material_master'],
        file_paths.material_master_file_path, pd.read_csv, dtype=str)

    print("\tLoading lot number master...")
    cache.lot_no_master_df = backend.load(
        'lot_no_master', queries['lot_no_master'],
        file_paths.lot_no_master_file_path, pd.read_csv, dtype=str)

    print("\tLoading lot number mapping...")
    cache.lot_no_mapping_df = backend.load(
        'lot_no_mapping', queries['lot_no_mapping'],
        file_paths.lot_no_mapping_file_path, pd.read_csv, dtype=str)

    print("\tLoading material description...")
    cache.material_description_df = backend.load(
        'material_description', queries['material_description'],
        file_paths.material_description_file_path, pd.read_csv, dtype=str)

    print("\tLoading UOM master...")
    cache.uom_master_df = backend.load(
        'uom_master', queries['uom_master'],
        file_paths.uom_master_file_path, pd.read_csv, dtype=str)

    print("\tLoading unit cost...")
    cache.unit_cost_df = backend.load(
        'unit_cost', queries['unit_cost'],
        file_paths.unit_cost_file_path, pd.read_csv, dtype=str)

    print("\tLoading material type...")
    cache.material_type_df = backend.load(
        'material_type', queries['material_type'],
        file_paths.material_type_file_path, pd.read_csv, dtype=str)

    print("\tLoading Gilead receipts...")
    cache.gil_receipts_df = backend.load(
        'gilead_receipts', queries['gilead_receipts'],
        file_paths.gil_receipts_file_path, pd.read_csv, dtype=str)

    print("All mapping files loaded successfully!")
    return cache
