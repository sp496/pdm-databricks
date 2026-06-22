"""
Local Debug Runner for MappingDataCache
========================================
Run this script directly in PyCharm (or any Python interpreter) to test
load_mapping_files() against local sample files — no Databricks cluster
or Starburst connection required.  All live queries fall back to the CSV/
Excel files you place in tests/fixtures/curated/.

PyCharm Setup (one-time):
  1. Right-click `3pl_inventory_reconciliation/` → Mark Directory as → Sources Root
  2. Ensure your interpreter has: pandas, openpyxl   (pip install -r requirements.txt)
  3. Press the green Run button, or set breakpoints in data_cache.py and press Debug.

Fallback files expected in tests/fixtures/curated/
  (create the folder and drop your CSVs there — filenames match the constants below)

  plant_name_mapping.csv
  material_master.csv
  lot_no_master.csv
  lot_no_mapping.csv
  material_description.csv
  uom_master.csv
  unit_cost.csv
  material_type.csv
  sap_report.csv

Mapping Excel file (already in tests/fixtures/):
  3PL_commercial_mapping_2026_Q1.xlsx
"""

import os
import sys
import logging

_THIS_DIR     = os.path.dirname(os.path.abspath(__file__))   # .../tests/unit
_TESTS_DIR    = os.path.dirname(_THIS_DIR)                   # .../tests
_PROJECT_ROOT = os.path.dirname(_TESTS_DIR)                  # .../3pl_inventory_reconciliation
_REPO_ROOT    = os.path.dirname(_PROJECT_ROOT)               # .../pdm-databricks
for _p in [_REPO_ROOT, _PROJECT_ROOT]:
    if _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)

from lib.curated.data_cache import MappingFilePaths, RefFilePaths, MappingDataCache, load_mapping_files

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_FIXTURES_DIR = os.path.join(_TESTS_DIR, "fixtures")
_CURATED_DIR  = os.path.join(_FIXTURES_DIR, "curated")      # drop fallback CSVs here
_OUTPUTS_DIR  = os.path.join(_TESTS_DIR, "outputs", "curated")


# ===========================================================================
# CONFIG — update year/quarter as needed
# ===========================================================================

YEAR    = "2026"
QUARTER = "Q1"

FILE_PATHS = MappingFilePaths(
    mapping_file_path         = os.path.join(_FIXTURES_DIR, "3PL_commercial_mapping_2026_Q1.xlsx"),
    header_mapping_sheet_name = "Header Mapping",
    item_mapping_sheet_name   = "Item Mapping",
    uom_mapping_sheet_name    = "UOM Mapping",
    lot_mapping_sheet_name    = "Lot Mapping",
    sap_report_file_path      = os.path.join(_FIXTURES_DIR, "sap_report.csv"),
)

REF_PATHS = RefFilePaths(
    plant_name_mapping_file_path   = os.path.join(_CURATED_DIR, "plant_name_mapping.csv"),
    material_master_file_path      = os.path.join(_CURATED_DIR, "material_master.csv"),
    lot_no_master_file_path        = os.path.join(_CURATED_DIR, "lot_no_master.csv"),
    lot_no_mapping_file_path       = os.path.join(_CURATED_DIR, "lot_no_mapping.csv"),
    material_description_file_path = os.path.join(_CURATED_DIR, "material_description.csv"),
    uom_master_file_path           = os.path.join(_CURATED_DIR, "uom_master.csv"),
    unit_cost_file_path            = os.path.join(_CURATED_DIR, "unit_cost.csv"),
    material_type_file_path        = os.path.join(_CURATED_DIR, "material_type.csv"),
)


# ===========================================================================
# Helpers
# ===========================================================================

def _print_df_summary(name: str, df):
    if df is None:
        logger.warning(f"  {name}: None")
        return
    logger.info(f"  {name}: {df.shape[0]} rows × {df.shape[1]} cols | columns: {list(df.columns)}")
    print(df.head(3).to_string())
    print()


def _write_outputs(cache: MappingDataCache):
    os.makedirs(_OUTPUTS_DIR, exist_ok=True)
    fields = {
        "header_mapping":       cache.header_mapping_df,
        "item_mapping":         cache.item_mapping_df,
        "uom_mapping":          cache.uom_mapping_df,
        "plant_name_mapping":   cache.plant_name_mapping_df,
        "material_master":      cache.material_master_df,
        "lot_no_master":        cache.lot_no_master_df,
        "lot_no_mapping":       cache.lot_no_mapping_df,
        "material_description": cache.material_description_df,
        "uom_master":           cache.uom_master_df,
        "unit_cost":            cache.unit_cost_df,
        "material_type":        cache.material_type_df,
        "sap_report":           cache.sap_report_df,
    }
    for name, df in fields.items():
        if df is not None and not df.empty:
            out_path = os.path.join(_OUTPUTS_DIR, f"{name}.csv")
            df.to_csv(out_path, index=False)
            logger.info(f"  Written: {out_path}")


# ===========================================================================
# Main
# ===========================================================================

def main():
    logger.info("=" * 70)
    logger.info("MappingDataCache — local debug runner")
    logger.info(f"Year: {YEAR}  Quarter: {QUARTER}")
    logger.info(f"Fallback files directory: {_CURATED_DIR}")
    logger.info("=" * 70)

    # Warn about any missing fallback files up front
    missing = [
        path for path in [
            FILE_PATHS.mapping_file_path,
            FILE_PATHS.sap_report_file_path,
            REF_PATHS.plant_name_mapping_file_path,
            REF_PATHS.material_master_file_path,
            REF_PATHS.lot_no_master_file_path,
            REF_PATHS.lot_no_mapping_file_path,
            REF_PATHS.material_description_file_path,
            REF_PATHS.uom_master_file_path,
            REF_PATHS.unit_cost_file_path,
            REF_PATHS.material_type_file_path,
        ]
        if path and not os.path.exists(path)
    ]
    if missing:
        logger.warning("The following fallback files are missing (queries will fail without a live connection):")
        for p in missing:
            logger.warning(f"  MISSING: {p}")
        print()

    # Run — data_source='file' skips connection attempts and loads directly from files
    cache = load_mapping_files(
        file_paths  = FILE_PATHS,
        ref_paths   = REF_PATHS,
        year        = YEAR,
        quarter     = QUARTER,
        data_source = "file",
    )

    # Print summary of every dataframe in the cache
    logger.info("\n--- Cache summary ---")
    _print_df_summary("header_mapping",       cache.header_mapping_df)
    _print_df_summary("item_mapping",         cache.item_mapping_df)
    _print_df_summary("uom_mapping",          cache.uom_mapping_df)
    _print_df_summary("plant_name_mapping",   cache.plant_name_mapping_df)
    _print_df_summary("material_master",      cache.material_master_df)
    _print_df_summary("lot_no_master",        cache.lot_no_master_df)
    _print_df_summary("lot_no_mapping",       cache.lot_no_mapping_df)
    _print_df_summary("material_description", cache.material_description_df)
    _print_df_summary("uom_master",           cache.uom_master_df)
    _print_df_summary("unit_cost",            cache.unit_cost_df)
    _print_df_summary("material_type",        cache.material_type_df)
    _print_df_summary("sap_report",           cache.sap_report_df)

    # Write all non-empty dataframes to outputs/curated/
    logger.info("\n--- Writing outputs ---")
    _write_outputs(cache)

    logger.info("\nDone.")


if __name__ == "__main__":
    main()
