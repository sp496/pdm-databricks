"""
Local Debug Runner for Curated Processing
==========================================
Run this script directly in PyCharm (or any Python interpreter) to test
curated_processing() against local sample files — no Databricks cluster
or Starburst connection required.

PyCharm Setup (one-time):
  1. Right-click `3pl_inventory_reconciliation/` → Mark Directory as → Sources Root
  2. Ensure your interpreter has: pandas, openpyxl   (pip install -r requirements.txt)
  3. Press the green Run button, or set breakpoints in transformations.py and press Debug.

Input: raw CSV files (output of the raw processing notebook)
---------------------------------------------------------------
Place one CSV per sheet under:

    tests/fixtures/raw/{SEGMENT}/{YEAR}/{QUARTER}/3pl_files/{SITE_ID}/

Example:
    tests/fixtures/raw/commercial/2026/Q1/3pl_files/1205/inventory.csv

The folder structure matters — curated_processing extracts site_id, year,
and quarter directly from the file path.

To generate these CSVs locally, run debug_excel_processor.py first and copy
its outputs from tests/outputs/raw/ into the structure above.

Reference / mapping files (already in tests/fixtures/):
  api_mapping_2026_Q1.xlsx
  dp_mapping_2026_Q1.xlsx
  sap_report.csv

Fallback CSV files expected in tests/fixtures/curated/
  plant_name_mapping.csv    material_master.csv    lot_no_master.csv
  lot_no_mapping.csv        material_description.csv  uom_master.csv
  unit_cost.csv             material_type.csv

  Note: gilead_receipts is intentionally excluded — it is loaded only in
  write_mapping_tables and stored in the sap_report Delta table.
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

import pandas as pd

from lib.curated.data_cache import MappingFilePaths, RefFilePaths, load_mapping_files
from lib.curated.transformations import curated_processing

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_FIXTURES_DIR = os.path.join(_TESTS_DIR, "fixtures")
_CURATED_DIR  = os.path.join(_FIXTURES_DIR, "curated")
_OUTPUTS_DIR  = os.path.join(_TESTS_DIR, "outputs", "curated")

# ===========================================================================
# CONFIG — update to match the fixtures you want to test
# ===========================================================================

YEAR    = "2026"
QUARTER = "Q1"
SEGMENT = "commercial"
SITE_ID = "1205"   # must match folder name AND the 3PL column in the mapping Excel

FILE_PATHS = MappingFilePaths(
    api_mapping_file_path     = os.path.join(_FIXTURES_DIR, "api_mapping_2026_Q1.xlsx"),
    dp_mapping_file_path      = os.path.join(_FIXTURES_DIR, "dp_mapping_2026_Q1.xlsx"),
    header_mapping_sheet_name = "Header Mapping",
    item_mapping_sheet_name   = "Item Mapping",
    uom_mapping_sheet_name    = "UOM Mapping",
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
    # gil_receipts_file_path intentionally omitted — loaded only in write_mapping_tables
)

# Root of the raw fixture tree — CSVs live at:
# {_RAW_SITE_DIR}/{sheet_slug}.csv
_RAW_SITE_DIR = os.path.join(
    _FIXTURES_DIR, "raw", SEGMENT, YEAR, QUARTER, "3pl_files", SITE_ID
)


# ===========================================================================
# Helpers
# ===========================================================================

def _discover_raw_csvs(site_dir: str) -> list[str]:
    """Return all CSV files in site_dir. Mirrors discover_all_raw_csvs logic."""
    if not os.path.isdir(site_dir):
        return []
    return [
        os.path.join(site_dir, f)
        for f in sorted(os.listdir(site_dir))
        if f.endswith(".csv") and not f.startswith("~$")
    ]


def _warn_missing_files() -> None:
    required = [
        FILE_PATHS.api_mapping_file_path,
        FILE_PATHS.dp_mapping_file_path,
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
    missing = [p for p in required if p and not os.path.exists(p)]
    if missing:
        logger.warning("The following files are missing:")
        for p in missing:
            logger.warning(f"  MISSING: {p}")
        print()


def _print_df_summary(name: str, df: pd.DataFrame) -> None:
    logger.info(f"  {name}: {df.shape[0]} rows x {df.shape[1]} cols")
    logger.info(f"  columns: {list(df.columns)}")
    print(df.head(5).to_string())
    print()


# ===========================================================================
# Main
# ===========================================================================

def main():
    logger.info("=" * 70)
    logger.info("Curated Processing — local debug runner")
    logger.info(f"Segment: {SEGMENT}  Year: {YEAR}  Quarter: {QUARTER}  Site: {SITE_ID}")
    logger.info(f"Raw CSV dir: {_RAW_SITE_DIR}")
    logger.info("=" * 70)

    _warn_missing_files()

    # ------------------------------------------------------------------
    # Discover raw CSVs for the configured site
    # ------------------------------------------------------------------
    raw_csvs = _discover_raw_csvs(_RAW_SITE_DIR)
    if not raw_csvs:
        logger.error(
            f"No raw CSV files found under {_RAW_SITE_DIR}\n"
            f"  Place one CSV per sheet there and re-run.\n"
            f"  Tip: run debug_excel_processor.py first, then copy its output CSVs."
        )
        return

    logger.info(f"Found {len(raw_csvs)} CSV file(s) to process:")
    for p in raw_csvs:
        logger.info(f"  {os.path.basename(p)}")
    print()

    # ------------------------------------------------------------------
    # Load mapping cache (data_source='file' — no live connection needed)
    # ------------------------------------------------------------------
    logger.info("Loading mapping cache...")
    cache = load_mapping_files(
        file_paths  = FILE_PATHS,
        ref_paths   = REF_PATHS,
        year        = YEAR,
        quarter     = QUARTER,
        data_source = "file",
    )
    logger.info("Mapping cache loaded.\n")

    logger.info(f"Header mapping built — {len(cache.header_mapping)} key(s): {sorted(cache.header_mapping.keys())}\n")

    # ------------------------------------------------------------------
    # Process each CSV
    # ------------------------------------------------------------------
    os.makedirs(_OUTPUTS_DIR, exist_ok=True)
    errors = []

    for raw_path in raw_csvs:
        file_stem = os.path.splitext(os.path.basename(raw_path))[0]
        out_path  = os.path.join(_OUTPUTS_DIR, f"{SITE_ID}_{file_stem}_curated.csv")

        logger.info(f"{'='*60}")
        logger.info(f"Processing: {os.path.basename(raw_path)}")

        try:
            raw_df     = pd.read_csv(raw_path, dtype=str)
            curated_df = curated_processing(raw_df, raw_path, cache, SEGMENT)
            curated_df.to_csv(out_path, index=False)
            logger.info(f"Output: {out_path}")
            _print_df_summary(file_stem, curated_df)

            error_rows = curated_df[curated_df["Has_Error"] == True]
            if not error_rows.empty:
                logger.warning(f"  {len(error_rows)} row(s) with Has_Error=True:")
                print(error_rows[["3PL_Material_Code", "3PL_Batch_Number", "Validation_Remark"]].to_string())
                print()

        except Exception as e:
            logger.exception(f"FAILED: {os.path.basename(raw_path)}")
            errors.append((os.path.basename(raw_path), str(e)))

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("=" * 70)
    logger.info(f"Done — {len(raw_csvs) - len(errors)}/{len(raw_csvs)} file(s) succeeded")
    if errors:
        logger.error("Failed files:")
        for fname, err in errors:
            logger.error(f"  {fname}: {err}")
    logger.info(f"Outputs written to: {_OUTPUTS_DIR}")


if __name__ == "__main__":
    main()
