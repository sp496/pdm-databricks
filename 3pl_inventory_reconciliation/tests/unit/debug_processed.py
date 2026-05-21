"""
Local Debug Runner for Processed Layer
=======================================
Run this script directly in PyCharm (or any Python interpreter) to test
process_commercial() against local CSV fixtures — no Databricks cluster
or Spark context required.

PyCharm Setup (one-time):
  1. Right-click `3pl_inventory_reconciliation/` → Mark Directory as → Sources Root
  2. Ensure your interpreter has: pandas, openpyxl   (pip install -r requirements.txt)
  3. Press the green Run button, or set breakpoints in
     lib/processed/transformations.py and press Debug.

Input fixtures expected in tests/fixtures/processed/
  (export the relevant Delta table partitions as CSVs and place them here)

  curated_commercial.csv      — curated_3pl_inventory filtered to Segment=commercial
  sap_report_commercial.csv   — sap_report filtered to Segment=commercial
  header_mapping_commercial.csv — header_mapping filtered to Segment=commercial

Additional fallback file (segment-independent):
  tests/fixtures/curated/material_description.csv
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

from lib.curated.transformations import remove_decimal_if_all_zeros
from lib.processed.transformations import process_commercial

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_FIXTURES_DIR   = os.path.join(_TESTS_DIR, "fixtures")
_PROCESSED_DIR  = os.path.join(_FIXTURES_DIR, "processed")
_CURATED_DIR    = os.path.join(_FIXTURES_DIR, "curated")
_OUTPUTS_DIR    = os.path.join(_TESTS_DIR, "outputs", "processed")


# ===========================================================================
# CONFIG — update year/quarter/segment as needed
# ===========================================================================

YEAR    = "2026"
QUARTER = "Q1"
SEGMENT = "commercial"

CURATED_CSV        = os.path.join(_PROCESSED_DIR, f"curated_{SEGMENT}.csv")
SAP_REPORT_CSV     = os.path.join(_PROCESSED_DIR, f"sap_report_{SEGMENT}.csv")
HEADER_MAPPING_CSV = os.path.join(_PROCESSED_DIR, f"header_mapping_{SEGMENT}.csv")


# ===========================================================================
# Helpers
# ===========================================================================

def _warn_missing_files() -> None:
    required = [
        CURATED_CSV,
        SAP_REPORT_CSV,
        HEADER_MAPPING_CSV,
    ]
    missing = [p for p in required if not os.path.exists(p)]
    if missing:
        logger.warning("The following fixture files are missing:")
        for p in missing:
            logger.warning(f"  MISSING: {p}")
        print()


def _print_df_summary(name: str, df: pd.DataFrame) -> None:
    logger.info(f"  {name}: {df.shape[0]} rows x {df.shape[1]} cols")
    logger.info(f"  columns: {list(df.columns)}")
    print(df.head(5).to_string())
    print()


def _print_reconciliation_summary(df: pd.DataFrame) -> None:
    """Break down the result by match type: both sides / curated-only / SAP-only."""
    sap_only     = df["Gilead_Material_Code"].isna() & df["Material_Number"].notna()
    curated_only = df["Material_Number"].isna() & df["Gilead_Material_Code"].notna()
    matched      = df["Material_Number"].notna() & df["Gilead_Material_Code"].notna()

    logger.info("  Reconciliation breakdown:")
    logger.info(f"    Matched (both sides) : {matched.sum()}")
    logger.info(f"    Curated-only         : {curated_only.sum()}")
    logger.info(f"    SAP-only             : {sap_only.sum()}")
    logger.info(f"    Total rows           : {len(df)}")
    print()

    if sap_only.any():
        logger.info("  SAP-only rows (sample):")
        print(df[sap_only][["Plant_Number", "Material_Number", "Batch_Number", "Stock_OH"]].head(10).to_string())
        print()

    if curated_only.any():
        logger.info("  Curated-only rows (sample):")
        print(df[curated_only][["Plant_Number", "Gilead_Material_Code", "Gilead_Batch_Number", "3PL_Quantity"]].head(10).to_string())
        print()


# ===========================================================================
# Main
# ===========================================================================

def main():
    logger.info("=" * 70)
    logger.info("Processed Layer — local debug runner")
    logger.info(f"Segment: {SEGMENT}  Year: {YEAR}  Quarter: {QUARTER}")
    logger.info(f"Fixtures dir: {_PROCESSED_DIR}")
    logger.info("=" * 70)

    _warn_missing_files()

    # ------------------------------------------------------------------
    # Load input fixtures
    # ------------------------------------------------------------------
    logger.info("Loading input fixtures...")
    try:
        curated_df        = pd.read_csv(CURATED_CSV,        dtype=str)
        sap_df            = pd.read_csv(SAP_REPORT_CSV,     dtype=str)
        header_mapping_df = pd.read_csv(HEADER_MAPPING_CSV, dtype=str)
    except FileNotFoundError as e:
        logger.error(f"Cannot load fixture: {e}")
        logger.error("Export the relevant Delta table partitions as CSVs and place them in tests/fixtures/processed/")
        return

    # Mirror the preprocessing applied by write_mapping_tables before Delta write
    # (Delta stores numeric columns as DOUBLE and batch numbers are normalised;
    #  CSV fixtures load everything as string so we replicate those steps here)
    sap_df["Batch_Number"]                  = sap_df["Batch_Number"].apply(remove_decimal_if_all_zeros)
    sap_df["Stock_Quantity__Base_UOM_"]     = pd.to_numeric(sap_df["Stock_Quantity__Base_UOM_"],     errors="coerce")
    sap_df["Group_Valuation_Standard_Cost"] = pd.to_numeric(sap_df["Group_Valuation_Standard_Cost"], errors="coerce")

    logger.info(f"  curated       : {curated_df.shape}  "
                f"(Material_Description present: {'Material_Description' in curated_df.columns})")
    logger.info(f"  sap_report    : {sap_df.shape}  "
                f"(Material_Description present: {'Material_Description' in sap_df.columns})")
    logger.info(f"  header_mapping: {header_mapping_df.shape}")
    print()

    # ------------------------------------------------------------------
    # Run process_commercial
    # ------------------------------------------------------------------
    logger.info("Running process_commercial()...")
    try:
        result_df = process_commercial(
            curated_df        = curated_df,
            sap_df            = sap_df,
            header_mapping_df = header_mapping_df,
            year              = YEAR,
            quarter           = QUARTER,
        )
    except Exception:
        logger.exception("process_commercial() raised an exception")
        return

    logger.info("process_commercial() completed.\n")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    _print_df_summary(f"{SEGMENT}_reconciled", result_df)
    _print_reconciliation_summary(result_df)

    # Validation errors
    error_rows = result_df[result_df["Has_Error"] == True]
    if not error_rows.empty:
        logger.warning(f"  {len(error_rows)} row(s) with Has_Error=True:")
        print(error_rows[["Plant_Number", "Gilead_Material_Code", "Gilead_Batch_Number",
                           "Validation_Remark"]].to_string())
        print()
    else:
        logger.info("  No validation errors found.")

    # ------------------------------------------------------------------
    # Write output
    # ------------------------------------------------------------------
    os.makedirs(_OUTPUTS_DIR, exist_ok=True)
    out_path = os.path.join(_OUTPUTS_DIR, f"{SEGMENT}_reconciled_{YEAR}_{QUARTER}.csv")
    result_df.to_csv(out_path, index=False)
    logger.info(f"\nOutput written to: {out_path}")


if __name__ == "__main__":
    main()
