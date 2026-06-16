"""
Local Debug Runner for Processed Layer — Clinical
==================================================
Run this script directly in PyCharm (or any Python interpreter) to test
process_clinical() against local CSV fixtures — no Databricks cluster or
Spark context required.

PyCharm Setup (one-time):
  1. Right-click `3pl_inventory_reconciliation/` → Mark Directory as → Sources Root
  2. Ensure your interpreter has: pandas, openpyxl   (pip install -r requirements.txt)
  3. Press the green Run button, or set breakpoints in
     lib/processed/transformations.py (in process_clinical) and press Debug.

Input fixtures expected in tests/fixtures/processed/
  (export the relevant Delta table partitions as CSVs and place them here)

  curated_clinical.csv         — curated_3pl_inventory filtered to Segment=clinical
  ebs_clinical.csv             — ebs_clinical_inventory filtered to the target Year/Quarter
  header_mapping_clinical.csv  — header_mapping filtered to Segment=clinical
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

from lib.processed.transformations import process_clinical

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_FIXTURES_DIR   = os.path.join(_TESTS_DIR, "fixtures")
_PROCESSED_DIR  = os.path.join(_FIXTURES_DIR, "processed")
_OUTPUTS_DIR    = os.path.join(_TESTS_DIR, "outputs", "processed")


# ===========================================================================
# CONFIG — update year/quarter as needed
# ===========================================================================

YEAR    = "2026"
QUARTER = "Q1"
SEGMENT = "clinical"

CURATED_CSV        = os.path.join(_PROCESSED_DIR, f"curated_{SEGMENT}.csv")
EBS_CSV            = os.path.join(_PROCESSED_DIR, f"ebs_{SEGMENT}.csv")
HEADER_MAPPING_CSV = os.path.join(_PROCESSED_DIR, f"header_mapping_{SEGMENT}.csv")


# ===========================================================================
# Helpers
# ===========================================================================

def _warn_missing_files() -> None:
    required = [
        CURATED_CSV,
        EBS_CSV,
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
    """Break down the result by match type: both sides / curated-only / EBS-only."""
    ebs_only     = df["Gilead_Material_Code"].isna() & df["Item_Number"].notna()
    curated_only = df["Item_Number"].isna() & df["Gilead_Material_Code"].notna()
    matched      = df["Item_Number"].notna() & df["Gilead_Material_Code"].notna()

    logger.info("  Reconciliation breakdown:")
    logger.info(f"    Matched (both sides) : {matched.sum()}")
    logger.info(f"    Curated-only         : {curated_only.sum()}")
    logger.info(f"    EBS-only             : {ebs_only.sum()}")
    logger.info(f"    Total rows           : {len(df)}")
    print()

    if ebs_only.any():
        logger.info("  EBS-only rows (sample):")
        print(df[ebs_only][["Org_Code", "Item_Number", "Lot_Number", "Onhand_Quantity"]].head(10).to_string())
        print()

    if curated_only.any():
        logger.info("  Curated-only rows (sample):")
        print(df[curated_only][["Org_Code", "Gilead_Material_Code", "Gilead_Batch_Number",
                                 "3PL_Material_Code", "3PL_Batch_Number"]].head(10).to_string())
        print()


# ===========================================================================
# Main
# ===========================================================================

def main():
    logger.info("=" * 70)
    logger.info("Processed Layer — local debug runner (CLINICAL)")
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
        ebs_df            = pd.read_csv(EBS_CSV,            dtype=str)
        header_mapping_df = pd.read_csv(HEADER_MAPPING_CSV, dtype=str)
    except FileNotFoundError as e:
        logger.error(f"Cannot load fixture: {e}")
        logger.error("Export the relevant Delta table partitions as CSVs and place them in tests/fixtures/processed/")
        return

    # Mirror the preprocessing applied before Delta write — EBS quantities are
    # stored as DOUBLE in ebs_clinical_inventory. The staging notebook casts
    # them with pd.to_numeric before writing; CSV fixtures load as strings, so
    # we replicate those casts here.
    for c in ("onhand_quantity", "reservation_quantity", "available_quantity"):
        if c in ebs_df.columns:
            ebs_df[c] = pd.to_numeric(ebs_df[c], errors="coerce")

    logger.info(f"  curated       : {curated_df.shape}  "
                f"(Material_Description present: {'Material_Description' in curated_df.columns})")
    logger.info(f"  ebs           : {ebs_df.shape}")
    logger.info(f"  header_mapping: {header_mapping_df.shape}")
    print()

    # ------------------------------------------------------------------
    # Run process_clinical
    # ------------------------------------------------------------------
    logger.info("Running process_clinical()...")
    try:
        result_df = process_clinical(
            curated_df        = curated_df,
            ebs_df            = ebs_df,
            header_mapping_df = header_mapping_df,
            year              = YEAR,
            quarter           = QUARTER,
        )
    except Exception:
        logger.exception("process_clinical() raised an exception")
        return

    logger.info("process_clinical() completed.\n")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    _print_df_summary(f"{SEGMENT}_reconciled", result_df)
    _print_reconciliation_summary(result_df)

    # Validation errors (carried from curated side; EBS-only rows have NaN Has_Error)
    error_rows = result_df[result_df["Has_Error"] == True]
    if not error_rows.empty:
        logger.warning(f"  {len(error_rows)} row(s) with Has_Error=True:")
        print(error_rows[["Org_Code", "Gilead_Material_Code", "Gilead_Batch_Number",
                          "Validation_Remark"]].to_string())
        print()
    else:
        logger.info("  No validation errors found.")

    # Sanity check: Plant_Classification should be filled even on EBS-only rows
    class_missing = result_df["Plant_Classification"].isna().sum()
    logger.info(f"  Plant_Classification NULLs: {class_missing}")

    # ------------------------------------------------------------------
    # Write output
    # ------------------------------------------------------------------
    os.makedirs(_OUTPUTS_DIR, exist_ok=True)
    out_path = os.path.join(_OUTPUTS_DIR, f"{SEGMENT}_reconciled_{YEAR}_{QUARTER}.csv")
    result_df.to_csv(out_path, index=False)
    logger.info(f"\nOutput written to: {out_path}")


if __name__ == "__main__":
    main()
