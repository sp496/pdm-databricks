"""
Local Debug Runner for 3PL Excel Processing
=============================================
Run this script directly in PyCharm (or any Python interpreter) to test and
step through the Excel cleaning logic against a real sample file — no
Databricks cluster or dbutils required.

PyCharm Setup (one-time):
  1. Right-click `3pl_inventory_reconciliation/` → Mark Directory as → Sources Root
  2. Set your Python interpreter (Settings → Project → Python Interpreter)
     and ensure it has: pandas, openpyxl, xlrd   (pip install -r requirements.txt)
  3. Open this file and press the green Run/Debug button, or set a breakpoint
     anywhere in excel_utils.py / excel_processor.py and press the Debug button.

Local files:
  Sample inventory xlsx  → tests/fixtures/sample_csvs/
  Sample mapping xlsx    → tests/fixtures/
  Processed outputs      → tests/outputs/   (gitignored, created on first run)
"""

import os
import sys
import logging

_THIS_DIR     = os.path.dirname(os.path.abspath(__file__))   # .../tests/unit
_TESTS_DIR    = os.path.dirname(_THIS_DIR)                   # .../tests
_PROJECT_ROOT = os.path.dirname(_TESTS_DIR)                  # .../3pl_inventory_reconciliation
_REPO_ROOT    = os.path.dirname(_PROJECT_ROOT)               # .../pdm-databricks
for _p in [_REPO_ROOT, _PROJECT_ROOT]:  # PROJECT_ROOT inserted last → ends up at position 0
    if _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)

import pandas as pd
from lib.raw import excel_utils as eu

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_FIXTURES_DIR   = os.path.join(_TESTS_DIR, "fixtures")
_SAMPLE_DIR     = os.path.join(_FIXTURES_DIR, "sample_csvs")
_OUTPUTS_DIR    = os.path.join(_TESTS_DIR, "outputs")


# ===========================================================================
# LOCAL PATHS — update to match the sample file you've placed in fixtures/
# ===========================================================================

# Drop a sample inventory xlsx into tests/fixtures/sample_csvs/ and point here.
SAMPLE_FILE  = os.path.join(_SAMPLE_DIR, "Accx.xlsx")

# Site id (used to look up sheet/column mappings — mirror the folder name)
SITE_ID      = "1205"
SEGMENT      = "commercial"   # or "commercial"

# If you have a local mapping file, point to it and set MAPPING_SHEET_NAME.
# Set to None to skip mapping-based sheet filtering and process all sheets.
MAPPING_FILE       = os.path.join(_FIXTURES_DIR, "api_mapping_2026_Q1.xlsx")   # e.g. os.path.join(_FIXTURES_DIR, "api_mapping_2025_Q1.xlsx")
MAPPING_SHEET_NAME = "Header Mappings"


# ===========================================================================
# Helpers
# ===========================================================================

def _load_mapping(file_path, sheet_name):
    if not file_path or not os.path.exists(file_path):
        logger.warning(f"Mapping file not found, all sheets will be processed: {file_path}")
        return {}, {}
    df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
    cmo_column_dict = df.groupby("3PL")["3PL Column Header"].apply(list).to_dict()
    df["Sheet Name"] = df.groupby("3PL")["Sheet Name"].ffill().str.lower()
    df["Sheet Name"] = df["Sheet Name"].replace("nan", None)
    cmo_sheet_dict = (
        df.groupby("3PL")["Sheet Name"]
        .apply(lambda x: sorted({s.strip() for name in x.dropna() for s in name.split(",")}))
        .to_dict()
    )
    return cmo_column_dict, cmo_sheet_dict


def _write_output(df, site_id, sheet_slug):
    out_dir = os.path.join(_OUTPUTS_DIR, site_id)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{sheet_slug}.csv")
    df.to_csv(out_path, index=False)
    logger.info(f"  Written: {out_path}")


# ===========================================================================
# Main debug routine
# ===========================================================================

def main():
    logger.info("=" * 70)
    logger.info("3PL Excel Processor — local debug runner")
    logger.info("=" * 70)

    if not os.path.exists(SAMPLE_FILE):
        logger.error(f"Sample file not found: {SAMPLE_FILE}")
        logger.error(f"Place your xlsx in {_SAMPLE_DIR} and update SAMPLE_FILE above.")
        return

    logger.info(f"File    : {SAMPLE_FILE}")
    logger.info(f"Site ID : {SITE_ID}")
    logger.info(f"Segment : {SEGMENT}")

    cmo_column_dict, cmo_sheet_dict = _load_mapping(MAPPING_FILE, MAPPING_SHEET_NAME)
    allowed_sheets = cmo_sheet_dict.get(SITE_ID, [])
    logger.info(f"Allowed sheets from mapping: {allowed_sheets or '(all)'}")

    xls = pd.ExcelFile(SAMPLE_FILE)
    is_single_sheet = len(xls.sheet_names) == 1
    logger.info(f"Sheets in workbook: {xls.sheet_names}")

    for sheet in xls.sheet_names:
        sheet_lc = sheet.strip().lower()
        if not ((not allowed_sheets and is_single_sheet) or sheet_lc in allowed_sheets):
            logger.info(f"\n[{sheet}] Skipped (not in mapping)")
            continue

        logger.info(f"\n--- Processing sheet: '{sheet}' ---")
        df_raw = pd.read_excel(xls, sheet_name=sheet, header=None)
        logger.info(f"  Raw shape: {df_raw.shape}")

        # ----------------------------------------------------------------
        # Step through each cleaning stage — comment out any that aren't
        # ported yet and re-run to isolate behaviour.
        # ----------------------------------------------------------------
        boundaries = eu.find_table_boundaries(df_raw, cmo_column_dict.get(SITE_ID, []))
        if boundaries:
            data = boundaries["data"]
            header = boundaries.get("header")
            if header is not None:
                data.columns = header
            logger.info(f"  Table found — shape after boundary detection: {data.shape}")
        else:
            logger.warning(f"  No table boundaries found, using raw data")
            data = df_raw

        data = eu.remove_rows_with_n_values(data)
        data = eu.remove_aggregate_rows(data)
        data = eu.remove_special_characters(data)

        data["3pl"] = SITE_ID
        data["segment"] = SEGMENT

        logger.info(f"  Final shape: {data.shape}")
        logger.info(f"  Columns: {list(data.columns)}")
        print(f"\nFirst 5 rows of '{sheet}':")
        print(data.head(5).to_string())

        sheet_slug = sheet.strip().replace(" ", "_") or "sheet"
        _write_output(data, SITE_ID, sheet_slug)

    logger.info("\nDone.")


if __name__ == "__main__":
    main()
