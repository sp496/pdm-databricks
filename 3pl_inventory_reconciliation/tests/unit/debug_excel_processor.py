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
for _p in [_REPO_ROOT, _PROJECT_ROOT]:
    if _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)

from lib.raw.excel_processor import process_3pl_file, process_sap_file
from lib.raw.mapping_loader import load_quarter_mappings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_FIXTURES_DIR = os.path.join(_TESTS_DIR, "fixtures")
_SAMPLE_DIR   = os.path.join(_FIXTURES_DIR, "sample_csvs")
_OUTPUTS_DIR  = os.path.join(_TESTS_DIR, "outputs")


# ===========================================================================
# LOCAL PATHS — update to match the sample files you've placed in fixtures/
# ===========================================================================

SAMPLE_FILE        = os.path.join(_SAMPLE_DIR, "Accx.xlsx")
SITE_ID            = "1205"
SEGMENT            = "commercial"

MAPPING_FILE_API   = os.path.join(_FIXTURES_DIR, "api_mapping_2026_Q1.xlsx")
MAPPING_FILE_DP    = os.path.join(_FIXTURES_DIR, "dp_mapping_2026_Q1.xlsx")
MAPPING_SHEET_NAME = "Header Mappings"

SAP_FILE           = None  # set to an xlsx path to also test process_sap_file


# ===========================================================================
# Main debug routine
# ===========================================================================

def _write_output(df, site_id, sheet_slug):
    out_dir = os.path.join(_OUTPUTS_DIR, site_id)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{sheet_slug}.csv")
    df.to_csv(out_path, index=False)
    logger.info(f"  Written: {out_path}")


def main():
    logger.info("=" * 70)
    logger.info("3PL Excel Processor — local debug runner")
    logger.info("=" * 70)

    if not os.path.exists(SAMPLE_FILE):
        logger.error(f"Sample file not found: {SAMPLE_FILE}")
        logger.error(f"Place your xlsx in {_SAMPLE_DIR} and update SAMPLE_FILE above.")
        return

    # -----------------------------------------------------------------------
    # Load mappings via load_quarter_mappings (tests mapping_loader too)
    # -----------------------------------------------------------------------
    logger.info("\n--- Loading mappings ---")
    mapping_paths = {SEGMENT: {"api": MAPPING_FILE_API, "dp": MAPPING_FILE_DP}}
    _, column_dict, sheet_dict = load_quarter_mappings(mapping_paths, MAPPING_SHEET_NAME)
    logger.info(f"  Allowed sheets for {SITE_ID}: {sheet_dict.get(SITE_ID, '(all)')}")

    # -----------------------------------------------------------------------
    # Process 3PL inventory file
    # -----------------------------------------------------------------------
    logger.info(f"\n--- process_3pl_file: {SAMPLE_FILE} ---")
    sheets = process_3pl_file(SAMPLE_FILE, SITE_ID, SEGMENT, sheet_dict, column_dict)

    for sheet_slug, data in sheets:
        logger.info(f"  Sheet '{sheet_slug}' → shape {data.shape}")
        logger.info(f"  Columns: {list(data.columns)}")
        print(f"\nFirst 5 rows of '{sheet_slug}':")
        print(data.head(5).to_string())
        _write_output(data, SITE_ID, sheet_slug)

    # -----------------------------------------------------------------------
    # Optionally process SAP report
    # -----------------------------------------------------------------------
    if SAP_FILE:
        if not os.path.exists(SAP_FILE):
            logger.warning(f"SAP file not found, skipping: {SAP_FILE}")
        else:
            logger.info(f"\n--- process_sap_file: {SAP_FILE} ---")
            df = process_sap_file(SAP_FILE)
            logger.info(f"  Shape: {df.shape}")
            print("\nFirst 5 rows of SAP report:")
            print(df.head(5).to_string())
            _write_output(df, "sap", "sap_report")

    logger.info("\nDone.")


if __name__ == "__main__":
    main()
