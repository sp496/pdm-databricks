# TODO: move _FileInfo and _FsMock to a shared tests/utils.py so other debug runners can reuse them
"""
Local Debug Runner for 3PL Discovery
=====================================
Run this script directly in PyCharm (or any Python interpreter) to test and
step through the discovery logic against a local folder structure — no
Databricks cluster or dbutils required.

PyCharm Setup (one-time):
  1. Right-click `3pl_inventory_reconciliation/` → Mark Directory as → Sources Root
  2. Set your Python interpreter (Settings → Project → Python Interpreter)
     and ensure it has: pandas, openpyxl, xlrd   (pip install -r requirements.txt)
  3. Open this file and press the green Run/Debug button.

Fixture layout (mirrors the real S3 structure):
  tests/fixtures/discovery/
  └── 2026/
      └── Q1/
          ├── 3pl_files/
          │   ├── clinical/
          │   │   └── WRTRR1226/   ← drop any inventory xlsx here
          │   └── commercial/
          │       └── 1205/        ← drop any inventory xlsx here
          ├── mapping_files/
          │   ├── clinical/        ← api_mapping_*.xlsx, dp_mapping_*.xlsx
          │   └── commercial/      ← api_mapping_*.xlsx, dp_mapping_*.xlsx
          └── sap_report_files/    ← sap_report_*.xlsx
"""

import os
import sys
import logging
from datetime import date
from pathlib import PurePath

_THIS_DIR     = os.path.dirname(os.path.abspath(__file__))   # .../tests/unit
_TESTS_DIR    = os.path.dirname(_THIS_DIR)                   # .../tests
_PROJECT_ROOT = os.path.dirname(_TESTS_DIR)                  # .../3pl_inventory_reconciliation
_REPO_ROOT    = os.path.dirname(_PROJECT_ROOT)               # .../pdm-databricks
for _p in [_REPO_ROOT, _PROJECT_ROOT]:
    if _p in sys.path:
        sys.path.remove(_p)
    sys.path.insert(0, _p)

from lib.raw.discovery import (
    get_latest_completed_quarter,
    discover_3pl_files,
    discover_mapping_files,
    discover_sap_file,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DISCOVERY_ROOT = os.path.join(_TESTS_DIR, "fixtures", "discovery")
SEGMENTS        = ["clinical", "commercial"]


# ===========================================================================
# Mock dbutils
# ===========================================================================

class _FileInfo:
    """Mimics the object returned by dbutils.fs.ls() on Databricks."""
    def __init__(self, path, is_dir):
        self.path = path.replace("\\", "/") + ("/" if is_dir else "")
        self.modificationTime = int(os.path.getmtime(path) * 1000)


class _FsMock:
    def ls(self, path):
        path = path.replace("/", os.sep)
        if not os.path.isdir(path):
            raise FileNotFoundError(f"ls: no such directory: {path}")
        return [
            _FileInfo(os.path.join(path, name), os.path.isdir(os.path.join(path, name)))
            for name in os.listdir(path)
        ]


dbutils = type("dbutils", (), {"fs": _FsMock()})()


# ===========================================================================
# Main debug routine
# ===========================================================================

def main():
    logger.info("=" * 70)
    logger.info("3PL Discovery — local debug runner")
    logger.info(f"Root: {_DISCOVERY_ROOT}")
    logger.info("=" * 70)

    if not os.path.isdir(_DISCOVERY_ROOT):
        logger.error(f"Fixture root not found: {_DISCOVERY_ROOT}")
        return

    # -----------------------------------------------------------------------
    # 1. Resolve latest completed quarter
    # -----------------------------------------------------------------------
    logger.info("\n--- get_latest_completed_quarter ---")
    year, quarter = get_latest_completed_quarter(dbutils, _DISCOVERY_ROOT, today=date.today())
    logger.info(f"  Result: year={year}, quarter={quarter}")

    if not year or not quarter:
        logger.error("No completed quarter found — check fixture folder names (e.g. 2026/Q1).")
        return

    quarter_root = os.path.join(_DISCOVERY_ROOT, year, quarter)
    logger.info(f"  Quarter root: {quarter_root}")

    # -----------------------------------------------------------------------
    # 2. Discover mapping files
    # -----------------------------------------------------------------------
    logger.info("\n--- discover_mapping_files ---")
    mapping_paths = discover_mapping_files(dbutils, quarter_root, SEGMENTS)
    for segment, paths in mapping_paths.items():
        logger.info(f"  [{segment}]  api={paths.get('api')}  dp={paths.get('dp')}")

    # -----------------------------------------------------------------------
    # 3. Discover 3PL inventory files
    # -----------------------------------------------------------------------
    logger.info("\n--- discover_3pl_files ---")
    files_3pl = discover_3pl_files(dbutils, quarter_root, SEGMENTS)
    logger.info(f"  Found {len(files_3pl)} file(s):")
    for entry in files_3pl:
        logger.info(f"  [{entry['segment']}] {entry['site_id']} → {entry['path']}")

    # -----------------------------------------------------------------------
    # 4. Discover SAP report
    # -----------------------------------------------------------------------
    logger.info("\n--- discover_sap_file ---")
    sap_path = discover_sap_file(dbutils, quarter_root)
    logger.info(f"  SAP file: {sap_path}")

    logger.info("\nDone.")


if __name__ == "__main__":
    main()
