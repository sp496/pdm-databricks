"""
Local Debug Runner for DataCurator
====================================
Run this script directly in PyCharm to test and step through data_curator.py
without a Databricks cluster.

PyCharm Setup (one-time):
  1. Right-click `clinical_inventory_optimization/` → Mark Directory as → Sources Root
  2. Set your Python interpreter (Settings → Project → Python Interpreter)
     and make sure it has: pandas, numpy, openpyxl   (pip install -r requirements.txt)
  3. Open this file and press the green Run/Debug button, or set a breakpoint
     anywhere in data_curator.py and press the Debug button.

Local files:
  Mapping Excel files  → tests/fixtures/
  Sample CSV files     → tests/fixtures/sample_csvs/
  Processed outputs    → tests/outputs/<file_type>/   (gitignored, created on first run)
"""

import sys
import os

# ---------------------------------------------------------------------------
# Path setup — makes `from lib.curated.data_curator import ...` work whether
# you run from the repo root or from inside tests/unit/.
# ---------------------------------------------------------------------------
_THIS_DIR     = os.path.dirname(os.path.abspath(__file__))        # .../tests/unit
_TESTS_DIR    = os.path.dirname(_THIS_DIR)                        # .../tests
_PROJECT_ROOT = os.path.dirname(_TESTS_DIR)                       # .../clinical_inventory_optimization
_REPO_ROOT    = os.path.dirname(_PROJECT_ROOT)                    # .../pdm-databricks
for _p in [_PROJECT_ROOT, _REPO_ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Imports (no Databricks / pyspark required)
# ---------------------------------------------------------------------------
import logging
from lib.curated.data_curator import DataCurator, load_excel_mapping, read_dynamic_csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Resolved directory paths
_FIXTURES_DIR   = os.path.join(_TESTS_DIR, "fixtures")
_SAMPLE_CSV_DIR = os.path.join(_FIXTURES_DIR, "sample_csvs")
_OUTPUTS_DIR    = os.path.join(_TESTS_DIR, "outputs")


# ===========================================================================
# LOCAL PATHS — update filenames to match the files you've placed in fixtures/
# ===========================================================================

# Excel mapping files — place in tests/fixtures/ and update filenames below.
# Set to None if you don't have the file locally; standardization will be skipped.
LOCAL_MAPPING = {
    "subject": r"../fixtures/Subject Summary Header Mapping.xlsx",
    "depot":   None,
    "site":    None,
    "slsm":    None,   # no mapping file for supply-method files
    "clsm":    None,
}

# Sample CSV files — place in tests/fixtures/sample_csvs/ and update filenames below.
# Set to None to skip that file type.
LOCAL_CSV = {
    "subject": r"../fixtures/sample_csvs/Gilead GS-US-409-5704_Subject Summary (Unblinded)Subject Summary2026-04-14-18-57-28.csv",
    "depot":   None,
    "site":    None,
    "slsm":    None,
    "clsm":    None,
}

# Subject-visit type uses three input CSVs. Set any value to None to skip.
LOCAL_SUBJECT_VISIT_CSV = {
    "visit_summary":   r"../fixtures/sample_csvs/EDGE-Lung_SubjectVisitSummary.csv",
    "subject_summary": r"../fixtures/sample_csvs/EDGE-Lung_SubjectSummary.csv",
    "site_depot_map":  r"../fixtures/sample_csvs/Signant-Suvoda Headers.csv",
}

# Date folder string — the extract date stamped on the source files
DATE_FOLDER = "20251110"


# ===========================================================================
# Column mapping config (mirrors MAPPING_CONFIG in curate_study_data.py,
# minus the Spark schema which isn't needed locally)
# ===========================================================================

COLUMN_MAPPING = {
    "subject": {
        "Study Protocol":                          "study_protocol",
        "Site ID":                                 "site_id",
        "Country":                                 "country",
        "Parent Depot":                            "parent_depot",
        "Investigator":                            "investigator",
        "Subject Number":                          "subject_number",
        "Year of Birth":                           "year_of_birth",
        "Gender":                                  "gender",
        "TPC":                                     "tpc",
        "Date Randomized":                         "date_randomized",
        "Date Treatment Discontinued":             "date_treatment_discontinued",
        "Date Crossover Enrolled":                 "date_crossover_enrolled",
        "Date Crossover Approved":                 "date_crossover_approved",
        "Date Crossover Treatment Discontinued":   "date_crossover_treatment_discontinued",
        "Subject Status":                          "subject_status",
        "Randomized Treatment":                    "randomized_treatment",
        "Last Study Visit Recorded":               "last_study_visit_recorded",
        "Last Study Visit Date":                   "last_study_visit_date",
        "Last Study Visit Number":                 "last_study_visit_number",
        "Next Min. Study Visit Date":              "next_min_study_visit_date",
        "Next Max. Study Visit Date":              "next_max_study_visit_date",
        "Additional Drug Status":                  "additional_drug_status",
        "Last Additional Drug Visit Recorded":     "last_additional_drug_visit_recorded",
        "Last Additional Drug Visit Date":         "last_additional_drug_visit_date",
        "Last Additional Drug Visit Number":       "last_additional_drug_visit_number",
        "Next Min. Additional Drug Visit Date":    "next_min_additional_drug_visit_date",
        "Next Max. Additional Drug Visit Date":    "next_max_additional_drug_visit_date",
    },
    "depot": {
        "Study Protocol":                               "study_protocol",
        "Depot ID":                                     "depot_id",
        "Country":                                      "country",
        "Depot Type":                                   "depot_type",
        "Study Drug Type":                              "study_drug_type",
        "Unblinded Study Drug Name":                    "unblinded_study_drug_name",
        "Britestock Lot Number":                        "britestock_lot_number",
        "Finished Lot Number":                          "finished_lot_number",
        "Part Number":                                  "part_number",
        "FP Expiry Date":                               "fp_expiry_date",
        "Quantity Study Drug - Requested":              "quantity_study_drug_requested",
        "Quantity Study Drug - Available":              "quantity_study_drug_available",
        "Quantity Study Drug - Lost":                   "quantity_study_drug_lost",
        "Quantity Study Drug - Damaged":                "quantity_study_drug_damaged",
        "Quantity Study Drug - Quarantined":            "quantity_study_drug_quarantined",
        "Quantity Study Drug - Rejected":               "quantity_study_drug_rejected",
        "Quantity Study Drug - Do Not Ship":            "quantity_study_drug_do_not_ship",
        "Quantity Study Drug - Expired":                "quantity_study_drug_expired",
        "Quantity Study Drug - Packaged (Unavailable)": "quantity_study_drug_packaged_unavailable",
        "Quantity Study Drug - Total":                  "quantity_study_drug_total",
        "Approved Countries":                           "approved_countries",
    },
    "site": {
        "Study Protocol":                          "study_protocol",
        "Site ID":                                 "site_id",
        "Country":                                 "country",
        "Investigator":                            "investigator",
        "Location":                                "location",
        "Parent Depot":                            "parent_depot",
        "Site Status":                             "site_status",
        "Study Drug Type":                         "study_drug_type",
        "Unblinded Study Drug Name":               "unblinded_study_drug_name",
        "Britestock Lot Number":                   "britestock_lot_number",
        "Finished Lot Number":                     "finished_lot_number",
        "Part Number":                             "part_number",
        "FP Expiry Date":                          "fp_expiry_date",
        "Quantity Study Drug - Requested":         "quantity_study_drug_requested",
        "Quantity Study Drug - Available":         "quantity_study_drug_available",
        "Quantity Study Drug - Assigned":          "quantity_study_drug_assigned",
        "Quantity Study Drug - Lost":              "quantity_study_drug_lost",
        "Quantity Study Drug - Damaged":           "quantity_study_drug_damaged",
        "Quantity Study Drug - Quarantined":       "quantity_study_drug_quarantined",
        "Quantity Study Drug - Rejected":          "quantity_study_drug_rejected",
        "Quantity Study Drug - Do Not Dispense":   "quantity_study_drug_do_not_dispense",
        "Quantity Study Drug - Expired":           "quantity_study_drug_expired",
        "Quantity Study Drug - Total":             "quantity_study_drug_total",
    },
    "slsm": {
        "Study Protocol":           "study_protocol",
        "Country":                  "country",
        "Site ID":                  "site_id",
        "Comparator Name":          "comparator_name",
        "Site Level Supply Method": "site_level_supply_method",
        "Site Status":              "site_status",
    },
    "clsm": {
        "Study Protocol":              "study_protocol",
        "Country":                     "country",
        "Comparator Name":             "comparator_name",
        "Country Level Supply Method": "country_level_supply_method",
    },
}

DATE_COLUMNS = {
    "subject": [
        "date_randomized", "date_treatment_discontinued", "date_crossover_enrolled",
        "date_crossover_approved", "date_crossover_treatment_discontinued",
        "last_study_visit_date", "next_min_study_visit_date", "next_max_study_visit_date",
        "last_additional_drug_visit_date", "next_min_additional_drug_visit_date",
        "next_max_additional_drug_visit_date",
    ],
    "depot": ["fp_expiry_date"],
    "site":  ["fp_expiry_date"],
    "slsm":  [],
    "clsm":  [],
}

SUBJECT_VISIT_DROP_COLUMNS = [
    'Finished Lot', 'Expiration Date', 'Manufacturing Lot', 'PCI Item Number Lot',
    'Drug Types Temporarily Held', 'Drug Types Permanently Discontinued', 'Assigned Drugs',
    'Quemliclustat Dose Level', 'Drug Code', 'Quantity Dispensed', 'Gilead Site Number',
]


# ===========================================================================
# Helper — load a mapping Excel file, returning None gracefully if missing
# ===========================================================================

def _load_mapping(file_type: str):
    path = LOCAL_MAPPING.get(file_type)
    if not path:
        return None
    if not os.path.exists(path):
        logger.warning(f"Mapping file not found (skipping standardization for {file_type}): {path}")
        return None
    return load_excel_mapping(path)


def _write_output(result_df, file_type: str):
    """Write processed DataFrame to tests/outputs/<file_type>/result_<DATE_FOLDER>.csv"""
    out_dir = os.path.join(_OUTPUTS_DIR, file_type)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"result_{DATE_FOLDER}.csv")
    result_df.to_csv(out_path, index=False)
    logger.info(f"  Output written: {out_path}")


# ===========================================================================
# Main debug routine
# ===========================================================================

def main():
    logger.info("=" * 70)
    logger.info("DataCurator local debug runner")
    logger.info("=" * 70)

    # ------------------------------------------------------------------
    # 1. Load mapping Excel files
    # ------------------------------------------------------------------
    logger.info("\n--- Loading mapping files ---")
    mapping = {ft: _load_mapping(ft) for ft in ("subject", "depot", "site", "slsm", "clsm")}
    for ft, df in mapping.items():
        status = f"{len(df)} rows" if df is not None else "not loaded"
        logger.info(f"  {ft:8s} mapping: {status}")

    # ------------------------------------------------------------------
    # 2. Initialise DataCurator
    # ------------------------------------------------------------------
    curator = DataCurator(
        subject_mapping_df=mapping["subject"],
        depot_mapping_df=mapping["depot"],
        site_mapping_df=mapping["site"],
        slsm_mapping_df=mapping["slsm"],
        clsm_mapping_df=mapping["clsm"],
    )

    # ------------------------------------------------------------------
    # 3. Process each configured CSV file
    # ------------------------------------------------------------------
    results = {}

    for file_type, csv_path in LOCAL_CSV.items():
        if not csv_path:
            logger.info(f"\n[{file_type}] No CSV configured — skipping")
            continue

        if not os.path.exists(csv_path):
            logger.warning(f"\n[{file_type}] CSV not found — skipping: {csv_path}")
            continue

        logger.info(f"\n--- Processing {file_type} ---")
        logger.info(f"  CSV  : {csv_path}")
        logger.info(f"  Date : {DATE_FOLDER}")

        result_df = curator.process_data_from_file(
            file_path=csv_path,
            file_type=file_type,
            date_folder=DATE_FOLDER,
            table_column_mapping=COLUMN_MAPPING[file_type],
            date_columns=DATE_COLUMNS[file_type],
        )

        if result_df is not None:
            results[file_type] = result_df
            logger.info(f"  Result: {result_df.shape[0]} rows x {result_df.shape[1]} cols")
            logger.info(f"  Columns: {list(result_df.columns)}")
            print(f"\n[{file_type}] First 3 rows:")
            print(result_df.head(3).to_string())
            _write_output(result_df, file_type)
        else:
            logger.error(f"  Processing failed for {file_type}")

    # ------------------------------------------------------------------
    # 4. Assemble subject data from the 3-file source, then process it
    #    through the regular 'subject' pipeline.
    # ------------------------------------------------------------------
    sv_paths = LOCAL_SUBJECT_VISIT_CSV
    if not all(sv_paths.values()):
        logger.info("\n[subject_visit] Not all CSVs configured — skipping")
    else:
        missing = [p for p in sv_paths.values() if not os.path.exists(p)]
        if missing:
            logger.warning(f"\n[subject_visit] CSV(s) not found — skipping: {missing}")
        else:
            logger.info("\n--- Assembling subject data from 3-file source ---")
            for label, path in sv_paths.items():
                logger.info(f"  {label:16s}: {path}")
            logger.info(f"  Date             : {DATE_FOLDER}")

            visit_df      = read_dynamic_csv(sv_paths["visit_summary"])
            subject_df    = read_dynamic_csv(sv_paths["subject_summary"])
            site_depot_df = read_dynamic_csv(sv_paths["site_depot_map"])

            assembled_df = curator.assemble_subject_visit_data(
                visit_df, subject_df, site_depot_df,
                drop_columns=SUBJECT_VISIT_DROP_COLUMNS,
            )

            # Study protocol comes from the data, not the filename
            study_protocol = assembled_df['Study Protocol'].dropna().iloc[0]
            assembled_df = assembled_df.drop(columns=['Study Protocol'])

            source_file = os.path.basename(sv_paths["visit_summary"])
            result_df = curator.process_data(
                assembled_df,
                file_type='subject',
                filename=source_file,
                date_folder=DATE_FOLDER,
                table_column_mapping=COLUMN_MAPPING["subject"],
                date_columns=DATE_COLUMNS["subject"],
                study_protocol=study_protocol,
            )

            if result_df is not None:
                results["subject_visit"] = result_df
                logger.info(f"  Result: {result_df.shape[0]} rows x {result_df.shape[1]} cols")
                logger.info(f"  Columns: {list(result_df.columns)}")
                print(f"\n[subject_visit] First 3 rows:")
                print(result_df.head(3).to_string())
                _write_output(result_df, "subject_visit")
            else:
                logger.error("  Processing failed for subject_visit")

    # ------------------------------------------------------------------
    # 5. Summary
    # ------------------------------------------------------------------
    logger.info("\n" + "=" * 70)
    logger.info("Summary")
    logger.info("=" * 70)
    for file_type in list(LOCAL_CSV) + ["subject_visit"]:
        if file_type in results:
            logger.info(f"  {file_type:16s}: {results[file_type].shape[0]} rows processed OK")
        else:
            logger.info(f"  {file_type:16s}: skipped or failed")

    return results


if __name__ == "__main__":
    main()
