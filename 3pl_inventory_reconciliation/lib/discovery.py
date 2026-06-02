"""Discovery utilities for traversing the 3PL inventory landing structure.

Source layout (segment-first):
    {root}/{segment}/{year}/{quarter}/3pl_files/{site_id}/{site_id}_inventory.xlsx
    {root}/{segment}/{year}/{quarter}/mapping_files/{api|dp}_mapping_{year}_{quarter}.xlsx
    {root}/{segment}/{year}/{quarter}/sap_report_files/sap_report_{quarter}_{year}.xlsx
"""
import os
import re
from datetime import date
from pathlib import PurePath


_QUARTER_END = {
    1: (3, 31),
    2: (6, 30),
    3: (9, 30),
    4: (12, 31),
}


def is_quarter_complete(year, quarter, today=None):
    """Return True if the given quarter has fully ended as of `today`."""
    if today is None:
        today = date.today()
    month, day = _QUARTER_END[quarter]
    return today > date(year, month, day)


def get_latest_completed_quarter(dbutils, segment_root, today=None):
    """Return (year_str, quarter_str) for the most recent fully-completed quarter under segment_root.

    Years are 4-digit folders; quarters are Q1..Q4.
    """
    year_dirs = sorted(
        [
            int(PurePath(d.path).name)
            for d in dbutils.fs.ls(segment_root)
            if PurePath(d.path).name.isdigit() and len(PurePath(d.path).name) == 4
        ],
        reverse=True,
    )
    if not year_dirs:
        return None, None

    for year in year_dirs:
        year_path = os.path.join(segment_root, str(year))
        quarter_dirs = sorted(
            [
                PurePath(d.path).name
                for d in dbutils.fs.ls(year_path)
                if re.fullmatch(r"Q[1-4]", PurePath(d.path).name)
            ],
            key=lambda q: int(q[1:]),
            reverse=True,
        )
        for q in quarter_dirs:
            if is_quarter_complete(year, int(q[1:]), today):
                return str(year), q
    return None, None


def latest_file_in_dir(dbutils, folder_path):
    """Return path of the file with the latest modificationTime in folder_path.

    Skips directories and Excel lock files (~$...). Returns None if no files.
    """
    latest_path = None
    latest_mtime = None
    for entry in dbutils.fs.ls(folder_path):
        if entry.path.endswith("/"):
            continue
        name = os.path.basename(entry.path)
        if name.startswith("~$"):
            continue
        mtime = entry.modificationTime
        if latest_mtime is None or mtime > latest_mtime:
            latest_mtime = mtime
            latest_path = entry.path
    return latest_path


def discover_3pl_files(dbutils, quarter_root):
    """Walk 3pl_files/{site_id}/ and return latest file per site_id.

    Returns a dict: {site_id: path}.
    """
    results = {}
    base = os.path.join(quarter_root, "3pl_files")
    try:
        site_dirs = [d for d in dbutils.fs.ls(base) if d.path.endswith("/")]
    except Exception as e:
        print(f"  No 3PL files found at {base}: {e}")
        return results
    for site in site_dirs:
        site_id = PurePath(site.path).name
        latest = latest_file_in_dir(dbutils, site.path)
        if latest is None:
            print(f"  No files found for {site_id}")
            continue
        results[site_id] = latest
    return results


def discover_mapping_file(dbutils, quarter_root):
    """Return the path to the per-quarter mapping workbook, or None.

    Walks `{quarter_root}/mapping_files/` and returns the first file whose
    basename starts with `mapping_` (case-insensitive), skipping Excel lock
    files (~$...). The legacy split into `api_mapping_*.xlsx` /
    `dp_mapping_*.xlsx` is no longer supported — both segments now ship a
    single per-quarter file named `mapping_{year}_{quarter}.xlsx`.
    """
    mapping_dir = os.path.join(quarter_root, "mapping_files")
    try:
        entries = dbutils.fs.ls(mapping_dir)
    except Exception as e:
        print(f"  No mapping files at {mapping_dir}: {e}")
        return None
    for entry in entries:
        name = os.path.basename(entry.path)
        if name.startswith("~$"):
            continue
        if name.lower().startswith("mapping_"):
            return entry.path
    return None


def discover_all_raw_csvs(dbutils, quarter_root):
    """Walk 3pl_files/{site_id}/ and return all CSV files per site_id.

    Unlike discover_3pl_files (which returns only the latest file), this returns
    every CSV in each site folder — used by the curated layer which may need to
    process multiple sheets written by the raw layer.

    Returns a dict: {site_id: [path, ...]}.
    """
    results = {}
    base = os.path.join(quarter_root, "3pl_files")
    try:
        site_dirs = [d for d in dbutils.fs.ls(base) if d.path.endswith("/")]
    except Exception as e:
        print(f"  No 3PL files found at {base}: {e}")
        return results
    for site in site_dirs:
        site_id = PurePath(site.path).name
        files = [
            e.path for e in dbutils.fs.ls(site.path)
            if not e.path.endswith("/") and not os.path.basename(e.path).startswith("~$")
        ]
        if files:
            results[site_id] = files
        else:
            print(f"  No files found for {site_id}")
    return results


def discover_sap_file(dbutils, quarter_root):
    """Return path to the latest SAP report file for the quarter, or None."""
    sap_dir = os.path.join(quarter_root, "sap_report_files")
    try:
        return latest_file_in_dir(dbutils, sap_dir)
    except Exception as e:
        print(f"  No SAP report found at {sap_dir}: {e}")
        return None
