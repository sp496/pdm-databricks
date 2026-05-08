"""Discovery utilities for traversing the CHAOS 3PL inventory landing structure.

Source layout:
    {root}/{year}/{quarter}/3pl_files/{segment}/{site_id}/{site_id}_inventory.xlsx
    {root}/{year}/{quarter}/mapping_files/{segment}/{api|dp}_mapping_{year}_{quarter}.xlsx
    {root}/{year}/{quarter}/sap_report_files/sap_report_{quarter}_{year}.xlsx
"""
import os
import re
from datetime import date, datetime
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


def _list_dir(dbutils, path):
    return dbutils.fs.ls(path)


def get_latest_completed_quarter(dbutils, root_directory, today=None):
    """Return (year_str, quarter_str) for the most recent fully-completed quarter under root.

    Years are 4-digit folders; quarters are Q1..Q4.
    """
    year_dirs = sorted(
        [
            int(PurePath(d.path).name)
            for d in _list_dir(dbutils, root_directory)
            if PurePath(d.path).name.isdigit() and len(PurePath(d.path).name) == 4
        ],
        reverse=True,
    )
    if not year_dirs:
        return None, None

    for year in year_dirs:
        year_path = os.path.join(root_directory, str(year))
        quarter_dirs = sorted(
            [
                PurePath(d.path).name
                for d in _list_dir(dbutils, year_path)
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
    for entry in _list_dir(dbutils, folder_path):
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


def discover_3pl_files(dbutils, quarter_root, segments):
    """Walk 3pl_files/{segment}/{site_id}/ and return latest file per (segment, site_id).

    Returns a list of dicts: {"segment", "site_id", "path"}.
    """
    results = []
    base = os.path.join(quarter_root, "3pl_files")
    for segment in segments:
        seg_path = os.path.join(base, segment)
        try:
            site_dirs = [d for d in _list_dir(dbutils, seg_path) if d.path.endswith("/")]
        except Exception as e:
            print(f"  No data for segment '{segment}' at {seg_path}: {e}")
            continue
        for site in site_dirs:
            site_id = PurePath(site.path).name
            latest = latest_file_in_dir(dbutils, site.path)
            if latest is None:
                print(f"  No files found for {segment}/{site_id}")
                continue
            results.append({"segment": segment, "site_id": site_id, "path": latest})
    return results


def discover_mapping_files(dbutils, quarter_root, segments):
    """Return {segment: {"api": path, "dp": path}} for the quarter's mapping files."""
    out = {}
    base = os.path.join(quarter_root, "mapping_files")
    for segment in segments:
        seg_path = os.path.join(base, segment)
        seg_map = {"api": None, "dp": None}
        try:
            entries = _list_dir(dbutils, seg_path)
        except Exception as e:
            print(f"  No mapping files for segment '{segment}' at {seg_path}: {e}")
            out[segment] = seg_map
            continue
        for entry in entries:
            name = os.path.basename(entry.path).lower()
            if name.startswith("api_mapping"):
                seg_map["api"] = entry.path
            elif name.startswith("dp_mapping"):
                seg_map["dp"] = entry.path
        out[segment] = seg_map
    return out


def discover_sap_file(dbutils, quarter_root):
    """Return path to the latest SAP report file for the quarter, or None."""
    sap_dir = os.path.join(quarter_root, "sap_report_files")
    try:
        return latest_file_in_dir(dbutils, sap_dir)
    except Exception as e:
        print(f"  No SAP report found at {sap_dir}: {e}")
        return None
