"""Discovery utilities for traversing the 3PL inventory landing structure.

Source layout (segment-first):
    {root}/{segment}/{year}/{quarter}/3pl_files/{site_id}/{site_id}_inventory.xlsx
    {root}/{segment}/{year}/{quarter}/mapping_files/3PL_{segment}_mapping_{year}_{quarter}.xlsx
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


def get_latest_available_quarter(dbutils, segment_root):
    """Return (year_str, quarter_str) for the most recent quarter folder present
    under segment_root, regardless of whether the quarter has completed.

    Mirrors get_latest_completed_quarter but skips the is_quarter_complete check —
    use this to target the newest quarter that exists even if it is still in
    progress. Years are 4-digit folders; quarters are Q1..Q4.
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
        if quarter_dirs:
            return str(year), quarter_dirs[0]
    return None, None


def get_segment_run_config(cfg, segment):
    """Resolve (run_mode, year, quarter) for one segment from the per-segment
    run_config block.

    run_config is keyed by segment:
        {"commercial": {"run_mode": "latest_completed", "year": null, "quarter": null},
         "clinical":   {"run_mode": "historical", "year": "2026", "quarter": "Q1"}}

    A legacy flat {"run_mode": ...} object (no segment keys) is accepted for
    backward compatibility and applies to every segment. Raises if the segment is
    missing, or if historical mode lacks year/quarter.
    """
    rc_all = cfg["run_config"]
    rc = rc_all if "run_mode" in rc_all else rc_all.get(segment)  # flat = legacy
    if rc is None:
        raise KeyError(
            f"run_config has no entry for segment '{segment}' (have: {sorted(rc_all)})"
        )
    run_mode = rc["run_mode"]
    year, quarter = rc.get("year"), rc.get("quarter")
    if run_mode == "historical" and (not year or not quarter):
        raise ValueError(
            f"run_mode is 'historical' for segment '{segment}' but year/quarter not set"
        )
    return run_mode, year, quarter


def resolve_quarter_from_source(cfg, segment, dbutils, segment_src_root):
    """Resolve (run_mode, year, quarter) for a segment, reading the source folders
    for the non-historical modes.

    Modes (per-segment run_config):
      historical       -> use the configured year/quarter
      latest_completed -> most recent calendar-complete quarter present
      latest_available -> most recent quarter folder present (may be in progress)

    For the latest_* modes year/quarter may come back None when no folder is
    found; the caller keeps its own not-found handling.
    """
    run_mode, year, quarter = get_segment_run_config(cfg, segment)
    if run_mode == "historical":
        return run_mode, year, quarter
    if run_mode == "latest_completed":
        year, quarter = get_latest_completed_quarter(dbutils, segment_src_root)
    elif run_mode == "latest_available":
        year, quarter = get_latest_available_quarter(dbutils, segment_src_root)
    else:
        raise ValueError(
            f"Unknown run_mode '{run_mode}' for segment '{segment}' "
            f"(expected historical | latest_completed | latest_available)"
        )
    return run_mode, year, quarter


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


def discover_mapping_file(dbutils, quarter_root, segment=None):
    """Return the path to the per-quarter mapping workbook, or None.

    Walks `{quarter_root}/mapping_files/` and returns the first Excel workbook
    whose basename contains `mapping` (case-insensitive), skipping Excel lock
    files (~$...). Workbooks are named `3PL_{segment}_mapping_{year}_{quarter}.xlsx`;
    when `segment` is supplied it must also appear in the basename, which
    disambiguates if both segments' files ever land in the same directory
    (`clinical` is not a substring of `commercial`, so they never cross-match).
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
        lname = name.lower()
        if not lname.endswith((".xlsx", ".xls")) or "mapping" not in lname:
            continue
        if segment and segment.lower() not in lname:
            continue
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
