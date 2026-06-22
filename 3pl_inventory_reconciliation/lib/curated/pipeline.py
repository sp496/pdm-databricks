"""Per-segment orchestration for the 3PL curated layer.

`curate_segment` runs the full curated pipeline for ONE segment (commercial or
clinical) and writes only that segment's partition, so the two segments can be
scheduled, retried, and failed independently. The thin notebooks
`curate_commercial_inventory` / `curate_clinical_inventory` each call it once.

The shared transform core (`curated_processing`) is segment-agnostic and lives in
`transformations.py`; this module only owns the segment-specific wiring
(reference paths, the commercial-only SAP plant lookup) and the partition write.
"""
import os
import traceback

import pandas as pd

from lib.curated.data_cache import MappingFilePaths, RefFilePaths, load_mapping_files
from lib.curated.transformations import curated_processing
from lib.discovery import discover_mapping_file, discover_all_raw_csvs
from common.dbfs_utils import dbfs_path


def build_ref_paths(segment: str, ref_base: str) -> RefFilePaths:
    """Return the reference-CSV fallback paths for a segment.

    Clinical pulls EBS-sourced datasets (no unit cost) and uses clinical_-prefixed
    fallback names so they don't collide with the SAP-shaped commercial CSVs.
    """
    if segment == "clinical":
        return RefFilePaths(
            plant_name_mapping_file_path   = dbfs_path(f"{ref_base}/clinical_plant_name_mapping.csv"),
            material_master_file_path      = dbfs_path(f"{ref_base}/clinical_material_master.csv"),
            lot_no_master_file_path        = dbfs_path(f"{ref_base}/clinical_lot_no_master.csv"),
            lot_no_mapping_file_path       = dbfs_path(f"{ref_base}/clinical_lot_no_mapping.csv"),
            material_description_file_path = dbfs_path(f"{ref_base}/clinical_material_description.csv"),
            uom_master_file_path           = dbfs_path(f"{ref_base}/clinical_uom_master.csv"),
            material_type_file_path        = dbfs_path(f"{ref_base}/clinical_material_type.csv"),
            unit_cost_file_path            = None,  # no EBS cost source — Cost stays null
        )
    return RefFilePaths(
        plant_name_mapping_file_path   = dbfs_path(f"{ref_base}/plant_name_mapping.csv"),
        material_master_file_path      = dbfs_path(f"{ref_base}/material_master.csv"),
        lot_no_master_file_path        = dbfs_path(f"{ref_base}/lot_no_master.csv"),
        lot_no_mapping_file_path       = dbfs_path(f"{ref_base}/lot_no_mapping.csv"),
        material_description_file_path = dbfs_path(f"{ref_base}/material_description.csv"),
        uom_master_file_path           = dbfs_path(f"{ref_base}/uom_master.csv"),
        unit_cost_file_path            = dbfs_path(f"{ref_base}/unit_cost.csv"),
        material_type_file_path        = dbfs_path(f"{ref_base}/material_type.csv"),
    )


def curate_segment(spark, dbutils, curated_cfg: dict, env: str, segment: str,
                   year: str, quarter: str, *, data_source: str,
                   starburst_config: dict | None) -> dict:
    """Curate ONE segment for an already-resolved (year, quarter) and write its
    own partition.

    The calling notebook owns run-context resolution (env, config, data_source,
    starburst_config, year/quarter) — consistent with the sibling staging
    notebooks. This function only does the reusable work: discover the segment's
    mapping workbook, load the reference cache, build the commercial-only SAP
    plant lookup, curate every raw CSV via curated_processing, then write
    `replaceWhere "Segment AND Year AND Quarter"` (a partition predicate now that
    curated_3pl_inventory is partitioned by Segment, Year, Quarter).

    Returns a summary dict: {segment, year, quarter, rows, errors}.
    """
    resolved_env = "prod" if env == "prd" else env

    src_root         = f"{curated_cfg['src_bkt_mount_point']}/{curated_cfg['src_data_dir'].format(env=resolved_env)}"
    raw_root         = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['raw_data_dir']}"
    ref_base         = f"{curated_cfg['data_bkt_mount_point']}/{curated_cfg['ref_data_dir']}"
    curated_table    = curated_cfg["curated_table"].format(env=env)
    sap_report_table = curated_cfg["sap_report_table"].format(env=env)

    print(f"\n{'='*60}\nSegment: {segment}  ({year}/{quarter})\n{'='*60}")
    print(f"Environment : {env}   Data source : {data_source}")

    src_quarter_root = f"{src_root}/{segment}/{year}/{quarter}"
    raw_quarter_root = f"{raw_root}/{segment}/{year}/{quarter}"

    # ------------------------------------------------------------------
    # Discover mapping workbook for this segment
    # ------------------------------------------------------------------
    mapping_path = discover_mapping_file(dbutils, src_quarter_root, segment)
    print(f"  Mapping file: {mapping_path}")
    if not mapping_path:
        print(f"  Could not find mapping file under {src_quarter_root}/mapping_files — nothing to do")
        return {"segment": segment, "year": year, "quarter": quarter, "rows": 0, "errors": []}

    file_paths = MappingFilePaths(
        mapping_file_path           = dbfs_path(mapping_path),
        header_mapping_sheet_name   = "Header Mapping",
        item_mapping_sheet_name     = "Item Mapping",
        uom_mapping_sheet_name      = "UOM Mapping",
        # Clinical almac workbook carries this; commercial workbooks don't —
        # the guarded load yields None when the sheet is absent.
        facility_mapping_sheet_name = "Facility Mapping",
        # Manual 3PL lot -> Gilead lot override; guarded load (absent/empty = no-op).
        lot_mapping_sheet_name      = "Lot Mapping",
    )
    ref_paths = build_ref_paths(segment, ref_base)

    mapping_cache = load_mapping_files(
        file_paths       = file_paths,
        ref_paths        = ref_paths,
        year             = year,
        quarter          = quarter,
        data_source      = data_source,
        starburst_config = starburst_config,
        segment          = segment,
    )
    print(f"  Header mapping built — {len(mapping_cache.header_mapping)} site/sheet key(s)")

    # ------------------------------------------------------------------
    # SAP plant lookup (commercial only) for multi-plant folders.
    # ------------------------------------------------------------------
    sap_plant_df = None
    if segment == "commercial":
        sap_plant_df = (
            spark.table(sap_report_table)
            .filter(f"Segment = 'commercial' AND Year = '{year}' AND Quarter = '{quarter}'")
            .select("Plant", "Material_Number", "Batch_Number", "Stock_Quantity__Base_UOM_")
            .toPandas()
        )
        print(f"  SAP plant lookup loaded — {len(sap_plant_df)} (Plant, Material, Batch, Qty) rows")

    # ------------------------------------------------------------------
    # Discover and curate raw CSVs
    # ------------------------------------------------------------------
    raw_files_by_site = discover_all_raw_csvs(dbutils, raw_quarter_root)
    total_files = sum(len(v) for v in raw_files_by_site.values())
    print(f"  Discovered {len(raw_files_by_site)} site(s), {total_files} CSV file(s) to curate")

    curated_dfs = []
    errors = []
    for site_id, raw_paths in raw_files_by_site.items():
        print(f"\n  Site: {site_id}  ({len(raw_paths)} file(s))")
        for raw_path in raw_paths:
            print(f"    Source: {os.path.basename(raw_path)}")
            try:
                raw_df     = pd.read_csv(dbfs_path(raw_path), dtype=str)
                curated_df = curated_processing(raw_df, raw_path, mapping_cache, segment, sap_plant_df)
                curated_dfs.append(curated_df)
                print(f"    Processed {curated_df.shape[0]} rows")
            except Exception as e:
                print(f"    ERROR: {os.path.basename(raw_path)}: {e}")
                traceback.print_exc()
                errors.append((segment, site_id, os.path.basename(raw_path), str(e)))

    # ------------------------------------------------------------------
    # Write this segment's partition (idempotent, scoped to Segment/Year/Quarter)
    # ------------------------------------------------------------------
    replace_where = f"Segment = '{segment}' AND Year = '{year}' AND Quarter = '{quarter}'"
    rows = 0
    if curated_dfs:
        all_curated = pd.concat(curated_dfs, ignore_index=True)
        rows = len(all_curated)
        spark_df = spark.createDataFrame(all_curated)
        (
            spark_df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_where)
            .option("overwriteSchema", "false")
            .saveAsTable(curated_table)
        )
        print(f"\n  Wrote {rows} rows to {curated_table} [{replace_where}]")
    else:
        # No rows for this segment/quarter — clear just this partition so a rerun
        # that produced nothing doesn't leave stale rows. spark.createDataFrame
        # can't infer a schema from an empty pandas df, so DELETE rather than write.
        try:
            spark.sql(f"DELETE FROM {curated_table} WHERE {replace_where}")
            print(f"\n  No rows for {segment}/{year}/{quarter} — cleared partition (no write)")
        except Exception as e:
            print(f"\n  No rows for {segment}/{year}/{quarter} — skipped "
                  f"(table not yet created: {e.__class__.__name__})")

    summary = {"segment": segment, "year": year, "quarter": quarter, "rows": rows, "errors": errors}
    print(f"\n{'='*60}")
    print(f"Curated processing complete for {segment}: rows={rows}, errors={len(errors)}")
    if errors:
        for seg, site_id, fname, err in errors:
            print(f"    [{seg}/{site_id}] {fname}: {err}")
    return summary
